r'''dfbuilder -- Backend for dataframe builder i2b2 plugin
..........................................................

Usage:

   $ python cdr2edc.dfbuilder db_and_output.conf job_info.json


i2b2 Clinical Data Repository
-----------------------------

Suppose we have an i2b2 clinical data repository with some number of
facts about some number of patients:

    >>> import i2b2_project_mock
    >>> cdw = i2b2_project_mock.mock_cdw(
    ...     n_patient=20, n_fact=1000)


Jobs Queued from Plug-in UI
---------------------------

The `DFBuilder` plug-in lets users choose a patient set, a list of
metadata concepts (aka variables), and (the base of) a filename; it
authenticates the user's credentials and queues the request:

    >>> concept_names = r"""
    ... Distance from KUMC
    ... ED Vitals
    ... """.strip().split("\n")
    >>> concept_item_keys = r"""
    ... \\table_code\i2b2\Demographics\KUMC radius\
    ... \\table_code\i2b2\Flowsheets\ED\Vitals\
    ... """.strip().split('\n')
    >>> job_info = dict(
    ...     label='Interesting Cohort', patient_set=123,
    ...     concepts=dict(names=concept_names,
    ...                   keys=concept_item_keys),
    ...     filename='cohort-1-dx-lab',
    ...     username='demo')


Configuration
-------------

Access to the i2b2 star schema and the output storage is specified in the
extract configuration file:

    >>> from pkg_resources import resource_string
    >>> config_txt = resource_string(__name__, 'dfbuild-extract.conf.example')

    >>> from lafile_mock import MockEditable
    >>> write_any_file = MockEditable('/', {
    ...     '/etc/dfbuild.conf': config_txt,
    ...     '/job1.json': json.dumps(job_info)
    ... })

    >>> result_db = i2b2_project_mock.in_memory_db()

    >>> def _create_engine(url):
    ...     return result_db if 'sqlite:' in str(url) else cdw

The parameters (config, filename) come in via the command-line:
    >>> argv='dfbuilder.py /etc/dfbuild.conf job1.json'.split()

The SMTP capability (for tests we have the quiet version - no print)
    >>> def sendmailquiet(sender, receiver, msg):
    ...     pass
    >>> mocksmtp = MockSMTP()
    >>> mocksmtp.sendmail = sendmailquiet

Then `_trusted_main()` gets access to I/O and the database and such
providing these capabilities to :py:func:`main`:

    >>> env = {DB_KEY: 'sekret'}
    >>> main(argv=argv,
    ...      arg_rd=write_any_file.ro(),
    ...      db_access=mk_db_access(_create_engine),
    ...      config_arg1=lambda: mock_config(write_any_file, env),
    ...      getuser=lambda: 'me',
    ...      smtp=mocksmtp,
    ...      gethostname=lambda : 'mock_host')


Results
-------

The `patient_dimension` table is pretty straightforward; it has one
row for each patient in the patient set:

    >>> pat = result_db.execute(
    ...     'select * from patient_dimension').fetchall()
    >>> int(pat[0].age_in_years_num), pat[0].race_cd, pat[0].sex_cd
    (52, u'white', u'm')

The `observation_fact` has many more rows than the patient table:

    >>> result_db.execute(
    ...     'select count(*) from observation_fact').scalar()
    174

It has more of an EAV shape. We can grab the records about one patient:

    >>> p1 = result_db.execute(
    ...     'select * from observation_fact_dt where patient_num = 1'
    ...     ' order by start_date, concept_cd').fetchall()

One fact about patient 1 (entity) is that their distance from KUMC
(attribute) was in the 5 mile range (value):

    >>> (p1[1].patient_num, p1[1].concept_cd, p1[1].start_date[:10])
    (1, u'DEM|GEO|KUMC:5mi', u'1965-10-14')

In some cases, the relationship bewteen the variable you asked for
and the observation fact you get back is even more indirect; we asked
for vitals, so in particular, we got pulse:

    >>> (p1[10].patient_num, p1[10].concept_cd, p1[10].start_date[:10])
    (1, u'KUH|MEASURE_ID:8', u'1985-11-27')

But we can join back from the concept_dimension to the variables we
asked for:

    >>> result_db.execute(
    ...     """select v.name_char
    ...        from variable v join concept_dimension cd
    ...        on cd.concept_path like (v.concept_path || '%')
    ...        where cd.concept_cd = :m8
    ...     """, m8=u'KUH|MEASURE_ID:8').scalar()
    u'ED Vitals'


Design Note
-----------

.. note:: See also :ref:`well-typed` regarding type declarations,
          `scala_hide`, `classOf`, and `pf_`.
.. i.e. devdoc/well_typed.rst

API
---

'''

import json
import logging
import re

from sqlalchemy import Table, Column, types
from sqlalchemy import select, and_
from sqlalchemy.engine.url import URL as DBURL
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import bindparam, func
from sqlalchemy.exc import OperationalError

from ocap import lafile
from emailer import Emailer
from emailer import MockSMTP

import sqla_float_date as fd
import i2b2_star
import table_copy as tc

from os.path import isdir
import errno


log = logging.getLogger('dfbuilder')

classOf = lambda cls: cls  # scala_hide
typed = lambda x, t: x  # scala_hide

DB_KEY = 'extract_password'


class DataExtract(object):
    cdw_section = 'deid'

    drivername = 'oracle+cx_oracle'

    data_tables = ['patient_dimension',
                   'visit_dimension',
                   'concept_dimension',
                   'modifier_dimension',
                   'observation_fact']

    @classmethod
    def mk_db(cls, create_engine, cdw_opts):
        log.info('engine.url.URL(%s)',
                 ', '.join('%s=...' % k
                           for (k, _v) in cdw_opts))
        return create_engine(DBURL(**dict(
            (k, v) for (k, v) in cdw_opts if k != DB_KEY)))

    def __init__(self, account, user_id,
                 label, concepts, patient_set, filename):
        # TODO: make these read-only properties
        self.user_id = user_id
        self.label = label
        self.concepts = concepts
        self.patient_set = patient_set
        self.filename = filename

        concept_keys = concepts['keys']

        def demographics():
            log.info('getting demographics for patient set #%d', patient_set)
            pat_q, enc_q = self.patients_query('result_instance_id')
            return [(pat_q, account.execute(pat_q,
                                            result_instance_id=patient_set)),
                    (enc_q, account.execute(enc_q,
                                            result_instance_id=patient_set))]
        self.demographics = demographics

        def term_info():
            log.info('getting term info for %d paths', len(concept_keys))
            tmp, ins, bind = DataExtract._save_concepts(concepts)
            account.execute(tmp.delete())
            if len(bind) > 0:
                account.execute(ins, bind)
            ordered = [q.order_by(col)
                       for (q, col) in self._term_query(tmp)]
            return [(q, account.execute(q))
                    for q in ordered]
        self.term_info = term_info

        def patient_data():
            log.info('getting patient data for patient set %d', patient_set)
            var_tmp, var_ins, var_bind = DataExtract._save_concepts(concepts)
            account.execute(var_tmp.delete())
            if len(var_bind) > 0:
                account.execute(var_ins, var_bind)
            code_tmp, ins, sel = DataExtract.patient_data_queries(var_tmp)
            account.execute(code_tmp.delete())
            account.execute(ins)
            return sel, account.execute(sel, id=patient_set)
        self.patient_data = patient_data

    @classmethod
    def copy_star_schema(cls, bind=None):
        m = MetaData()
        for t in cls.data_tables:
            i2b2_star.metadata.tables[t].tometadata(m)
        if bind:
            m.bind = bind
        return m

    @classmethod
    def patient_data_queries(cls, tmp):
        '''
        >>> var_tmp = i2b2_star.t_global_temp_fact_param_table
        >>> code_tmp, ins, sel = DataExtract.patient_data_queries(var_tmp)

        >>> print code_tmp
        query_global_temp

        >>> print ins
        ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
        INSERT INTO query_global_temp (concept_cd)
        SELECT DISTINCT tq.concept_cd
        FROM
          (SELECT DISTINCT cd.concept_path AS concept_path,
                           cd.concept_cd AS concept_cd,
                           cd.name_char AS name_char
           FROM global_temp_fact_param_table
           JOIN concept_dimension AS cd
             ON cd.concept_path
                LIKE (global_temp_fact_param_table.char_param1
                      || :char_param1_1) ESCAPE '|') AS tq

        >>> print sel
        ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
        SELECT f.encounter_num, f.patient_num, f.concept_cd, ...
        FROM observation_fact AS f
        JOIN query_global_temp
          ON f.concept_cd = query_global_temp.concept_cd
        JOIN qt_patient_set_collection AS pset
          ON pset.patient_num = f.patient_num
        WHERE pset.result_instance_id = :id

        '''
        [(tq_sql, _sortcol), _modifier_stuff] = cls._term_query(tmp)

        code_tmp = i2b2_star.t_query_global_temp
        ins = (code_tmp.insert()
               .from_select(
                   ['concept_cd'],
                   select([tq_sql.alias('tq').c.concept_cd]).distinct()))

        f = i2b2_star.t_observation_fact.alias('f')
        pset = i2b2_star.t_qt_patient_set_collection.alias('pset')

        s_facts = (select([f])
                   #@@.with_hint(f, '+ index(%(name)s, OBS_FACT_CON_CODE_BI)')
                   .select_from(
                       f.join(code_tmp,
                              f.c.concept_cd == code_tmp.c.concept_cd)
                       .join(pset,
                             pset.c.patient_num == f.c.patient_num))
                   .where(pset.c.result_instance_id == bindparam("id")))

        return code_tmp, ins, s_facts

    @classmethod
    def patients_query(cls, bind):
        '''
        >>> pat_q, enc_q = DataExtract.patients_query('result_instance_id')

        >>> print pat_q
        ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
        SELECT pd.patient_num, pd.vital_status_cd, pd.birth_date, ...
        FROM patient_dimension AS pd
        JOIN qt_patient_set_collection AS pset
          ON pset.patient_num = pd.patient_num
        WHERE pset.result_instance_id = :result_instance_id

        >>> print enc_q
        ... # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
        SELECT vd.encounter_num, vd.patient_num, ...
        FROM visit_dimension AS vd
        JOIN
          (SELECT max(vd.encounter_num) AS encounter_num,
                  vd.patient_num AS patient_num
           FROM visit_dimension AS vd
           JOIN
             (SELECT vd.patient_num AS patient_num,
                     max(vd.start_date) AS last_visit
              FROM visit_dimension AS vd
              JOIN patient_dimension AS pd
                ON pd.patient_num = vd.patient_num
              JOIN qt_patient_set_collection AS pset
                ON pset.patient_num = pd.patient_num
             WHERE pset.result_instance_id = :result_instance_id
             GROUP BY vd.patient_num) AS last
             ON vd.patient_num = last.patient_num
            AND vd.start_date = last.last_visit
           GROUP BY vd.patient_num) AS arb_visit
        ON vd.encounter_num = arb_visit.encounter_num
        '''
        patient_set = bindparam(bind, type_=types.Integer)

        pset = i2b2_star.t_qt_patient_set_collection.alias('pset')
        pd = i2b2_star.t_patient_dimension.alias('pd')
        vd = i2b2_star.t_visit_dimension.alias('vd')

        pat_q = (
            select([pd])
            .select_from(
                pd
                .join(pset,
                      pset.c.patient_num == pd.c.patient_num))
            .where(pset.c.result_instance_id == patient_set))

        last = (
            select([vd.c.patient_num,
                    func.max(vd.c.start_date).label('last_visit')])
            .select_from(
                vd
                .join(pd, pd.c.patient_num == vd.c.patient_num)
                .join(pset,
                      pset.c.patient_num == pd.c.patient_num))
            .where(pset.c.result_instance_id == patient_set)
            .group_by(vd.c.patient_num)).alias('last')

        arb_visit = (
            select([func.max(vd.c.encounter_num).label('encounter_num'),
                    vd.c.patient_num])
            .select_from(
                vd
                .join(last,
                      and_(vd.c.patient_num == last.c.patient_num,
                           vd.c.start_date == last.c.last_visit)))
            .group_by(vd.c.patient_num)).alias('arb_visit')

        enc_q = (
            select([vd])
            .select_from(
                vd
                .join(arb_visit,
                      arb_visit.c.encounter_num == vd.c.encounter_num)))

        return pat_q, enc_q

    @classmethod
    def _term_query(cls, tmp,
                    escape='|'):
        r'''
        :param escape: character to use to override LIKE escape character,
                       since both postgres and i2b2 make use of \.

        >>> tmp = i2b2_star.t_global_temp_fact_param_table
        >>> [(q_cd, col_cd), (q_md, col_md)] = DataExtract._term_query(tmp)

        >>> print col_cd
        concept_path

        >>> print q_cd
        ... # doctest: +NORMALIZE_WHITESPACE
        SELECT DISTINCT cd.concept_path, cd.concept_cd, cd.name_char
        FROM global_temp_fact_param_table
        JOIN concept_dimension AS cd
          ON cd.concept_path LIKE
             (global_temp_fact_param_table.char_param1 || :char_param1_1)
             ESCAPE '|'
        '''
        cd = i2b2_star.t_concept_dimension.alias('cd')
        md = i2b2_star.t_modifier_dimension.alias('md')

        q_cd = (select([cd.c.concept_path,
                        cd.c.concept_cd,
                        cd.c.name_char]).distinct()
                .select_from(
                    tmp
                    .join(cd,
                          cd.c.concept_path.like(tmp.c.char_param1 + '%',
                                                 escape=escape))))

        q_md = (select([md.c.modifier_path,
                        md.c.modifier_cd,
                        md.c.name_char]).distinct()
                .select_from(
                    tmp
                    .join(md,
                          tmp.c.char_param1.like(md.c.modifier_path))))

        return [(sql, iter(sql.columns).next())
                for sql in [q_cd, q_md]]

    @classmethod
    def _save_concepts(cls, concepts):
        r'''Prepare to save concepts in a temporary table.

        >>> concepts = dict(
        ...     names=['apples', 'bananas', 'cherries'],
        ...     keys=[r'\\tk\a', r'\\tk\b', r'\\tk\c'])
        >>> tmp, ins, bind = DataExtract._save_concepts(concepts)

        >>> print tmp.delete()
        DELETE FROM global_temp_fact_param_table

        >>> print ins
        ... # doctest: +NORMALIZE_WHITESPACE
        INSERT INTO global_temp_fact_param_table (char_param1, char_param2)
        VALUES (:path, :name)

        >>> sorted(bind[0].keys())
        ['name', 'path']

        >>> sorted(bind[0].values())
        ['\\a', 'apples']

        '''
        names = concepts['names']
        paths = I2B2MetaData.keys_to_paths(concepts['keys'])
        bind = [dict(name=name,
                     path=path)
                for (path, name) in zip(paths, names)]
        tmp = i2b2_star.t_global_temp_fact_param_table
        ins = tmp.insert().values(char_param1=bindparam('path'),
                                  char_param2=bindparam('name'))
        return tmp, ins, bind


class I2B2MetaData(object):
    @classmethod
    def keys_to_paths(cls, keys):
        r'''Strip TABLE_ACCESS key off keys

        .. note:: This assumes just one TABLE_ACCESS record.
        .. note:: Consider moving the note above to the module documentation.

        >>> keys = r"""
        ... \\t1\a\b\
        ... \\t2\c\d\
        ... """.strip().split('\n')
        >>> for p in I2B2MetaData.keys_to_paths(keys):
        ...     print p
        \a\b\
        \c\d\

        >>> keys = [r'\\i2b2\i2b2\Demographics\KUMC radius\X'[:-1]]
        >>> for p in I2B2MetaData.keys_to_paths(keys):
        ...     print p
        \i2b2\Demographics\KUMC radius\

        '''
        bs = '\\'
        return [bs + bs.join(key.split(bs)[3:])
                for key in keys]


class DataDest(object):
    '''Data extract destination, which pulls from a DataExtract.

    >>> from i2b2_project_mock import in_memory_db

    >>> dest_db = in_memory_db()

    >>> dest = DataDest(dest_db, '/home/me/heron/job1.db')

    >>> from i2b2_project_mock import mock_cdw
    >>> cdw = mock_cdw(10, 100)

    >>> concepts = dict(keys=[r'\\tk\k1', r'\\tk\k2'],
    ...                 names=['n1', 'n2'])

    >>> job = DataExtract(cdw, 'me',
    ...                   'Interesting Query', concepts, 123, 'job1.db')


    >>> out = dest.export(job)
    >>> sorted(out.items())[:-1]
    ... # doctest: +NORMALIZE_WHITESPACE
    [('filename', '/home/me/heron/job1.db'),
     ('id', 123),
     ('n_patient', 5)]

    >>> print out['str']
    ... # doctest: +NORMALIZE_WHITESPACE
    Variable                                 N. Patient    N. Obs.
    n1                                                5        116
    n2                                                5        116
    '''
    drivername = 'sqlite'

    def __init__(self, dest_db, full_path):
        self.full_path = full_path

        def init_tables(job):
            log.info('initializing tables in %s', dest_db)
            dest_star = job.copy_star_schema(bind=dest_db)
            log.debug('dest_star tables: %s', dest_star.tables.keys())
            self._dumb_down_schema(dest_star)
            dest_star.drop_all(dest_db)
            dest_star.create_all(dest_db)
            for t in dest_star.tables.values():
                fd.create_unpacked_view(dest_db, t)
            return dest_star
        self.init_tables = init_tables

        def export_job(dest_star, job):
            jobt = self.job_table(dest_star)
            jobt.drop(bind=dest_db, checkfirst=True)
            jobt.create(bind=dest_db)
            dest_db.execute(jobt.insert(),
                            pset=job.patient_set,
                            label=job.label,
                            concepts=json.dumps(job.concepts),
                            name=job.filename)
        self.export_job = export_job

        def export_patients(dest_star, job):
            pd = dest_star.tables['patient_dimension']
            vd = dest_star.tables['visit_dimension']
            [(pat_q, pat_data), (enc_q, enc_data)] = job.demographics()
            tc.copy_in_chunks(dest_db, pat_data, pd,
                              'demographics (patient_dimension)', [])
            tc.copy_in_chunks(dest_db, enc_data, vd,
                              'demographics (visit_dimension)', [])
            return dest_db.execute(
                'select count(*) from patient_dimension').scalar()
        self.export_patients = export_patients

        def export_terms(dest_star, job):
            v = self.variable_table(dest_star)
            keys = job.concepts['keys']
            names = job.concepts['names']
            paths = I2B2MetaData.keys_to_paths(keys)
            v.create(bind=dest_db)
            dest_db.execute(v.insert(),
                            [dict(id=id,
                                  item_key=key,
                                  concept_path=path,
                                  name_char=name,
                                  name=strip_counts(name))
                             for (id, (path, key, name)) in
                             enumerate(zip(paths, keys, names))])

            [(q_cd, result_cd), (q_md, result_md)] = job.term_info()
            cd = dest_star.tables['concept_dimension']
            md = dest_star.tables['modifier_dimension']

            values = lambda _: dict(concept_path=bindparam('concept_path'),
                                    concept_cd=bindparam('concept_cd'))
            tc.copy_in_chunks(dest_db, result_cd, cd,
                              'concept_dimension', [],
                              values=values)
            values = lambda _: dict(concept_path=bindparam('modifier_path'),
                                    concept_cd=bindparam('modifer_cd'))
            tc.copy_in_chunks(dest_db, result_cd, md,
                              'modifier_dimension', [])
        self.export_terms = export_terms

        def export_data(dest_star, job):
            q, data = job.patient_data()
            obs = dest_star.tables['observation_fact']
            dest_db.execute(obs.delete())
            tc.copy_in_chunks(dest_db, data, obs,
                              'patient data', [])
        self.export_data = export_data

        def export_summary():
            dest_db.execute('''
            create view data_summary as
            select v.concept_path, v.name_char,
              count(distinct patient_num) pat_qty, count(*) fact_qty
            from observation_fact f
            join concept_dimension cd
            on cd.concept_cd = f.concept_cd
            join variable v
            on cd.concept_path like (v.concept_path || '%')
            group by v.concept_path, v.name_char
            ''')
            return dest_db.execute('select * from data_summary').fetchall()
        self.export_summary = export_summary

    @classmethod
    def mk_db(cls, create_engine, on):
        dburl = DBURL(drivername=cls.drivername, database=on.ro().fullPath())
        log.info('dataset DB: %s', dburl)
        return create_engine(dburl)

    def export(self, job,
               term_table_name='code'):
        log.info('exporting: %d concepts from #%d %s',
                 len(job.concepts), job.patient_set, job.label)

        dest_star = self.init_tables(job)
        self.export_job(dest_star, job)
        self.export_data(dest_star, job)
        self.export_terms(dest_star, job)
        pat_qty = self.export_patients(dest_star, job)

        summary = '\n'.join(
            ['%-40s %10s %10s' % ('Variable', 'N. Patient', 'N. Obs.')] +
            ['%-40s %10d %10d' %
             (v.name_char[:40], v.pat_qty, v.fact_qty)
             for v in self.export_summary()])
        log.info('data summary:\n%s', summary)

        return dict(id=job.patient_set,
                    n_patient=pat_qty,
                    filename=self.full_path,
                    str=summary)

    @classmethod
    def job_table(cls, meta,
                  name='job'):
        return Table(name, meta,
                     Column('pset', types.Integer),
                     Column('label', types.String),
                     Column('concepts', types.String),
                     Column('name', types.String))

    @classmethod
    def variable_table(cls, meta,
                       name='variable'):
        return Table(name, meta,
                     Column('id', types.Integer),
                     Column('item_key', types.String),
                     Column('concept_path', types.String),
                     Column('name_char', types.String),
                     Column('name', types.String),
                     Column('short_name', types.String),
                     Column('section', types.String),
                     Column('redundant', types.Boolean),
                     extend_existing=True)

    @classmethod
    def _dumb_down_schema(cls, meta):
        for table in meta.tables.values():
            cls._dumb_down_table(table)

    @classmethod
    def _dumb_down_table(cls, table,
                         float_dates=False):
        '''Un-specialize Oracle numeric types
        '''
        for col in table.columns:
            ty = col.type
            if isinstance(ty, types.Numeric):
                col.type = types.Numeric(ty.precision, ty.scale,
                                         asdecimal=ty.asdecimal)
            elif float_dates and isinstance(ty, types.DateTime):
                col.type = fd.FloatDateTime()

    @classmethod
    def variable_strip_counts(cls, db):
        '''post-hoc schema migration to add name col
        '''
        for (col_ty) in [
                'name VARCHAR',
                'redundant INT',
                'short_name VARCHAR',
                'section VARCHAR']:
            try:
                db.execute('alter table variable add column %s' % col_ty)
            except OperationalError as ex:
                log.debug('cannot add column: %s', ex)

        db.execute('''
        update variable
        set name = substr(name_char, 1, instr(name_char, ' [') - 1)
        ''')
        return db.execute('select * from variable').fetchall()


def strip_counts(txt):
    '''
    >>> strip_counts('broken toe [200 facts]')
    'broken toe'
    '''
    return re.sub(r' \[\d+.*\]', '', txt)


class BuilderApp(object):
    '''App to build data files for the i2b2 Data Builder plug-in.
    '''

    heron_work_dir = 'heron'

    def __init__(self, access):
        self._access = access

    @classmethod
    def make(cls, cdw_section, db_access, home_dirs, ext='.db'):
        def user_access(username):
            cdw_account = db_access(cdw_config=cdw_section)

            #work_dir = home_dirs / username / BuilderApp.heron_work_dir
            work_dir = home_dirs / username 
            try:
                work_dir.mkDir()
            except OSError as exc:
                if exc.errno == errno.EEXIST and isdir(work_dir.ro().fullPath()):
                    # prevent race condition if running multiple extractions simultaneously
                    pass
                else: 
                    log.critical('Error creating user subdir: %s', work_dir.ro().fullPath())
                    raise
            except Exception as e:
                log.critical('Error creating user subdir: %s', work_dir.ro().fullPath())
                raise

            # TODO: check filename?

            def job_storage(name):
                out = work_dir / (name + ext)
                return db_access(on=out), out

            return cdw_account, job_storage  # TODO: mailer?

        return BuilderApp(user_access)

    def __call__(self, username, label, concepts, filename, patient_set):
        '''
        :param String username: the user requesting the data extract
        :param String label: i2b2 patient set label
        :param String concepts: json-encoded data variables
        :param String filename: the filename of the resulting data extract
        :param String patient_set: patient_set id (numeral)

        :rtype: Iterable[String]
        '''
        account, job_storage = self._access(username)

        db, storage = job_storage(filename)
        dest = DataDest(db, storage.ro().fullPath())
        try:
            job = DataExtract(account, username,
                              label, concepts, patient_set, filename)
            out = dest.export(job)
        except IOError as ex:
            log.critical('Error building data:', exc_info=ex)
            return ['error:', str(ex)]

        return [json.dumps(out)]


def send_completion_mail(smtp, email_config, username, gethostname, filename,
                         home_dirs, summary):
    '''
    Config Setup
    >>> from ConfigParser import SafeConfigParser
    >>> cp = SafeConfigParser()
    >>> cp.add_section('email')
    >>> cp.set('email', 'user_domain', 'kumc.edu')
    >>> cp.set('email', 'sender', 'nobody@kumc.edu')
    >>> cp.add_section('output')
    >>> cp.set('output', 'home_dirs', '/home')

    >>> import os
    >>> fs = lafile.Readable('/', os.path, os.listdir, open)
    >>> config_dir = lafile.ConfigRd(cp, fs)

    Mock Host
    >>> gethostname = lambda : 'mock_host'

    Mock SMTP
    >>> mocksmtp = MockSMTP()

    Sending completion email
    >>> send_completion_mail(mocksmtp, config_dir / 'email', 'somebody',
    ...                      gethostname, 'data.db',
    ...                      (config_dir / 'output' / 'home_dirs').fullPath(),
    ...                      'some summary')
    MockSMTP:sendmail()
    ===================
    FROM:nobody@kumc.edu
    TO:['somebody@kumc.edu']
    ===================
    Content-Type: text/plain; charset="us-ascii"
    MIME-Version: 1.0
    Content-Transfer-Encoding: 7bit
    Subject: The dataset 'data.db' is now available
    From: nobody@kumc.edu
    To: somebody@kumc.edu
    <BLANKLINE>
    The dataset data.db is now available on mock_host at '/home/somebody/heron'
    <BLANKLINE>
    ====DATA SUMMARY====
    some summary

    Modify sendmail to raise an IOError
    >>> def sendmailex(sender, receiver, msg):
    ...     raise IOError
    >>> mocksmtp.sendmail = sendmailex

    Failure to send the e-mail will result in a logged warning *trust me*,
    but will otherwise continue on.
    >>> send_completion_mail(mocksmtp, config_dir / 'email', 'somebody',
    ...                      gethostname, 'data.db',
    ...                      (config_dir / 'output' / 'home_dirs').fullPath(),
    ...                      'some summary')
    '''
    if email_config.exists():
        domain = email_config.get('user_domain')

        #TODO get actual users email (from json)
        #It is an unfortunate hack that we use the [username]+[config domain]
        recipient = '{0}@{1}'.format(username, domain)
        recipient = 'bos@uthscsa.edu'
        emailer = Emailer(smtp, lambda: [recipient])

        message_kwds = {'filename': filename,
                        'hostname': gethostname(),
                        'location': '%s/%s' % (home_dirs, username),
                        'summary': summary}
        body = 'The dataset {filename} is now available on {hostname} ' \
               'at \'{location}\'' \
               '\n\n====DATA SUMMARY====\n{summary}'.format(**message_kwds)
        subject = 'The dataset \'{filename}\' is now available'.format(
            **message_kwds)

        sender = email_config.get('sender')
        log.info('dfbuilder.py:send_completion_mail()\n From: %s\n To: %s'
                 % (sender, recipient))
        try:
            emailer.sendEmail(body, subject, sender)
        except Exception as ex:
            log.warning('Exception sending e-mail!',
                        exc_info=ex)


def main(argv, arg_rd, db_access, config_arg1, getuser, smtp, gethostname):
    config = config_arg1()
    _config_fn, request_fn = argv[1:3]

    request_readable = arg_rd / request_fn
    home_dirs = config / 'output' / 'home_dirs'
    prefix = request_fn.split('/')[-1].split('.json')[0]

    builder = BuilderApp.make(config.ro() / DataExtract.cdw_section,
                              db_access, home_dirs)

    # todo: static types?
    params = json.load(request_readable.inChannel())
    concepts = params['concepts']
    patient_set = params['patient_set']
    username = params['username']
    #filename = params['filename']
    filename = '%s_%s' % (prefix, params['filename'])
    builder_json = builder(username, params['label'], concepts,
                           filename, patient_set)

    summary = json.loads(builder_json[0])['str']
    send_completion_mail(smtp, (config / 'email').ro(), username, gethostname,
                         filename, home_dirs.ro().fullPath(), summary)


def mk_db_access(create_engine):
    def db_access(cdw_config=None, on=None):
        if on:
            return DataDest.mk_db(create_engine, on)
        else:
            return DataExtract.mk_db(create_engine, cdw_config.items())
    return db_access


def mk_access(os, openf, argv, fileConfig, environ):
    '''Bundle logging.fileConfig() and access to config file.
    Nobody gets access to the latter without doing the former.'''

    # TODO: mock os, openf, argv, and fileConfig
    # rather than mock_config so that we can unit test
    # this function.

    config_fn, request_fn = argv[1:3]

    write_any_file = lafile.Editable('/', os, openf)
    arg_rd = lafile.ListReadable(argv, write_any_file.ro(),
                                 os.path.abspath)

    def config_arg1():
        defaults = {DB_KEY: environ[DB_KEY]}
        log.debug('config defaults:', defaults)
        config_arg1 = lafile.ConfigEd.fromRd(
            rd=arg_rd / config_fn,
            base=write_any_file,
            defaults=defaults)
        fileConfig((config_arg1.ro() / 'logging').get('config'),
                   disable_existing_loggers=False)

        return config_arg1

    return config_arg1, arg_rd


def mock_config(write_any_file, environ,
                path='etc/dfbuild.conf'):
    return lafile.ConfigEd.fromRd(
        rd=write_any_file.ro() / path,
        base=write_any_file,
        defaults={DB_KEY: environ[DB_KEY]})

if __name__ == '__main__':
    def _trusted_main():
        from __builtin__ import open as openf
        from getpass import getuser
        from os import environ
        from sys import argv
        from smtplib import SMTP
        import logging.config
        import os
        import socket

        from sqlalchemy.engine import create_engine

        config_arg1, arg_rd = mk_access(
            os, openf, argv[:], logging.config.fileConfig, environ)

        db_access = mk_db_access(create_engine)

        main(argv[:], arg_rd,
             db_access=db_access,
             config_arg1=config_arg1,
             getuser=getuser,
             smtp=SMTP(),
             gethostname=socket.gethostname)

    _trusted_main()
