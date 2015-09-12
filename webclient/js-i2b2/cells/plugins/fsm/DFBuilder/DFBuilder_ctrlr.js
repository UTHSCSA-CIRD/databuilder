/* DFBuilder_ctrlr.js -- UI for building R data.frame from i2b2

Note: we assume Array.prototype has been extended with map, join,
since i2b2 uses the prototype framework.

TODO: localize jslint exceptions
*/
/*jslint maxlen: 79, nomen: true, white: true*/
/*jslint vars: true */
/*global $j, i2b2, Ajax */
/*global window, alert */
'use strict';
(function (exports, i2b2, tool_widgets, Ajax, $j) {
    var tw = tool_widgets;

    var DFTool = (function (_super) {
	tw.__extends(DFTool, _super);

	function DFTool(container, rgate, builder) {
            _super.apply(this, arguments);

	    this.concepts = [];

            this.pw = new tw.DropWidget(
		this, $j('#DFBuilder-PRSDROP'), 0, tw.PRS);
            this.cw = new tw.DropWidget(
		this, $j('#DFBuilder-CONCPTDROP'), 0, tw.CONCPT);
            this.qw = new tw.DropWidget(
		this, $j('#DFBuilder-QDROP'), 0, tw.QM);

	    //$j('.username').text(i2b2.h.getUser());
	    //TODO: drop concepts
	    this.filename = null;
	    this.project_id = null;
	}

	// http://stackoverflow.com/questions/710586/json-stringify-bizarreness
	if(window.Prototype) {
	    //delete Object.prototype.toJSON;
	    delete Array.prototype.toJSON;
	    //delete Hash.prototype.toJSON;
	    //delete String.prototype.toJSON;
	}

	function queryConcepts(cw, qdef) {
	    var xpath = i2b2.h.XPath, xval=i2b2.h.getXNodeVal,
	    panels = xpath(qdef, "//query_name/../panel/item");

	    return panels.map(function (i) {
		return cw.mkItem(xval(i, "item_name"), xval(i, "item_key"));
	    });
	}

	DFTool.prototype.dropNotify = function (item, ix, kind) {
	    var that = this;
	    $j('#df_str').hide();
	    $j('#df_suggest').hide();
            //$j('#runKM').disable(false);

	    if (kind === this.pw.kind) {
		this.prs = item;
		if (! $j("#filename").val()) {
		    $j("#filename").val("cird-" + this.pw.pset_id(this.prs));
                    this.filename =  $j("#filename").val(); 
                    this.infoElt.hide();
		}
	    } else if (kind === this.cw.kind) {
		if (item.origData.isModifier) {
		    alert("Modifier support is TODO.");
		    return false;
		}

		this.concepts.push(item);
		this.conceptsSort();
		this.conceptsRender();
	    } else if (kind === this.qw.kind) {
		var pushConcept = function(c) {
		    that.concepts.push(c);
		};

		var onGetQuery = function(results) {
		    if (results.error) {
			that.warn(results.errorMsg);
			return;
		    }

		    // TODO: what about modifiers?
		    queryConcepts(that.cw,
				  results.refXML).each(pushConcept);
		    that.conceptsSort();
		    that.conceptsRender();
		};

		tw.getQueryDef(i2b2.CRC.ajax, this.qw.qm_id(item),
			       onGetQuery);
	    }
	    return true;
	};

	DFTool.prototype.conceptsSort = function () {
	    var cw = this.cw;
	    this.concepts.sort(function(a, b) {
		return cw.displayName(a) > cw.displayName(b);
	    });
	};

	DFTool.prototype.conceptsRender = function() {
	    var i, s='',
	    that=this, cw=this.cw,
	    dz = $j('#DFBuilder-CONCPTDROP'),
	    cl = this.concepts.length,
	    escape = i2b2.h.Escape;
	    if (cl === 0) {
		dz.html('<div class="concptItem">'
			+ 'Drop one or more Concepts here</div>');
		// hide delete message?
		return;
	    }

	    for (i = 0; i < cl; i += 1) {
		if (i > 0) {
		    s += '<div class="concptDiv"></div>';
		}
		s += ('<a class="concptItem" id="_' + i + '">'
		      + escape(cw.displayName(this.concepts[i])) + '</a>');
	    }
	    dz.html(s);
	    $j('#DFBuilder-CONCPTDROP a.concptItem').click(function(e) {
		// take leading _ off _3 and convert to number
		var which = $j(this).attr('id').substr(1) |0;
		that.conceptDelete(which);
	    });
	};

	DFTool.prototype.conceptDelete = function(which) {
	    this.concepts.splice(which, 1);
	    this.conceptsRender();
	};

	DFTool.prototype.params = function (choice){
	    var filename = $j('#filename').val();
	    var project_id = $j('#project_id').val();
	    var concepts = {names: this.concepts.map(this.cw.displayName),
			keys: this.concepts.map(this.cw.concept_key)};
	    var concepts_str = JSON.stringify(concepts);

            if(!this.prs) {
		return this.warn(
		    "Patient set missing; please drag one from previous queries.");
            }
	    // TODO: warn about uppercase? VL reported it doesn't work.
	    if (!/^[A-Za-z0-9\._\-]+$/.test(filename)) {
		return this.warn("Please use only letters, digits, period (.), underscore (_) or hyphen (-) in filenames.");
	    }
	    if (!/^[A-Za-z0-9\._\-]+$/.test(project_id)) {
		return this.warn("Please use only letters, digits, period (.), underscore (_) or hyphen (-) in project ID.");
	    }
	    //var backend = $j('#backend').val();
	    var backend = 'builder';

	    return {
		backend: backend,
		r_script: 'dfbuilder.R',
		r_function: 'build.dataframes',
		patient_set: this.pw.pset_id(this.prs),
		label: this.pw.displayName(this.prs),
		concepts: concepts_str,
		filename: filename,
		project_id: project_id
	    };
	};

	DFTool.prototype.show_results = function (results) {
	    var suggested_R = (
		"items <- readRDS('"
		    + results.filename
		    + "')\nstr(items)");
	    $j('#df_suggest').text(suggested_R);
	    
	    // show structure of resulting items
	    $j('#df_str').text(results.str);
	    $j('#df_str').append('<br><br>Your data pull query has been successfully submitted. CIRD will contact you shortly, using the email address associated with your i2b2 account, with instructions to obtain the data.');
	    $j('#df_str').show();
	    //$j('#df_suggest').show();
	    this.resultsElt.hide();
            this.infoElt.hide();
	    //$j("#filename").val("");
            //$j("#project_id").val("");;
            //$j('#runKM').disable(true);
	};
	return DFTool;
    }(tw.RGateTool));

    exports.model = undefined;
    function Init(loadedDiv) {
	var rgate = tw.mkWebPostable('/cgi-bin/rgate.cgi', Ajax);
	var builder = tw.mkWebPostable('/cgi-bin/dfbuild.cgi', Ajax);
	var dftool = new DFTool($j(loadedDiv), rgate, builder);
	exports.model = dftool;
        $j('#runKM').click(function() {
	    dftool.runTool();
	});
        $j('#filename').keyup(function(e) {
            if ($j(this).val() != this.filename) {
                ClearResults();
            }
            this.filename =  $j(this).val(); 
        });
        $j('#project_id').keyup(function(e) {
            if ($j(this).val() != this.project_id) {
                ClearResults();
            }
            this.project_id =  $j(this).val(); 
        });
        $j('#df_str').hide(); 
        dftool.resultsElt.hide();
    }
    exports.Init = Init;
    function Unload() {
	exports.model = undefined;
	return true;
    }
    exports.Unload = Unload;

    function ClearResults() {
        $j('#df_str').hide(); 
        dftool.resultsElt.hide();
        //$j('#runKM').disable(false);
    } 

}(i2b2.DFBuilder, i2b2,
  i2b2.DFBuilder_tool_widgets,
  Ajax, $j)
);
