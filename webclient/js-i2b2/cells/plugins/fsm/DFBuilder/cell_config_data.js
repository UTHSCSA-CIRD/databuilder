{
    files: [
	'tool_widgets.js',
	'DFBuilder_ctrlr.js'],
    css: ['tools.css'],
    config: {
	short_name: 'DataBuilder',
	name: 'Data Builder',
	description: ('Build SQLite and condensed csv data files from ' +
		      'patient set and variable concepts'),
	category: ["celless", "plugin", "kumc"],
	plugin: {
	    isolateHtml: false,
	    isolateComm: true, // which framework is this about?
	    standardTabs: false,
	    html: {
		source: 'dfb_ui.html',
		mainDivId: 'analysis-mainDiv'
	    }
	}
    }
}
