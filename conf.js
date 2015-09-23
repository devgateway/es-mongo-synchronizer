module.exports = {

	"oplog_db": "//127.0.0.1:27017/local",
	"es": "localhost:9200",
	'log': 'info',
	'max_active_request':25, //how many request to elastic search can be active ,
	 'requestTimeout': 60000,
	/*namespace should be database.collection*/
	'ns_mapping': {
		'aiddata.project': {
			'index': 'project-index',
			'type': 'project',
		},

		'aiddata.location': {
			'index': 'project-index',
			'type': 'locations', //keep an eye location(s) type name is in plural,
			'_parent': 'loc_parent'
		},

		'aiddata.contribution': {
			'index': 'project-index',
			'type': 'contribution',
		},

		'gfdrr.project': {
			'index': 'gfdrr-project-index',
			'type': 'project',
		},
		'gfdrr.location': {
			'index': 'gfdrr-project-index',
			'type': 'locations', //keep an eye location(s) type name is in plural,
			'_parent': 'loc_parent'
		}
	},
	'options': {
		'retryOnConflict': 10,

	}

};