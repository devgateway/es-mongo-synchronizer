module.exports = {

	"oplog_db": "//127.0.0.1:27017/local",
	"es": "localhost:9200",
	'log': 'info',
	'max_active_request': 5, //how many request to elastic search can be active ,
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
		'requestTimeout': 60000,
	}

};