module.exports = {

	"oplog_db": "//127.0.0.1:27017/local",
	"mongo": {
		"host": 'localhost',
		"port": 27017
	},
	"es": "localhost:9200",
	'log': 'info',
	'useQueue': false,
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
		'requestTimeout': Infinity,
		'retryOnConflict': 1,

	}

};