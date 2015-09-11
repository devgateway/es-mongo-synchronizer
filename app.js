var _ = require('lodash')
var MongoOplog = require('mongo-oplog');
var oplog = MongoOplog('mongodb://127.0.0.1:27017/local', {
	ns: 'aiddata.project'
}).tail();

var elasticsearch = require('elasticsearch');
var counter = 0;

var client = new elasticsearch.Client({
	host: 'localhost:9200',
	log: 'info'
});

client.ping({
	requestTimeout: Infinity,
	hello: "elasticsearch!"
}, function(error) {
	if (error) {
		console.trace('Elasticsearch cluster is down');
	} else {
		console.log('Elastic Search is running');
	}
});

oplog.on('op', function(data) {
	//console.log(data);
});

oplog.on('insert', function(doc) {
	var _id = doc.o2._id.toString();
	var clonedDoc = _.clone(doc.o)
	delete clonedDoc._id;

	client.index({
		index: 'project-index',
		type: 'project',
		id: _id,
		body: {
			doc: clonedDoc
		}
	}, function(error, response) {
		counter++;
		console.log('Document indexed ' + _id);
		if (error)
			console.log(error);
	})
});

oplog.on('update', function(doc) {
	console.log(doc);

	var _id = doc.o2._id.toString();

	var orgDoc = doc.o;
	var body;

	if (orgDoc['$set']) {
		//set single property using partial document
		body = _orgDoc['$set'];

	} else {
		//update whole doc
		var clonedDoc = _.clone(doc.o)
		delete clonedDoc._id;
		body = {
			doc: clonedDoc
		}
	}


	client.update({
		index: 'project-index',
		type: 'project',
		id: _id,
		body:body
	}, function(error, response) {
		counter++;
		console.log('Document updated ' + _id);
		if (error)
			console.log(error);
	})
});

oplog.on('delete', function(doc) {
	var _id = doc.o2._id.toString();

	client.delete({
		index: 'project-index',
		type: 'project',
		id: _id,

	}, function(error, response) {
		counter++;
		console.log('Document deleted ' + _id);
		if (error)
			console.log(error);
	})

});

oplog.on('error', function(error) {
	console.log(error);
});

oplog.on('end', function() {
	console.log('nothing more to do');
});

oplog.stop(function() {
	console.log('Stopping');
});