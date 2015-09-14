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


function count(){
	counter++;
	console.log(counter);
}


oplog.on('op', function(data) {
	//console.log(data);
});

oplog.on('insert', function(doc) {
	var _id = doc.o._id.toString();
	var clonedDoc = _.clone(doc.o)
	delete clonedDoc._id;

	client.index({
		index: 'project-index',
		type: 'project',
		id: _id,
		body: clonedDoc

	}, function(error, response) {
		count();
		console.log('Document indexed ' + _id);
		if (error)
			console.log(error);
	})
});




/*Handle Updates*/
oplog.on('update', function(doc) {
	var _id = doc.o2._id.toString();

	if (doc.o['$set']) { //SET VALUE

		var partial = doc.o['$set'];
		var body = {};

		_.mapKeys(partial, function(value, key) {
			_.set(body, key, value)
		});

		console.log('$set fields recevied ');

		client.update({
			index: 'project-index',
			type: 'project',
			id: _id,
			body: {
				doc: body
			}
		}, function(error, response) {
			count();
			console.log('Document updated ' + _id);
			if (error)
				console.log(error);
		})

	} else if (doc.o['$unset']) { //UNSET VALUE
		console.log('$unset fields');
		var partial = doc.o['$unset'];
		var fields = _.keys(partial);
		client.update({
			index: 'project-index',
			type: 'project',
			id: _id,
			body: {
				"script": "for(i=0; i < fields.size();i++){	ctx._source.remove(fields.get(i))}",
				"params": {
					"fields": fields
				}
			}
		}, function(error, response) {
			count();
			console.log('Document updated ' + _id);
			if (error)
				console.log(error);
		})


	} else {
		console.log('Full update');
		var partial = doc.o;
		var clonedDoc = _.clone(doc.o)
		delete clonedDoc._id;
		client.update({
			index: 'project-index',
			type: 'project',
			id: _id,
			body: {
				doc: clonedDoc

			}
		}, function(error, response) {
			count();
			console.log('Document updated ' + _id);
			if (error)
				console.log(error);
		})

	}

});

oplog.on('delete', function(doc) {
	var _id = doc.o._id.toString();

	console.log('Delete document ' + _id);

	client.delete({
		index: 'project-index',
		type: 'project',
		id: _id,

	}, function(error, response) {
		count();
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