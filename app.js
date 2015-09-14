var _ = require('lodash')

var MongoOplog = require('mongo-oplog');
var conf = require('./conf.java');
var elasticsearch = require('elasticsearch');

var counter = 0;

var oplog = MongoOplog('mongodb:' + conf.db, {
	ns: conf.ns
}).tail();
var client = new elasticsearch.Client({
	host: conf.es,
	log: conf.log
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



/**
 * [parse_id from oplog document]
 * @param  {[type]} doc [description]
 * @return {[type]}     [description]
 */
 function parse_id(doc) {
 	return doc.o2 ? doc.o2._id.toString() : doc.o._id.toString();
 }

/**
 * Handles ES response
 * @param  {[type]} action   [description]
 * @param  {[type]} _id      [description]
 * @param  {[type]} error    [description]
 * @param  {[type]} response [description]
 * @return {[type]}          [description]
 */
 function handleResponse(action, _id, error, response) {
 	if (error) {
 		if (error.status == 404) {
 			console.log('ERROR: document not found on elastic search: ' + _id);
 		} else {
 			console.log(error)
 		}
 	} else {
 		counter++;
 		console.log(action + ' - ' + ' - ' + _id + ' - ' + counter);

 	}
 }

/**
 * Index document 
 * @param  {[type]} _id [description]
 * @param  {[type]} o   [description]
 * @return {[type]}     [description]
 */
 function index(_id, o) {
 	client.index(
 		_.assign({
 			id: _id,
 			body: o
 		}, conf.type),
 		function(error, response) {
 			handleResponse('insert', _id, error, response);
 		});
 }

/**
 * Set document properties
 * @param {[type]} _id [description]
 * @param {[type]} o   [description]
 */
 function set(_id, o) {
 	var partial = o['$set'];
 	var body = {};
 	_.mapKeys(partial, function(value, key) {
 		_.set(body, key, value)
 	});
 	client.update(_.assign({
 		id: _id,
 		body: {
 			doc: body
 		}
 	},
 	conf.type),
 	function(error, response) {
 		handleResponse('set', _id, error, response);
 	}
 	);

 }


/**
 * unset document properties
 * @type {[type]}
 */
 function unset(_id, o) {
 	var partial = o['$unset'];
 	var fields = _.keys(partial);
 	client.update(_.assign({
 		id: _id,
 		body: {
 			"script": "for(i=0; i < fields.size();i++){	ctx._source.remove(fields.get(i))}",
 			"params": {
 				"fields": fields
 			}
 		}
 	},
 	conf.type),
 	function(error, response) {
 		handleResponse('unset', _id, error, response);
 	});
 }


/**
 * Delte document from index
 * @param  {[type]} _id [description]
 * @return {[type]}     [description]
 */
 function remove(_id) {
 	client.delete(_.assign({
 		id: _id,
 	}, conf.type), function(error, response) {
 		handleResponse('remove', _id, error, response);
 	})
 }

 oplog.on('op', function(data) {
	//console.log(data);
});

 oplog.on('insert', function(doc) {
 	index(parse_id(doc), doc.o);
 });


 /*Handle Updates*/
 oplog.on('update', function(doc) {
	if (doc.o['$set']) { //SET VALUE
		set(parse_id(doc), doc.o);
	} else if (doc.o['$unset']) { //UNSET VALUE
		unset(parse_id(doc), doc.o);
	} else {
		index(parse_id(doc), doc.o);
	}

});

 oplog.on('delete', function(doc) {
 	remove(parse_id(doc));
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