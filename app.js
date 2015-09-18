 /**
  * @author Sebastian Dimunzio
  */

 var Db = require('mongodb').Db,
 	MongoClient = require('mongodb').MongoClient,
 	Server = require('mongodb').Server,
 	ReplSetServers = require('mongodb').ReplSetServers,
 	ObjectID = require('mongodb').ObjectID,
 	_ = require('lodash'),
 	Promise = require('promise'),
 	MongoClient = require('mongodb').MongoClient,
 	MongoOplog = require('mongo-oplog'),
 	conf = require('./conf.js'),
 	elasticsearch = require('elasticsearch');

 var counter = 0;

 var mongoclient = new MongoClient(conf.target_db, {
 	native_parser: true
 });

 var oplog = MongoOplog('mongodb:' + conf.oplog_db, {}).tail();

 var client = new elasticsearch.Client({
 	host: conf.es,
 	log: conf.log
 });

 client.ping({
 	requestTimeout: Infinity,
 	hello: "elasticsearch!"
 }, function(error) {
 	if (error) {
 		logWithDate('Elasticsearch cluster is down');
 	} else {
 		logWithDate('Elastic Search is running');
 	}
 });



 /**
  * [parse_id description]
  * @param  {[type]} doc [description]
  * @return {[type]}     [description]
  */
 function parse_id(doc) {
 	return doc.o2 ? doc.o2._id.toString() : doc.o._id.toString();
 }



 function logWithDate(message) {
 	console.log((new Date().toISOString()) + '=>' + message);
 }

 /**
  * [handleResponse description]
  * @param  {[type]} ns       [description]
  * @param  {[type]} action   [description]
  * @param  {[type]} _id      [description]
  * @param  {[type]} error    [description]
  * @param  {[type]} response [description]
  * @return {[type]}          [description]
  */
 function handleResponse(ns, action, _id, error, response) {
 	if (error) {
 		logWithDate('ERROR: - ' + ns + ' - ' + action + ' - ' + error + _id);
 	} else {
 		counter++;
 		logWithDate(' - ' + ns + ' - ' + action + ' - ' + ' - ' + _id + ' - ' + counter);
 	}
 }

 /**
  * [index description]
  * @param  {[type]} ns     [description]
  * @param  {[type]} target [description]
  * @param  {[type]} _id    [description]
  * @param  {[type]} o      [description]
  * @return {[type]}        [description]
  */
 function index(ns, target, _id, o) {
 	client.index(
 		_.assign({
 			id: _id,
 			body: o
 		}, target, conf.options),
 		function(error, response) {
 			handleResponse(ns, 'insert', _id, error, response);
 		});
 }



 function indexDocumentFromDb(ns, target, _id) {
 	getOriginalDocument(ns, _id).then(
 		function(doc) {
 			logWithDate('Indexing from source database');
 			index(ns, target, _id, doc);
 		})

 }
 /**
  * [set description]
  * @param {[type]} ns     [description]
  * @param {[type]} target [description]
  * @param {[type]} _id    [description]
  * @param {[type]} o      [description]
  */
 function set(ns, target, _id, o) {
 	debugger;
 	/*Call set once we checked if routing is needed*/
 	var setFunction = function(_target) {
 		var partial = o['$set'];
 		var body = {};
 		target = target || _target;
 		_.mapKeys(partial, function(value, key) {
 			_.set(body, key, value)
 		});
 		client.update(_.assign({
 					id: _id,
 					body: {
 						doc: body
 					}
 				},
 				target, conf.options),
 			function(error, response) {
 				if (error && error.status == 404) {
 					logWithDate('Got 404 - ' + _id);
 					indexDocumentFromDb(ns, target, _id);
 				} else {
 					handleResponse(ns, 'set', _id, error, response);
 				}
 			}
 		);

 	};


 	if (target._parent && !target.routing) { //if routing is not present I should get the routing from the orginal document
 		var document = getOriginalDocument(ns, _id).then(
 			function(doc) {
 				var _target = getTarget(ns, doc);
 				logWithDate('Got target, calling set function now');
 				setFunction(_target); //call update after setting routing ;
 			});
 	} else {
 		setFunction(); //routing is not needed or already set;
 	}


 }


 /**
  * [unset description]
  * @param  {[type]} ns     [description]
  * @param  {[type]} target [description]
  * @param  {[type]} _id    [description]
  * @param  {[type]} o      [description]
  * @return {[type]}        [description]
  */
 function unset(ns, target, _id, o) {
 	var unSetFunction = function() {
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
 				target, conf.options),
 			function(error, response) {
 				if (error && error.status == 404) {
 					logWithDate('Got 404 - ' + _id);
 					indexDocumentFromDb(ns, target, _id);
 				} else {
 					handleResponse(ns, 'unset', _id, error, response);
 				}
 			});
 	}


 	if (target._parent && !target.routing) { //if routing is not present I should get the routing from the orginal document
 		var document = getOriginalDocument(ns, _id).then(
 			function(doc) {
 				logWithDate('Got target, calling unset function now');
 				var _target = getTarget(ns, doc);
 				unSetFunction(_target); //call update after setting routing ;
 			});
 	} else {
 		unSetFunction(); //routing is not needed or already set;
 	}

 }


 /**
  * [remove description]
  * @param  {[type]} ns     [description]
  * @param  {[type]} target [description]
  * @param  {[type]} _id    [description]
  * @return {[type]}        [description]
  */
 function remove(ns, target, _id) {
 	client.delete(_.assign({
 		id: _id,
 	}, target, conf.options), function(error, response) {
 		handleResponse(ns, 'remove', _id, error, response);
 	})
 }


 /**
  * [getTarget description]
  * @param  {[type]} ns       [description]
  * @param  {[type]} document [description]
  * @return {[type]}          [description]
  */
 function getTarget(ns, doc) {
 	debugger;
 	var target = conf.ns_mapping[ns];
 	if (target) {

 		if (target._parent) {
 			if (doc[target._parent]) {
 				_.assign(target, {
 					'routing': doc[target._parent].oid.toString(),
 					'parent': doc[target._parent].oid.toString()
 				})
 			}
 		}
 		return target;
 	} else {
 		return null;
 	}

 }

 function getOriginalDocument(ns, _id) {
 	return new Promise(function(resolve, reject) {
 		var server = new Server('localhost', 27017);
 		var db = ns.split('.')[0];
 		var name = ns.split('.')[1];
 		var db = new Db(db, server);
 		db.open(function(err, db) {
 			if (err) {
 				reject();
 			}
 			var collection = db.collection(name);

 			collection.findOne({
 					"_id": new ObjectID(_id)
 				},
 				function(err, item) {
 					logWithDate('Got original document for ' + name + ' ' + item._id);
 					db.close();
 					resolve(item);
 					if (err) {
 						logWithDate(err)
 						reject();
 					}
 				});

 		});
 	});



 }

 oplog.on('op', function(data) {
 	//console.log(data);
 });

 oplog.on('insert', function(doc) {
 	var target = getTarget(doc.ns, doc.o);
 	if (target) {
 		index(doc.ns, target, parse_id(doc), doc.o);
 	}
 });



 /*Handle Updates*/
 oplog.on('update', function(doc) {
 	var target = getTarget(doc.ns, doc.o);
 	if (target) {
 		if (doc.o['$set']) { //SET VALUE
 			set(doc.ns, target, parse_id(doc), doc.o);
 		} else if (doc.o['$unset']) { //UNSET VALUE
 			unset(doc.ns, target, parse_id(doc), doc.o);
 		} else {
 			index(doc.ns, target, parse_id(doc), doc.o);
 		}
 	} else {
 		logWithDate('nothing to do with ' + doc.ns);
 	}

 });

 oplog.on('delete', function(doc) {
 	var target = getTarget(doc.ns, doc.o);
 	if (target) {
 		remove(doc.ns, target, parse_id(doc));
 	} else {
 		logWithDate('nothing to do with ' + doc.ns);
 	}

 });

 oplog.on('error', function(error) {
 	logWithDate(error);
 });

 oplog.on('end', function() {
 	logWithDate('nothing more to do');
 });

 oplog.stop(function() {
 	logWithDate('Stopping');
 });