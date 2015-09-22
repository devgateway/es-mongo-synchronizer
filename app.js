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

 var storage = require('node-persist');
 storage.initSync({
   dir: __dirname + '/store',
   stringify: JSON.stringify,
   parse: JSON.parse,
   encoding: 'utf8',
   logging: false, // can also be custom logging function
   continuous: true,
   interval: false,
   ttl: false, // ttl* [NEW], can be true for 24h default or a number in MILLISECONDS
 });

 var counter = 0;
 console.log('last ts is: ' + storage.getItem('ts'));
 //   since: storage.getItem('ts')
 var Timestamp = require('mongodb').Timestamp;
 var ts;
 if (storage.getItem('ts')) {
   ts = Timestamp.fromString(storage.getItem('ts'));
 }

 /*Oplog Reader*/
 var oplog = MongoOplog('mongodb:' + conf.oplog_db, {
   'since': ts
 }).tail();

 /*Elastic search client*/
 var client = new elasticsearch.Client({
   host: conf.es,
   log: conf.log
 });


 //Check if ES is running
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
   active_request--;
   if (error) {
     logWithDate('ERROR: - ' + ns + ' - ' + action + ' - ' + error + _id + ' active:' + active_request);
   } else {
     counter++;
     logWithDate(ns + ' - ' + action + ' - ' + ' - ' + _id + ' - ' + counter + ' active:' + active_request);
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

   return new Promise(function(resolve, reject) {
     active_request++;
     client.index(
       _.assign({
         id: _id,
         body: o
       }, target, conf.options),
       function(error, response) {
         handleResponse(ns, 'insert', _id, error, response);
         resolve();
       });

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
   return new Promise(function(resolve, reject) {

     /*Call set once we checked if routing is needed*/
     var setFunction = function(_target) {
       active_request++;
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
           if (error && error.status == 409) {
             logWithDate('WArning got 409 document may not be updated propertly');
           } else if (error && error.status == 404) {
             logWithDate('Got 404 - ' + _id);
             indexDocumentFromDb(ns, target, _id);
           } else {
             handleResponse(ns, 'set', _id, error, response);
           }
           resolve(); //resolve promise
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

   });

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
   return new Promise(function(resolve, reject) {
     var unSetFunction = function() {

       active_request++;

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

           resolve(); //resolve promise
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
   });
 }


 /**
  * [remove description]
  * @param  {[type]} ns     [description]
  * @param  {[type]} target [description]
  * @param  {[type]} _id    [description]
  * @return {[type]}        [description]
  */
 function remove(ns, target, _id) {
   return new Promise(function(resolve, reject) {
     client.delete(_.assign({
       id: _id,
     }, target, conf.options), function(error, response) {
       handleResponse(ns, 'remove', _id, error, response);
       resolve(); //resolve promise
     })
   })
 }


 /**
  * [getTarget description]
  * @param  {[type]} ns       [description]
  * @param  {[type]} document [description]
  * @return {[type]}          [description]
  */
 function getTarget(ns, doc) {
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


 var max_active_request = 100;
 var active_request = 0;

 function setTs(ts) {
  // console.log('savig ts' + ts);
   storage.setItem('ts', ts);

 }

 function doInsert(doc) {

   if (max_active_request > active_request) {
     setTs(doc.ts);
     var target = getTarget(doc.ns, doc.o);
     if (target) {
       index(doc.ns, target, parse_id(doc), doc.o).then(function() {

       });
     }
   } else {
     setTimeout(function() {
       doInsert(doc)
     }, 0)
   }
 }

 function doUpdate(doc) {
   if (max_active_request > active_request) {
     setTs(doc.ts);

     var target = getTarget(doc.ns, doc.o);
     if (target) {
       if (doc.o['$set']) { //SET VALUE
         set(doc.ns, target, parse_id(doc), doc.o).then(function() {

         });
       } else if (doc.o['$unset']) { //UNSET VALUE
         unset(doc.ns, target, parse_id(doc), doc.o).then(function() {
           setTs(doc.ts)
         });
       } else {
         index(doc.ns, target, parse_id(doc), doc.o).then(function() {
           setTs(doc.ts)
         });
       }
     } else {
       logWithDate('nothing to do with ' + doc.ns);
     }
   } else {
     //console.log('delaying request')
     setTimeout(function() {
       doUpdate(doc)
     }, 0)
   }
 }

 oplog.on('op', function(data) {

   //logWithDate('Last ts id' + data.ns + '- ts ' + data.ts);
 });


 oplog.on('insert', function(doc) {
   doInsert(doc);
 });


 /*Handle Updates*/
 oplog.on('update', function(doc) {
   doUpdate(doc);
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