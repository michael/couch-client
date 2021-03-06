/* global Buffer */

var http  = require('http'),
    https = require('https'),
    Url   = require('url'),
    EventEmitter = require('events').EventEmitter,
    querystring = require('querystring');

var MAX_DOCS = 1000; // The maximum number of docs to send in a single batch
var noOp = function(err) { if (err) { throw err; } }

var CONNECTION_DEFAULTS = {
  host: '127.0.0.1:5984',
  port: 5984,
  hostname: '127.0.0.1',
  pathname: "/"
};

function CouchClient(url) {
  var uri = Url.parse(url);
  uri.protocolHandler = uri.protocol == 'http:' ? http : https;
  uri.__proto__ = CONNECTION_DEFAULTS;

  // A simple wrapper around node's http(s) request.
  function request(method, path, body, callback) {
    var stream;
    // Body is optional
    if (typeof body === 'function' && typeof callback === 'undefined') {
      callback = body;
      body = undefined;
    }
    
    // Return a stream if no callback is specified
    if (!callback) {
      stream = new EventEmitter();
      stream.setEncoding = function () {
        throw new Error("This stream is always utf8");
      };
    }

    function errorHandler(err) {
      if (callback) { callback(err); }
      if (stream) { stream.emit('error', err); }
    }

    var headers = {
      "Host": uri.hostname
    };
    
    if (uri.auth) {
      headers["Authorization"] = "Basic " + new Buffer(uri.auth, "ascii").toString("base64");
    }

    if (body) {
      body = JSON.stringify(body);
      headers["Content-Length"] = Buffer.byteLength(body);
      headers["Content-Type"] = "application/json";
    }

    var options = {
      host: uri.hostname,
      method: method,
      path: path,
      port: uri.port,
      headers: headers  
    };

    var request = uri.protocolHandler.request(options, function (response) {
      response.setEncoding('utf8');
      var body = "";
      response.on('data', function (chunk) {
        if (callback) { body += chunk; }
        if (stream) { stream.emit('data', chunk); }
      });
      response.on('end', function () {
        if (callback) { callback(null, JSON.parse(body)); }
        if (stream) { stream.emit('end'); }
      });
      response.on('error', errorHandler);
    });
    request.on('error', errorHandler);

    if (body) { request.write(body, 'utf8'); }
    request.end();
    return stream;
  }

  // Requests UUIDs from the couch server in tick batches
  var uuidQueue = [];
  function getUUID(callback) {
    uuidQueue.push(callback);
    if (uuidQueue.length > 1) { return; }
    function consumeQueue() {
      var pending = uuidQueue.splice(0, MAX_DOCS);
      if (uuidQueue.length) { process.nextTick(consumeQueue); }
      request("GET", "/_uuids?count=" + pending.length, function (err, result) {
        if (err) {
          pending.forEach(function (callback) {
            callback(err);
          });
          return;
        }
        if (result.uuids.length !== pending.length) {
          throw new Error("Wrong number of UUIDs generated " + result.uuids.length + " != " + pending.length);
        }
        result.uuids.forEach(function (uuid, i) {
          pending[i](null, uuid);
        });
      });
    }
    process.nextTick(consumeQueue);
  }

  // Saves documents in batches
  var saveValues = [];
  var saveQueue = [];
  function realSave(doc, callback) {
    // Put key and rev on the value without changing the original
    saveValues.push(doc);
    saveQueue.push(callback);
    if (saveQueue.length > 1) { return; }
    
    function consumeQueue() {
      var pending = saveQueue.splice(0, MAX_DOCS);
      var body = saveValues.splice(0, MAX_DOCS);
      if (saveQueue.length) { process.nextTick(consumeQueue); }
      request("POST", uri.pathname + "/_bulk_docs", {docs: body}, function (err, results) {
        if (!err && results && results.error) err = results;
        if (err) {
          pending.forEach(function (callback) {
            callback(err);
          });
          return;
        }
        results.forEach(function (result, i) {
          var doc = body[i];
          if (result.error && result.error === "conflict") {
            // handle update conflict
            pending[i](result);
          } else {
            doc._id = result.id;
            doc._rev = result.rev;
            pending[i](null, doc);
          }
        });
      });
    }
    process.nextTick(consumeQueue);
  }

  var getQueue = [];
  var getKeys = [];
  function realGet(key, includeDoc, callback) {
    getKeys.push(key);
    getQueue.push(callback);
    if (getQueue.length > 1) { return; }
    function consumeQueue() {
      var pending = getQueue.splice(0, MAX_DOCS);
      var keys = getKeys.splice(0, MAX_DOCS);
      if (getQueue.length) { process.nextTick(consumeQueue); }
      var path = uri.pathname + "/_all_docs";
      if (includeDoc) { path += "?include_docs=true"; }
      request("POST", path, {keys: keys}, function (err, results) {
        if (!results.rows) err = results;
        if (err) {
          pending.forEach(function (callback) {
            callback(err);
          });
          return;
        }
        results.rows.forEach(function (result, i) {
          var err;
          if (includeDoc) {
            if (result.error) {
              err = result;
              pending[i](err);
              return;
            }
            pending[i](null, result.doc);
            return;
          }
          pending[i](null, result.value);
        });
      });
    }
    process.nextTick(consumeQueue);
  }

  function save(doc, options, callback) {
    if (typeof options === 'function') {
      callback = options;
      options = null;
    }
    if (!callback) { callback = noOp; }
    if (doc._id) {
      if (!doc._rev && options && options.force) {
        realGet(doc._id, false, function (err, result) {
          if (err) { return callback(err); }
          if (result) doc._rev = result.rev;
          realSave(doc, callback);
        });
        return;
      }
    }
    realSave(doc, callback);
  }

  function get(key, callback) {
    realGet(key, true, callback);
  }

  function changes(since, callback) {
    var stream = request("GET", uri.pathname + "/_changes?feed=continuous&heartbeat=1000&since=" + since);
    var data = "";
    function checkData() {
      var p = data.indexOf("\n");
      if (p >= 0) {
        var line = data.substr(0, p).trim();
        data = data.substr(p + 1);
        if (line.length) {
          callback(null, JSON.parse(line));
        }
        checkData();
      }
    }
    
    stream.on('error', callback);
    stream.on('data', function (chunk) {
      data += chunk;
      checkData();
    });
    stream.on('end', function () {
      throw new Error("Changes feed got broken!");
    });
  }
  
  function view(path, obj, callback) {
  	var method = "GET";
  	var body = null;
    
    if (typeof obj === 'function') {
      callback = obj;
      obj = null;
    }
    
    path = path.split('/');
    path = [uri.pathname, '_design', path[0], '_view', path[1]].join('/');
    
    if (obj && typeof obj === 'object') {
      Object.keys(obj).forEach(function(key){
		    if ( key === 'keys' ) {
			    body = { keys: obj[key] }; // body is json stringified in request fn
			    method='POST';
		    } else {
			    obj[key] = JSON.stringify(obj[key]);
		    }
      });
      
      var getParams = querystring.stringify(obj);
      if (getParams) {
        path = path + '?' + getParams;
      }
    }
    request(method, path, body, function(err, result) {
      result && result.error ? callback(result) : callback(err, result);
    });
  }

  // Expose the public API
  return {
    get: get,
    save: save,
    changes: changes,
    request: request,
    uri: uri,
    view: view
  };
}

module.exports = CouchClient;