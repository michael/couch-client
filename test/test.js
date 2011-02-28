var assert = require('assert');
var CouchClient = require('../lib/couch-client');
var db = CouchClient("http://127.0.0.1:5984/test");
var async = require('async');

// CouchClient#save
// --------------

// Insert 3,000 documents with specified ids in a single sync block of code
// The driver should automatically chunk these into parallel requests for best
// performance.

function testSave(callback) {
  var i = 3000;
  var fns = []
  while (i--) {
    fns.push(function(callback) {
      db.save({content: "Foo"}, function (err, result) {
        assert.ok(!err);
        callback();
      });
    });
  }
  
  async.parallel(fns, function(err, results) {
    // Conflict handling, some save operations may fail due to conflicts
    db.save({_id:"foo", content: "Foo"}, function (err, result) {
      assert.ok(!err);
      
      db.save({_id:"bar", content: "Bar"}, function (err, result) {
        assert.ok(!err); // no conflict here
        
        db.save({_id:"foo", content: "Conflicting"}, function (err, result) {
          assert.ok(err.error == 'conflict'); // detect conflict!

          // Force update (ignore conflicts and overwrite)
          db.save({_id:"foo", content: "Forced update"}, {force: true}, function (err, doc) {
            assert.ok(!err);
            assert.ok(doc);
            callback();
          });
        });
      });
    });
  });
}


// CouchClient#get
// --------------

function testGet(callback) {
  db.get('foo', function(err, doc) {
    assert.ok(!err);
    assert.ok(doc);
    
    db.get('the_doc_that_wasnt_there', function(err, doc) {
      assert.ok(err);
      assert.ok(!doc);
      callback();
    });
  });
}

// CouchClient#view
// --------------

function testView(callback) {
  var view = {
    _id: '_design/queries',
    views: {
      "by_content": {
        "map": "function(doc) { emit(doc.content, doc); }"
      }
    }
  };
  
  db.save(view, function (err, doc) {
    db.view('queries/by_content', {limit: 10}, function(err, result) {
      assert.ok(result.rows.length === 10);
      assert.ok(!err);
      
      db.view('queries/not_existing_view', function(err, result) {
        assert.ok(err);
        assert.ok(!result);
        callback();
      });
    });
  });
}

// CouchClient#request - with basic-auth support
// --------------

function testRequest(callback) {
  db.request('PUT', '/_config/admins/couch-client', 'test', function(err, doc) {
    assert.equal('', doc);
    db.request('PUT', '/secure_test', function(err, doc) {
      assert.ok(doc.error && doc.error == 'unauthorized');
      assert.ok(doc.reason == 'You are not a server admin.')

      var securedb = CouchClient('http://couch-client:test@127.0.0.1:5984/');
      securedb.request('PUT', '/secure_test', function(err, doc) {
        assert.ok(doc.ok);

        securedb.request('DELETE', '/secure_test', function(err, doc) {
          assert.ok(doc.ok);
        });

        securedb.request('DELETE', '/_config/admins/couch-client', function(err, doc) {
          assert.ok(!err);
          callback();
        }); 
      });
    });
  }); 
}

// Flush DB and perform tests
db.request("DELETE", db.uri.pathname, function (err) {
  if (err) console.log(err);
  db.request("PUT", db.uri.pathname, function(err) {
    if (err) console.log(err);
    async.series([
      testSave,
      testGet,
      testView
    ], function(err) {
      console.log('Tests completed.');
    });
  });
});