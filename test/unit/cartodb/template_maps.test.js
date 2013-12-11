var assert = require('assert')
  //, _ = require('underscore')
  , RedisPool = require('redis-mpool')
  , SignedMaps = require('../../../lib/cartodb/signed_maps.js')
  , TemplateMaps = require('../../../lib/cartodb/template_maps.js')
  , test_helper = require('../../support/test_helper')
  , Step = require('step')
  , tests = module.exports = {};

suite('template_maps', function() {

  // configure redis pool instance to use in tests
  var redis_pool = RedisPool(global.environment.redis);
  var signed_maps = new SignedMaps(redis_pool);
    
  test('does not accept template with unsupported version', function(done) {
    var tmap = new TemplateMaps(redis_pool, signed_maps);
    assert.ok(tmap);
    var tpl = { version:'6.6.6',
      name:'k', auth: {}, layergroup: {} };
    Step(
      function() {
        tmap.addTemplate('me', tpl, this);
      },
      function checkFailed(err) {
        assert.ok(err);
        assert.ok(err.message.match(/unsupported.*version/i), err);
        return null;
      },
      function finish(err) {
        done(err);
      }
    );
  });

  test('does not accept template with missing name', function(done) {
    var tmap = new TemplateMaps(redis_pool, signed_maps);
    assert.ok(tmap);
    var tpl = { version:'0.0.1',
      auth: {}, layergroup: {} };
    Step(
      function() {
        tmap.addTemplate('me', tpl, this);
      },
      function checkFailed(err) {
        assert.ok(err);
        assert.ok(err.message.match(/missing.*name/i), err);
        return null;
      },
      function finish(err) {
        done(err);
      }
    );
  });

  test('does not accept template with invalid name', function(done) {
    var tmap = new TemplateMaps(redis_pool, signed_maps);
    assert.ok(tmap);
    var tpl = { version:'0.0.1',
      auth: {}, layergroup: {} };
    var invalidnames = [ "ab|", "a b", "a@b", "1ab" ];
    var testNext = function() {
      if ( ! invalidnames.length ) { done(); return; }
      var n = invalidnames.pop();
      tpl.name = n;
      tmap.addTemplate('me', tpl, function(err) {
        if ( ! err ) {
          done(new Error("Unexpected success with invalid name '" + n + "'"));
        }
        else if ( ! err.message.match(/invalid.*name/i) ) {
          done(new Error("Unexpected error message with invalid name '" + n
            + "': " + err));
        }
        else {
          testNext();
        }
      });
    };
    testNext();
  });

  test('add, get and delete a valid template', function(done) {
    var tmap = new TemplateMaps(redis_pool, signed_maps);
    assert.ok(tmap);
    var expected_failure = false;
    var tpl_id;
    var tpl = { version:'0.0.1',
      name: 'first', auth: {}, layergroup: {} };
    Step(
      function() {
        tmap.addTemplate('me', tpl, this);
      },
      function addOmonimousTemplate(err, id) {
        if ( err ) throw err;
        tpl_id = id;
        assert.equal(tpl_id, 'first');
        expected_failure = true;
        // should fail, as it already exists
        tmap.addTemplate('me', tpl, this);
      },
      function getTemplate(err) {
        if ( ! expected_failure && err ) throw err;
        assert.ok(err);
        assert.ok(err.message.match(/already exists/i), err);
        tmap.getTemplate('me', tpl_id, this);
      },
      function delTemplate(err, got_tpl) {
        if ( err ) throw err;
        assert.deepEqual(got_tpl, tpl);
        tmap.delTemplate('me', tpl_id, this);
      },
      function finish(err) {
        done(err);
      }
    );
  });
    
});
