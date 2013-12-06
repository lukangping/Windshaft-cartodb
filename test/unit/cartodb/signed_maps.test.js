var assert = require('assert')
  //, _ = require('underscore')
  , SignedMaps = require('../../../lib/cartodb/signed_maps.js')
  , test_helper = require('../../support/test_helper')
  , Step = require('step')
  , tests = module.exports = {};

suite('signed_maps', function() {

    // configure redis pool instance to use in tests
    var redis_opts = global.environment.redis;
    
    test.skip('can sign map with open auth', function(done) {
      var smap = new SignedMaps(redis_opts);
      assert.ok(smap);
      var sig = 'sig1';
      var map = 'map1';
      var tok = 'tok1';
      var crt = {
        version:'0.0.1',
        layergroup_id:map,
        auth: {}
      };
      var crt1_id, crt2_id;
      Step(
        function() {
          smap.isAuthorized(sig,map,tok,this);
        },
        function checkAuthFailure1(err, authorized) {
          if ( err ) throw err;
          assert.ok(!authorized, "unexpectedly authorized");
          crt.auth.method = 'token';
          crt.auth.valid_tokens = [tok];
          smap.addSignature(sig, map, crt, this)
        },
        function getCert1(err, id) {
          if ( err ) throw err;
          assert.ok(id, "undefined signature id");
          crt1_id = id; // keep note of it
          smap.isAuthorized(sig,map,'',this);
        },
        function checkAuthFailure2(err, authorized) {
          if ( err ) throw err;
          assert.ok(!authorized, "unexpectedly authorized");
          smap.isAuthorized(sig,map,tok,this);
        },
        function checkAuthSuccess1(err, authorized) {
          if ( err ) throw err;
          assert.ok(authorized, "unauthorized :(");
          crt.auth.method = 'open';
          delete crt.auth.valid_tokens;
          smap.addSignature(sig, map, crt, this)
        },
        function getCert2(err, id) {
          if ( err ) throw err;
          assert.ok(id, "undefined signature id");
          crt2_id = id; // keep note of it
          smap.isAuthorized(sig,map,'arbitrary',this);
        },
        function checkAuthSuccess2(err, authorized) {
          if ( err ) throw err;
          assert.ok(authorized, "unauthorized :(");
        },
        function deleteCert1(err) {
          if ( ! crt1_id ) {
            if ( err ) throw err;
            return null;
          }
          var next = this;
          smap.delSignature(crt1_id, function(e) {
            next(err ? err : e);
          });
        },
        function deleteCert2(err) {
          if ( ! crt2_id ) {
            if ( err ) throw err;
            return null;
          }
          var next = this;
          smap.delSignature(crt2_id, function(e) {
            next(err ? err : e);
          });
        },
        function finish(err) {
          done(err);
        }
      );
    });

    
});
