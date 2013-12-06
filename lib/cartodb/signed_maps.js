var RedisPool = require('redis-mpool');
var crypto    = require('crypto');
var Step      = require('step');
var _         = require('underscore');

function SignedMaps(redis_opts) {
  this.redis_pool = new RedisPool(redis_opts);

  // Database containing signatures
  // TODO: allow configuring ?
  // NOTE: currently it is the same as
  //       the one containing layergroups
  this.db_signatures = 0;

  this.key_map_sig = "map_sig|<%= signer %>|<%= map %>";
  this.key_map_crt = "map_crt|<%= signer %>";
};

var o = SignedMaps.prototype;

o.acquireRedis = function(callback) {
  this.redis_pool.acquire(this.db_signatures, callback);
};

o.releaseRedis = function(client) {
  this.redis_pool.release(this.db_signatures, client);
};

// Check if shown credential are authorized to access a map
// by the given signer.
//
// @param signer a signer name (cartodb username)
// @param map a layergroup_id
// @param auth an authentication token, or undefined if none
//                    (can still be authorized by signature)
//
// @param callback function(Error, Boolean)
//
o.isAuthorized = function(signer, map, auth, callback) {
  console.log("Should check if any signature by " + signer
    + " authorizes credential " + auth
    + " to access map " + map);

  var that = this;
  var redis_client;
  var authorized = false;
  Step(
    function getRedisClient() {
      that.acquireRedis(this);
    },
    function checkSignatures(err, data) {
      if ( err ) throw err;
      redis_client = data;

      // TODO: loop over all signatures for the map,
      //       from the signer, and check if any gives
      //       access with the auth
      return null; 
    },
    function releaseRedisClient(err, data) {
      if ( ! _.isUndefined(redis_client) )
        that.releaseRedis(redis_client);
      callback(err, authorized);
    }
  );
};

// Add an authorization for anyone showing any given credential
// to access the given map as the given signer
//
// @param signer a signer name (cartodb username)
// @param map a layergroup_id
// @param cert signature certificate
//
// @param callback function(Error, String) return certificate id
//
o.addSignature = function(signer, map, cert, callback) {

  var crt_val = JSON.stringify(cert);
  var crt_id = crypto.createHash('md5').update(crt_val).digest('hex');

  var that = this;
  var redis_client;
  Step(
    function getRedisClient() {
      that.acquireRedis(this);
    },
    function execTransaction(err, data) {
      if ( err ) throw err;
      redis_client = data;

      // 0. Start a transaction
      var tx = redis_client.MULTI();

      // 1. Add certificate in user certificates list
      var key = _.template(that.key_map_crt, {signer:signer});
      tx.SADD(key, crt_val);

      // 2. Add cert reference in map signatures set
      var key = _.template(that.key_map_sig, {signer:signer, map:map});
      tx.SADD(key, crt_id);

      // COMMIT
      tx.EXEC(this);
    },
    function releaseRedisClient(err, data) {
      if ( ! _.isUndefined(redis_client) )
        that.releaseRedis(redis_client);
      callback(err, crt_id);
    }
  );
};

o.delSignature = function(cert_id, callback) {
  callback(new Error("Certificate revokal interface not implemented yet"));
  // LREM
};


/**
 * Use Redis
 *
 * @param redisFunc - the redis function to execute
 * @param redisArgs - the arguments for the redis function in an array
 * @param callback - function to pass results too.
 */
o.redisCmd = function(redisFunc, redisArgs, callback) {
  var redisClient;
  var that = this;
  var db = that.db_signatures;

  Step(
    function getRedisClient() {
      that.redis_pool.acquire(db, this);
    },
    function executeQuery(err, data) {
      if ( err ) throw err;
      redisClient = data;
      redisArgs.push(this);
      redisClient[redisFunc.toUpperCase()].apply(redisClient, redisArgs);
    },
    function releaseRedisClient(err, data) {
      if ( ! _.isUndefined(redisClient) ) redis_pool.release(db, redisClient);
      callback(err, data);
    }
  );
};

module.exports = SignedMaps;
