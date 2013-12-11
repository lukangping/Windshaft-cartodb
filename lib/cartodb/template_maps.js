var crypto    = require('crypto');
var Step      = require('step');
var _         = require('underscore');
//var SignedMaps = require('./signed_maps.js');

// Templates in this hash (keyed as <username>@<template_name>)
// are being worked on.
var user_template_locks = {};

// Class handling map templates
//
// See http://github.com/CartoDB/Windshaft-cartodb/wiki/Template-maps
//
// @param redis_pool an instance of a "redis-mpool"
//        See https://github.com/CartoDB/node-redis-mpool
//        Needs version 0.x.x of the API.
//
// @param signed_maps an instance of a "signed_maps" class,
//        See signed_maps.js
// 
function TemplateMaps(redis_pool, signed_maps) {
  this.redis_pool = redis_pool;
  this.signed_maps = signed_maps;

  // Database containing templates
  // TODO: allow configuring ?
  // NOTE: currently it is the same as
  //       the one containing layergroups
  this.db_signatures = 0;

  //
  // Map templates are owned by a user that specifies access permissions
  // for their instances.
  // 
  // We have the following datastores:
  //
  //  1. User teplates: set of per-user map templates
  //     NOTE: each template would have an associated auth
  //           reference, see signed_maps.js

  // User templates (HASH:tpl_id->tpl_val)
  this.key_usr_tpl = "map_tpl|<%= owner %>";

  // User template locks (HASH:tpl_id->ctime)
  this.key_usr_tpl_lck = "map_tpl|<%= owner %>|locks";

};

var o = TemplateMaps.prototype;

//--------------- PRIVATE METHODS --------------------------------

o._acquireRedis = function(callback) {
  this.redis_pool.acquire(this.db_signatures, callback);
};

o._releaseRedis = function(client) {
  this.redis_pool.release(this.db_signatures, client);
};

/**
 * Internal function to communicate with redis
 *
 * @param redisFunc - the redis function to execute
 * @param redisArgs - the arguments for the redis function in an array
 * @param callback - function to pass results too.
 */
o._redisCmd = function(redisFunc, redisArgs, callback) {
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
      if ( ! _.isUndefined(redisClient) ) that.redis_pool.release(db, redisClient);
      callback(err, data);
    }
  );
};

// @param callback function(err, obtained)
o._obtainTemplateLock = function(owner, tpl_id, callback) {
  var usr_tpl_lck_key = _.template(this.key_usr_tpl_lck, {owner:owner});
  var that = this;
  var gotLock = false;
  Step (
    function obtainLock() {
      var ctime = Date.now();
      that._redisCmd('HSETNX', [usr_tpl_lck_key, tpl_id, ctime], this);
    },
    function checkLock(err, locked) {
      if ( err ) throw err;
      if ( ! locked ) {
        // Already locked
        // TODO: unlock if expired ?
        throw new Error("Template '" + tpl_id + "' of user '" + owner + "' is locked");
      }
      return gotLock = true;
    },
    function finish(err) {
      callback(err, gotLock);
    }
  );
};

// @param callback function(err, deleted)
o._releaseTemplateLock = function(owner, tpl_id, callback) {
  var usr_tpl_lck_key = _.template(this.key_usr_tpl_lck, {owner:owner});
  this._redisCmd('HDEL', [usr_tpl_lck_key, tpl_id], callback);
};

//--------------- PUBLIC API -------------------------------------

// Add a template
//
// NOTE: locks user+template_name or fails
//
// @param owner cartodb username of the template owner
//
// @param template layergroup template, see
//        http://github.com/CartoDB/Windshaft-cartodb/wiki/Template-maps#template-format
//
// @param callback function(err, tpl_id) 
//        Return template identifier (only valid for given user)
//
o.addTemplate = function(owner, template, callback) {
  if ( template.version != '0.0.1' ) {
    callback(new Error("Unsupported template version " + template.version));
    return;
  }
  var tplname = template.name;
  if ( ! tplname ) {
    callback(new Error("Missing template name"));
    return;
  }
  if ( ! tplname.match(/^[a-zA-Z][0-9a-zA-Z_]*$/) ) {
    callback(new Error("Invalid characters in template name '" + tplname + "'"));
    return;
  }

  // TODO: run more checks over template format ?

  // Procedure:
  //
  // 0. Obtain a lock for user+template_name, fail if impossible
  // 1. Check no other template exists with the same name
  // 2. Install certificate extracted from template, extending
  //    it to contain a name to properly salt things out.
  // 3. Modify the template object to reference certificate by id
  // 4. Install template
  // 5. Release lock
  //
  //

  var usr_tpl_key = _.template(this.key_usr_tpl, {owner:owner});
  //var usr_tpl_lck_key = _.template(this.key_usr_tpl_lck, {owner:owner});
  var gotLock = false;
  var that = this;
  Step(
    // try to obtain a lock
    function obtainLock() {
      that._obtainTemplateLock(owner, tplname, this);
    },
    function getExistingTemplate(err, locked) {
      if ( err ) throw err;
      if ( ! locked ) {
        // Already locked
        throw new Error("Template '" + tplname + "' of user '" + owner + "' is locked");
      }
      gotLock = true;
      that._redisCmd('HEXISTS', [ usr_tpl_key, tplname ], this);
    },
    function installCertificate(err, exists) {
      if ( err ) throw err;
      if ( exists ) {
        throw new Error("Template '" + tplname + "' of user '" + owner + "' already exists"); 
      }
      var cert = template.auth;
      cert.template_id = tplname;
      that.signed_maps.addCertificate(owner, cert, this);
    },
    function installTemplate(err, crt_id) {
      if ( err ) throw err;
      delete template.auth.name;
      template.auth_id = crt_id;
      var tpl_val = JSON.stringify(template);
      that._redisCmd('HSET', [ usr_tpl_key, tplname, tpl_val ], this);
    },
    function releaseLock(err, newfield) {
      if ( ! err && ! newfield ) {
        console.log("ERROR: addTemplate overridden existing template '"
          + tplname + "' of '" + owner
          + "' -- HSET returned " + overridden + ": someone added it without locking ?");
        // TODO: how to recover this ?!
      }

      if ( ! gotLock ) {
        if ( err ) throw err;
        return null;
      }

      // release the lock
      var next = this;
      that._releaseTemplateLock(owner, tplname, function(e, d) {
        if ( e ) {
          console.log("Error removing lock on template '" + tplname
            + "' of user '" + owner + "': " + e);
        } else if ( ! d ) {
          console.log("ERROR: lock on template '" + tplname
            + "' of user '" + owner + "' externally removed!");
        }
        next(err);
      });
    },
    function finish(err) {
      callback(err, tplname);
    }
  );
};

// Delete a template
//
// NOTE: locks user+template_name or fails
//
// Also deletes associated authentication certificate, which
// in turn deletes all instance signatures
//
// @param owner cartodb username of the template owner
//
// @param tpl_id template identifier as returned
//        by addTemplate or listTemplates
//
// @param callback function(err)
//
o.delTemplate = function(owner, tpl_id, callback) {
  var usr_tpl_key = _.template(this.key_usr_tpl, {owner:owner});
  var usr_tpl_lck_key = _.template(this.key_usr_tpl_lck, {owner:owner});
  var gotLock = false;
  var that = this;
  Step(
    // try to obtain a lock
    function obtainLock() {
      that._obtainTemplateLock(owner, tpl_id, this);
    },
    function getExistingTemplate(err, locked) {
      if ( err ) throw err;
      if ( ! locked ) {
        // Already locked
        throw new Error("Template '" + tpl_id + "' of user '" + owner + "' is locked");
      }
      gotLock = true;
      that._redisCmd('HGET', [ usr_tpl_key, tpl_id ], this);
    },
    function delCertificate(err, tplval) {
      if ( err ) throw err;
      var tpl = JSON.parse(tplval);
      if ( ! tpl.auth_id ) {
        // not sure this is an error, in case we'll ever
        // allow unsigned templates...
        console.log("ERROR: installed template '" + tpl_id
            + "' of user '" + owner + "' has no auth_id reference: "); console.dir(tpl);
        return null;
      }
      var next = this;
      that.signed_maps.delCertificate(owner, tpl.auth_id, function(err) {
        if ( err ) {
          var msg = "ERROR: could not delete certificate '"
                  + tpl.auth_id + "' associated with template '"
                  + tpl_id + "' of user '" + owner + "': " + err;
          // I'm actually not sure we want this event to be fatal
          // (avoiding a deletion of the template itself) 
          next(new Error(msg));
        } else {
          next();
        }
      });
    },
    function delTemplate(err) {
      if ( err ) throw err;
      that._redisCmd('HDEL', [ usr_tpl_key, tpl_id ], this);
    },
    function releaseLock(err, deleted) {
      if ( ! err && ! deleted ) {
          console.log("ERROR: template '" + tpl_id
            + "' of user '" + owner + "' externally removed!");
      }

      if ( ! gotLock ) {
        if ( err ) throw err;
        return null;
      }

      // release the lock
      var next = this;
      that._releaseTemplateLock(owner, tpl_id, function(e, d) {
        if ( e ) {
          console.log("Error removing lock on template '" + tpl_id
            + "' of user '" + owner + "': " + e);
        } else if ( ! d ) {
          console.log("ERROR: lock on template '" + tpl_id
            + "' of user '" + owner + "' externally removed!");
        }
        next(err);
      });
    },
    function finish(err) {
      callback(err);
    }
  );
};

// Update a template
//
// NOTE: locks user+template_name or fails
//
// @param owner cartodb username of the template owner
//
// @param tpl_id template identifier as returned by addTemplate
//
// @param template layergroup template, see
//        http://github.com/CartoDB/Windshaft-cartodb/wiki/Template-maps#template-format
//
// @param callback function(err)
//        
o.updTemplate = function(owner, tpl_id, template, callback) {
  callback(new Error("Updating a template is not implemented yet"));
  // TODO: remove all instance signatures !
};

// List user templates
//
// @param owner cartodb username of the templates owner
//
// @param callback function(err, tpl_id_list)
//        Returns a list of template identifiers
// 
o.listTemplates = function(owner, callback) {
  callback(new Error("Listing templates is not implemented yet"));
};

// Get a templates
//
// @param owner cartodb username of the template owner
//
// @param tpl_id template identifier as returned
//        by addTemplate or listTemplates
//
// @param callback function(err, template)
//        Return full template definition
//
o.getTemplate = function(owner, tpl_id, callback) {
  var usr_tpl_key = _.template(this.key_usr_tpl, {owner:owner});
  var that = this;
  Step(
    function getTemplate() {
      that._redisCmd('HGET', [ usr_tpl_key, tpl_id ], this);
    },
    function parseTemplate(err, tpl_val) {
      if ( err ) throw err;
      var tpl = JSON.parse(tpl_val);
      // TODO: strip auth_id ?
      return tpl;
    },
    function finish(err, tpl) {
      callback(err, tpl);
    }
  );
};

module.exports = TemplateMaps;
