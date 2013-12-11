
var _ = require('underscore')
    , Step       = require('step')
    , Windshaft = require('windshaft')
    , redisPool = new require('redis-mpool')(global.environment.redis)
    // TODO: instanciate cartoData with redisPool
    , cartoData  = require('cartodb-redis')(global.environment.redis)
    , templateMaps = new require('./template_maps.js')(redisPool)
    , Cache = require('./cache_validator');

var CartodbWindshaft = function(serverOptions) {

    if(serverOptions.cache_enabled) {
        console.log("cache invalidation enabled, varnish on ", serverOptions.varnish_host, ' ', serverOptions.varnish_port);
        Cache.init(serverOptions.varnish_host, serverOptions.varnish_port);
        serverOptions.afterStateChange = function(req, data, callback) {
            Cache.invalidate_db(req.params.dbname, req.params.table);
            callback(null, data);
        }
    }

    serverOptions.beforeStateChange = function(req, callback) {
        var err = null;
        if ( ! req.params.hasOwnProperty('dbuser') ) {
          err = new Error("map state cannot be changed by unauthenticated request!");
        }
        callback(err, req);
    }

    // boot
    var ws = new Windshaft.Server(serverOptions);

    // Override getVersion to include cartodb-specific versions
    var wsversion = ws.getVersion;
    ws.getVersion = function() {
      var version = wsversion();
      version.windshaft_cartodb = require('../../package.json').version;
      return version;
    }

    /**
     * Helper to allow access to the layer to be used in the maps infowindow popup.
     */
    ws.get(serverOptions.base_url + '/infowindow', function(req, res){
        ws.doCORS(res);
        Step(
            function(){
                serverOptions.getInfowindow(req, this);
            },
            function(err, data){
                if (err){
                    ws.sendError(res, {error: err.message}, 500, 'GET INFOWINDOW');
                    //res.send({error: err.message}, 500);
                } else {
                    res.send({infowindow: data}, 200);
                }
            }
        );
    });


    /**
     * Helper to allow access to metadata to be used in embedded maps.
     */
    ws.get(serverOptions.base_url + '/map_metadata', function(req, res){
        ws.doCORS(res);
        Step(
            function(){
                serverOptions.getMapMetadata(req, this);
            },
            function(err, data){
                if (err){
                    ws.sendError(res, {error: err.message}, 500, 'GET MAP_METADATA');
                    //res.send(err.message, 500);
                } else {
                    res.send({map_metadata: data}, 200);
                }
            }
        );
    });

    /**
     * Helper API to allow per table tile cache (and sql cache) to be invalidated remotely.
     * TODO: Move?
     */
    ws.del(serverOptions.base_url + '/flush_cache', function(req, res){
        ws.doCORS(res);
        Step(
            function flushCache(){
                serverOptions.flushCache(req, serverOptions.cache_enabled ? Cache : null, this);
            },
            function sendResponse(err, data){
                if (err){
                    ws.sendError(res, {error: err.message}, 500, 'DELETE CACHE');
                    //res.send(500);
                } else {
                    res.send({status: 'ok'}, 200);
                }
            }
        );
    });

    // ---- Template maps interface starts @{

    ws.dbOwnerByReq = function(req) {
        return cartoData.userFromHostname(req.headers.host);
    }

    var template_baseurl = serverOptions.base_url_notable + '/template';

    // Add a template
    ws.post(template_baseurl, function(req, res) {
      ws.doCORS(res);
      var that = this;
      var response = {};
      var cdbuser = ws.dbOwnerByReq(req);
      Step(
        function checkPerms(){
            cartoData.checkMapKey(req, this);
        },
        function addTemplate(err, authenticated) {
          if ( err ) throw err;
          if (authenticated !== 1) {
            err = new Error("Only authenticated user can create templated maps");
            err.http_status = 401;
            throw err;
          }
          var next = this;
          if ( ! req.headers['content-type'] || req.headers['content-type'].split(';')[0] != 'application/json' )
            throw new Error('template POST data must be of type application/json');
          var cfg = req.body;
          templateMaps.addTemplate(cdbuser, cfg, this);
        },
        function prepareResponse(err, tpl_id){
          if ( err ) throw err;
          // NOTE: might omit "cbduser" if == dbowner ...
          return { template_id: cbduser + '@' + tpl_id };
        },
        function finish(err, response){
            var statusCode = 200;
            if (err){
                response = { error: ''+err };
                if ( ! _.isUndefined(err.http_status) ) {
                  statusCode = err.http_status;
                }
                ws.sendError(res, response, statusCode, 'POST TEMPLATE', err.message);
            } else {
              res.send(response, statusCode);
            }
        }
      );
    });

    // ---- Template maps interface ends @}

    return ws;
}

module.exports = CartodbWindshaft;
