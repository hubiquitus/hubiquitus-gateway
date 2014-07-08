/**
 * @module gateway
 */

var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:gateway');
var sockjsLogger = hubiquitus.logger('hubiquitus:addons:gateway:sockjs');
var sockjs = require('sockjs');
var http = require('http');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var _ = require('lodash');
var tv4 = require('tv4');
var schemas = require('./lib/schemas');

var events = new EventEmitter();
events.setMaxListeners(0);

var defaultPort = 8888;
var defaultPath = '/hubiquitus';
var loginTimeout = 30000;
var defaultHeartbeatFreq = 5000;
var defaultClientTimeout = 2 * defaultHeartbeatFreq;


/**
 * TODO : Send an error when server refuse a login attempt or disconnect
 */

/**
 * Create a gateway
 * @param [session] {function} session actor
 * @param [login] {function} loginentication function
 * @param [opts] {object} options (heartbeatFreq, clientTimeout)
 * @returns {Gateway}
 */
exports.createGateway = function (session, login, opts) {
  if (!_.isFunction(session)) {
    logger.trace('session actor not provided or invalid; default implementation is used instead');
    session = null;
  }
  if (!_.isFunction(login)) {
    logger.trace('login procedure not provided or invalid; default implementation is used instead');
    login = null;
  }

  opts = opts || {};

  if (!_.isNumber(opts.heartbeatFreq)) opts.heartbeatFreq = defaultHeartbeatFreq;
  if (!_.isNumber(opts.clientTimeout)) opts.clientTimeout = defaultClientTimeout;

  return new Gateway(session, login, opts);
};

/**
 * Gateway constructor
 * @param [login] {function} auth function
 * @param [session] {function} session actor
 * @param [opts] {object} options (heartbeatFreq, clientTimeout)
 * @constructor
 */
function Gateway(session, login, opts) {
  EventEmitter.call(this);

  var _this = this;

  login = login || basicLogin;
  session = session || basicSession;

  _this.socks = {};
  _this.heartbeatFreq = opts.heartbeatFreq;
  _this.clientTimeout = opts.clientTimeout;

  var sockOptions = {
    log: function (level, message) {
      sockjsLogger[level](message);
    }
  };
  _this.sock = sockjs.createServer(sockOptions);

  _this.sock.on('connection', function (sock) {
    logger.debug('connection from ' + sock.remoteAddress + ' (using ' + sock.protocol + ')');

    _this.socks[sock.id] = sock;
    sock.loginTimeout = setTimeout(function () {
      logger.debug('authentication delay timeout !');
      disconnect(sock, 'login timeout');
    }, loginTimeout);
    sock.hb = Date.now();

    sock.on('data', function (data) {
      if (data === 'hb') {
        sock.hb = Date.now();
        return;
      }

      var msg = decode(data);
      if (msg && msg.type) {
        switch (msg.type) {
          case 'req':
            sock.identity ? processReq(sock, msg) : enqueueReq(sock, msg);
            break;
          case 'res':
            sock.identity && processRes(sock, msg);
            break;
          case 'login':
            processLogin(sock, msg.authData);
            break;
          case 'negotiate':
            sock.write(encode({type: 'negotiate', heartbeatFreq:_this.heartbeatFreq}));
            break;
          default:
            logger.warn('received unknown message type', msg);
        }
      } else {
        logger.warn('received malformat data', msg);
      }
    });

    sock.on('close', function () {
      if (sock.id) delete _this.socks[sock.id];
      disconnect(sock);
    });
  });

  function processLogin(sock, data) {
    if (sock.identity) {
      disconnect(sock, 'Already logged');
      return;
    }

    login(sock, data, function (err, identity) {
      clearTimeout(sock.loginTimeout);
      if (!err) {
        var duplicata = _.find(_this.socks, function (item) {
          return item.identity === identity;
        });

        if (duplicata) {
          logger.debug('login error : User with identity' + identity + ' already connected');
          disconnect(sock, 'Duplicated identity');
          return;
        }

        sock.identity = identity;
        logger.debug('login success from ' + sock.remoteAddress + '; identifier : ' + sock.identity);
        var feedBack = {type: 'login', content: {id: sock.identity}};
        sock.write(encode(feedBack));
        _this.emit('connected', sock.identity);
        processQueue(sock);
        hubiquitus.addActor(sock.identity, session, {sock: sock});
      } else {
        logger.debug('login error', err);
        disconnect(sock, 'Invalid credentials');
      }
    });
  }

  function processReq(sock, req) {
    if (tv4.validate(req, schemas.message)) {
      logger.trace('processing request', req);
      var cb = null;
      if (req.cb) {
        cb = function (err, res) {
          if (err && err.code === 'TIMEOUT') return; // timeout is delegated to client side
          res.id = req.id;
          res.type = 'res';
          sock.write(encode(res));
        };
      }
      hubiquitus.send(sock.identity, req.to, req.content, cb);
    } else {
      logger.warn('received malformat request', {err: tv4.error, req: req});
    }
  }

  function processRes(sock, res) {
    logger.debug('processing response', res);
    if (tv4.validate(res, schemas.message)) {
      events.emit('res|' + sock.identity + '|' + res.id, res);
    } else {
      logger.warn('received malformat response', {err: tv4.error, res: res});
    }
  }

  function enqueueReq(sock, req) {
    sock.queue = sock.queue || [];
    sock.queue.push(req);
  }

  function processQueue(sock) {
    if (!sock.queue) return;
    logger.debug('processing ' + sock.identity + ' queue (' + sock.queue.length + ' elements)');
    sock.queue && _.forEach(sock.queue, function (req) {
      processReq(sock, req);
    });
  }

  function disconnect(sock, msg) {
    if (msg) {
      var reason = {type: 'disconnect', content: msg};
      sock.write(encode(reason));
      setTimeout(function () {
        _disconnect(sock);
      }, 2000);
    } else {
      _disconnect(sock);
    }
  }

  function _disconnect(sock) {
    if (sock.identity) {
      _this.emit('disconnected', sock.identity);
      hubiquitus.removeActor(sock.identity);
      delete sock.identity;
    }
    sock.close();
  }

  /**
   * Send a heartbeat to all opened sockets
   */
  function sendHeartbeat() {
    _.forOwn(_this.socks, function (sock) {
      sock.write('hb');
    });
    setTimeout(sendHeartbeat, _this.heartbeatFreq);
  }
  sendHeartbeat();

  /**
   * Check that all clients respond to the heartbeat in time
   */
  function checkClientsHeartbeat() {
    var now = Date.now();
    _.forOwn(_this.socks, function (sock) {
      if (sock.hb + _this.clientTimeout < now) {
        disconnect(sock);
      }
    });
    setTimeout(checkClientsHeartbeat, 1000)
  }
  checkClientsHeartbeat();
}

util.inherits(Gateway, EventEmitter);

/**
 * Start the gateway
 * @param [server] {Server} server
 * @param [params] {object} parameters
 */
Gateway.prototype.start = function (server, params) {
  var _this = this;
  params = params || {};
  if (!params.port) params.port = defaultPort;
  if (!params.path) params.path = defaultPath;
  if (params.heartbeatFreq) _this.heartbeatFreq = params.heartbeatFreq;
  server = server || http.createServer();
  _this.sock.installHandlers(server, {prefix: params.path});

  server.on('error', function (err) {
    logger.debug('error', err);
    _this.emit('error', err);
  });

  server.on('listening', function () {
    logger.debug('started on port ' + params.port + ' (path : ' + params.path + ')');
    _this.emit('started');
  });

  server.on('close', function () {
    logger.debug('stopped');
    _this.emit('stopped');
  });

  server.listen(params.port);
};

/* basic implementations */
function basicLogin(sock, data, cb) {
  if (data && _.isString(data.username) && !_.isEmpty(data.username)) {
    cb && cb(null, data.username);
  } else {
    cb && cb('invalid identifier ' + data);
  }
}

function basicSession(req) {
  var reply = req.reply;
  delete req.reply;
  var eventName = 'res|' + this.sock.identity + '|' + req.id;
  req.cb && events.once(eventName, function (res) {
    reply(res.err, res.content);
  });
  setTimeout(function () {
    events.removeAllListeners(eventName);
  }, req.timeout);

  req.to = this.sock.identity;
  req.type = 'req';
  this.sock.write(encode(req));
}

/* encoding & decoding */

function encode(data) {
  var encodedData = null;
  try {
    encodedData = JSON.stringify(data);
  } catch (err) {
    logger.warn('failed encoding data', data);
  }
  return encodedData;
}

function decode(data) {
  var decodedData = null;
  try {
    decodedData = JSON.parse(data);
  } catch (err) {
    logger.warn('failed decoding data', data);
  }
  return decodedData;
}
