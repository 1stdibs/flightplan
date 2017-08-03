var Fiber = require('fibers')
  , Future = require('fibers/future')
  , extend = require('util-extend')
  , SSH = require('../transport/ssh')
  , logger = require('../logger')()
  , prettyTime = require('pretty-hrtime')
  , errors = require('../errors')
  , dns = require('dns');

var _connections = [];

function hostNameCheck(_context, callback) {
    var options = {
      family: 4,
      hints: dns.ADDRCONFIG | dns.V4MAPPED,
    };
    var hostName = _context.remote.host;
    dns.lookup(hostName, options, function (err, address) {
      if(err) { throw new errors.ConnectionFailedError("Hostname not found: '" + _context.remote.host + "': " + err.message); }
      else { logger.info("Validated hostname '" + _context.remote.host + "': " + address); callback(); }
    });
}

function connect(_context) {
  var future = new Future();

  hostNameCheck(_context, function() {
    new Fiber(function() {
      logger.info("Connecting to '" + _context.remote.host + "'");

      try {
        var connection = new SSH(_context);
        _connections.push(connection);
      } catch(e) {
        if(!_context.remote.failsafe) {
          throw new errors.ConnectionFailedError("Error connecting to '" +
                                _context.remote.host + "': " + e.message);
        }

        logger.warn("Safely failed connecting to '" +
                      _context.remote.host + "': " + e.message);
      }

      return future.return();
    }).run();
  });

  return future;
}

function execute(transport, fn) {
  var future = new Future();

  new Fiber(function() {
    var t = process.hrtime();

    logger.info('Executing remote task on ' + transport.runtime.host);

    fn(transport);

    logger.info('Remote task on ' + transport.runtime.host +
                ' finished after ' + prettyTime(process.hrtime(t)));

    return future.return();
  }).run();

  return future;
}

exports.run = function(fn, context) {
  if(_connections.length === 0) {
    Future.wait(context.hosts.map(function(host) {
      var _context = extend({}, context);
      _context.remote = host;

      return connect(_context);
    }));
  }

  Future.wait(_connections.map(function(connection) {
    return execute(connection, fn);
  }));
};

exports.disconnect = function() {
  _connections.forEach(function(connection) {
    connection.close();
  });

  _connections = [];
};