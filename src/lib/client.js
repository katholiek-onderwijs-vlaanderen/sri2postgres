/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');
var pg = require('pg');
var Q = require('q');

// Constructor
function Client (config) {

    config = config || {};

    if (!config.hasOwnProperty('apiUrl')) {
        throw new Error('Api is not defined.');
    }

    this.apiUrl = config.apiUrl;
    this.apiCredentials = config.credentials;

    this.dbUser = config.dbUser;
    this.dbPassword = config.dbPassword;
    this.database = config.database;
    this.dbPort = config.dbPort;
    this.dbHost = config.dbHost;
    this.dbSsl = config.hasOwnProperty('dbSsl') ? config.dbSsl : false;
    this.dbTable = config.dbTable;

    this.postgresClient = null;

    this.createPostgresClient = function(){

        this.postgresClient = new pg.Client({
            user: this.dbUser,
            password: this.dbPassword,
            database: this.database,
            port: this.dbPort,
            host: this.dbHost,
            ssl: this.dbSsl
        });
    }
}

// class methods
Client.prototype.connect = function(next) {

    if ( this.postgresClient == null){
        this.createPostgresClient();
    }

    this.postgresClient.connect(function(err) {
        next(err);
    });
}

//Creating-NodeJS-modules-with-both-promise-and-callback-API-support-using-Q
Client.prototype.saveContent = function(table,callback) {

    var deferred = Q.defer();

    if (table) {
        var fullName = "TABLE:" + " " + table;
        deferred.resolve(fullName);
    }
    else {
        deferred.reject("table must be passed.");
    }

    deferred.promise.nodeify(callback);
    return deferred.promise;
}

Client.prototype.getApiContent = function(next) {
    needle.get(this.apiUrl,this.apiCredentials, function (error,response) {
        next(error,response);
    });
}

// export the class
module.exports = Client;