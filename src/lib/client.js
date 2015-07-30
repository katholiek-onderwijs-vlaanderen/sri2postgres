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
};

//Creating-NodeJS-modules-with-both-promise-and-callback-API-support-using-Q
Client.prototype.saveContent = function(table,callback) {

    var deferred = Q.defer();

    if (table) {
        this.dbTable = table;

        this.getApiContent().then(function(response){
            deferred.resolve(response.body);
        }).fail(function(error){
            deferred.reject(error);
        });
    }
    else {
        deferred.reject("table must be passed.");
    }

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

Client.prototype.getApiContent = function(next) {

    var deferred = Q.defer();

    needle.get(this.apiUrl,this.apiCredentials, function (error,response) {
        if (error) {
            deferred.reject(new Error(error));
        } else {
            deferred.resolve(response);
        }
    });

    deferred.promise.nodeify(next);
    return deferred.promise;
};

// export the class
module.exports = Client;