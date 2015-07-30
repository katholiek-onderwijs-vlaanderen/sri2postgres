/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');
var pg = require('pg');
var Q = require('q');
var uuid = require('node-uuid');

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

//private method
var insertData = function(jsonData) {

    var deferred = Q.defer();
    var key = uuid.v4();
    var insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+JSON.stringify(jsonData.body)+"')";

    this.Client.postgresClient.query(insertQuery, function (error, result) {
        if (error) {
            deferred.reject(new Error(error));
        } else {
            deferred.resolve(result);
        }
    });

    return deferred.promise;
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

        this.getApiContent().then(insertData).then(function(response){
            deferred.resolve(response);
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

    var clientCopy = this;

    // Implementing a wrapper to convert getApiContent in a Q Promise
    needle.get(this.apiUrl,this.apiCredentials, function (error,response) {
        if (error) {
            deferred.reject(new Error(error));
        } else {

            //Doing this bind to keep Client instance reference.
            this.Client = clientCopy;
            deferred.resolve(response);
        }
    });

    deferred.promise.nodeify(next);
    return deferred.promise;
};

// export the class
module.exports = Client;