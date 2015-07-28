/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');
var pg = require('pg');

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

Client.prototype.saveContent = function(next) {
    console.log("Saving content from API to PG");
}

Client.prototype.getApiContent = function(next) {
    needle.get(this.apiUrl,this.apiCredentials, function (error,response) {
        next(error,response);
    });
}

// export the class
module.exports = Client;