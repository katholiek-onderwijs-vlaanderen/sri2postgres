/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');

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
    this.dbSsl = config.dbSsl;
    this.dbTable = config.dbTable;
}

// class methods
Client.prototype.connect = function(next) {
    console.log("Connecting to PG");
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