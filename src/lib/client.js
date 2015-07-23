/**
 * Created by pablo on 23/07/15.
 */
module.exports = Client;

function Client(config) {

    config = config || {};

    if (!config.hasOwnProperty('api')) {
        throw new Error('Api is not defined.');
    }

    this.apiUrl = config.apiUrl;
    this.apiUser = config.apiUser;
    this.apiPassword = config.apiPassword;

    this.dbUser = config.dbUser;
    this.dbPassword = config.dbPassword;
    this.database = config.database;
    this.dbPort = config.dbPort;
    this.dbHost = config.dbHost;
    this.dbSsl = config.dbSsl;
}

Client.prototype.connect = function(next) {
    console.log("Connecting to PG");
}

Client.prototype.saveContent = function(next) {
    console.log("Saving content from API to PG");
}