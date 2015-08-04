/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');
var pg = require('pg');
var Q = require('q');
var Transaction = require('pg-transaction');

// Constructor
function Client (config) {

    config = config || {};

    if (!config.hasOwnProperty('baseApiUrl')) {
        throw new Error('Api is not defined.');
    }

    this.baseApiUrl = config.baseApiUrl;
    this.functionApiUrl = config.functionApiUrl
    this.apiCredentials = config.credentials;

    this.dbUser = config.dbUser;
    this.dbPassword = config.dbPassword;
    this.database = config.database;
    this.dbPort = config.dbPort;
    this.dbHost = config.dbHost;
    this.dbSsl = config.hasOwnProperty('dbSsl') ? config.dbSsl : false;
    this.dbTable = config.dbTable;

    this.lastSync = null;

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
    };

    this.updateDateSync = function() {
        this.lastSync = new Date();
    };

}

var totalSync = 0;
var totalNotSync = 0;

var insertResources = function(jsonData) {

    var deferred = Q.defer();
    var count = jsonData.body.results.length;

    var tx = new Transaction(this.Client.postgresClient);

    var errorFound = false;

    tx.on('error', function(error){
        //hast to pass the next but, storing that this one was skipped
        errorFound = true;
    });

    tx.begin();

    for (var i = 0; i < count; i++){
        var key = jsonData.body.results[i].$$expanded.key;
        var insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+JSON.stringify(jsonData.body.results[i].$$expanded)+"')";
        tx.query(insertQuery);
    }

    tx.commit(function(){

        if (errorFound){
            totalNotSync += Number(jsonData.body.results.length);
        }else{
            totalSync += Number(jsonData.body.results.length);
        }

        deferred.resolve(jsonData.body.$$meta.next);
    });

    return deferred.promise;
};

var updateData = function(jsonData){
    var deferred = Q.defer();
    var key = jsonData.body.key;
    var updateQuery  = "UPDATE "+this.Client.dbTable+" SET details = '"+JSON.stringify(jsonData.body)+"' WHERE key = '"+key+"'";

    this.Client.postgresClient.query(updateQuery, function (error, result) {
        if (error) {
            deferred.reject(new Error(error));
        } else {
            deferred.resolve(result);
        }
    });

    return deferred.promise;
};

//private method
var insertData = function(jsonData) {

    var deferred = Q.defer();
    var key = jsonData.body.key;
    var insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+JSON.stringify(jsonData.body)+"')";

    this.Client.postgresClient.query(insertQuery, function (error, result) {

        //error.code == 23505 UNIQUE VIOLATION
        if (error && error.code == 23505) {

            updateData(jsonData).then(function(response){
                deferred.resolve(response);
            }).fail(function(error){
                deferred.reject(new Error(error));
            });

        } else {
            deferred.resolve(result);
        }
    });

    return deferred.promise;
};

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

    if ( !this.dbTable && !table){
        deferred.reject("table must be passed.");
    }else{

        if (table) {
            this.dbTable = table;
        }

        this.getApiContent().then(insertData).then(function(response){
            this.Client.updateDateSync();
            deferred.resolve(response);
        }).fail(function(error){
            deferred.reject(error);
        });
    }

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

Client.prototype.getApiContent = function(next) {

    var deferred = Q.defer();

    var clientCopy = this;

    // Implementing a wrapper to convert getApiContent in a Q Promise
    needle.get(this.baseApiUrl+this.functionApiUrl,this.apiCredentials, function (error,response) {
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

Client.prototype.saveResources = function(){

    var deferred = Q.defer();

    function recurse() {

        this.Client.getApiContent().then(insertResources).then(function(nextPage){

            if (typeof nextPage == 'undefined'){
                this.Client.updateDateSync();

                deferred.resolve({resourcesSync: totalSync,resourcesNotSync: totalNotSync });
            }else{
                this.Client.functionApiUrl = nextPage;
                recurse();
            }
        }).fail(function(error){
            deferred.reject(error);
        });
    }

    recurse();

    return deferred.promise;
}

// export the class
module.exports = Client;