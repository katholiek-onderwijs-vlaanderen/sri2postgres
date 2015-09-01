/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');
var pg = require('pg');
var Q = require('q');
var Transaction = require('pg-transaction');
var QueryStream = require('pg-query-stream');

// Constructor
function Client (config) {

    config = config || {};

    if (!config.hasOwnProperty('baseApiUrl')) {
        throw new Error('Api is not defined.');
    }

    this.baseApiUrl = config.baseApiUrl;
    this.functionApiUrl = config.functionApiUrl;
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

String.prototype.replaceAll = function(search, replace) {
    if (replace === undefined) {
        return this.toString();
    }
    return this.split(search).join(replace);
};

var insertResources = function(composeObject) {

    var deferred = Q.defer();

    var jsonData = composeObject.jsonData;
    var count = jsonData.body.results.length;
    var inserted = 0;

    var tx = new Transaction(this.Client.postgresClient);

    var errorFound = false;
    var insertQuery;

    tx.on('error', function(){
        errorFound = true;
    });

    tx.begin();

    for (var i = 0; i < count; i++){

        //first check if there is a filter
        if (composeObject.filter){

            var resource = jsonData.body.results[i];

            if ( composeObject.filter.isValid(resource) ){

                var key = composeObject.filter.getKeyFrom(resource);
                var value = composeObject.filter.getValueFrom(resource);
                value = value.replaceAll("'", "''");
                insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+value+"')";
                tx.query(insertQuery);
                inserted++;
            }
        }else{
            //process all of them
            var key = jsonData.body.results[i].$$expanded.key;
            var stringifiedJson = JSON.stringify(jsonData.body.results[i].$$expanded);
            stringifiedJson = stringifiedJson.replaceAll("'", "''");
            insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+stringifiedJson+"')";
            tx.query(insertQuery);
            inserted++;
        }

    }

    tx.commit(function(){

        if (errorFound){
            totalNotSync += Number(composeObject.jsonData.body.results.length);
        }else{
            totalSync += Number(inserted);
        }

        deferred.resolve(composeObject.jsonData.body.$$meta.next);
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
Client.prototype.saveResource = function(table,callback) {

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


Client.prototype.saveResources = function(filter,callback){

    var deferred = Q.defer();
    totalSync = 0;
    totalNotSync = 0;
    var clientCopy = this;

    function recurse(filter,client) {

        client.getApiContent().then(function(jsonData){

            var composeObject = {filter: filter,jsonData: jsonData};
            return insertResources(composeObject);

        }).then(function(nextPage){

            if (typeof nextPage == 'undefined'){
                client.updateDateSync();

                deferred.resolve({resourcesSync: totalSync,resourcesNotSync: totalNotSync });
            }else{
                client.functionApiUrl = nextPage;
                recurse(filter,client);
            }
        }).fail(function(error){
            deferred.reject(error);
        });
    }

    recurse(filter,clientCopy);

    deferred.promise.nodeify(callback);
    return deferred.promise;
};


Client.prototype.deleteFromTable = function(propertyConfig){

    var deferred = Q.defer();

    var clientInstance = this;

    var deletionQuery = "DELETE FROM "+propertyConfig.targetTable;
    this.postgresClient.query(deletionQuery, function (err) {
        if (err) {
            deferred.reject(new Error(err));
        }else{
            clientInstance.propertyConfig = propertyConfig;
            deferred.resolve(clientInstance);
        }
    });

    return deferred.promise;
};


Client.prototype.readFromTable = function(sri2PostgresClient){

    var deferred = Q.defer();
    var sqlQuery = "SELECT key, "+sri2PostgresClient.propertyConfig.propertyName+" AS link FROM "+sri2PostgresClient.dbTable+" WHERE 1000000 = $1 ";
    var query = new QueryStream(sqlQuery, [1000000]);
    var stream = sri2PostgresClient.postgresClient.query(query);
    var count = 0;
    var resourcesSync = 0;
    var resourcesSyncInActualTransaction = 0;

    var tx = new Transaction(sri2PostgresClient.postgresClient);
    tx.begin();

    tx.on('error',function(error){
        stream.pause();

        tx.abort();
        tx.begin();

        console.log("TRANSACTION ERROR: "+error+ ". "+resourcesSyncInActualTransaction+" resources will be discouraged");

        resourcesSyncInActualTransaction = 0;

        stream.resume();

    });

    stream.on('data',function(chunk){

        stream.pause();
        count++;

        sri2PostgresClient.baseApiUrl = chunk.link;
        sri2PostgresClient.functionApiUrl = '';

        sri2PostgresClient.getApiContent().then(function(response){

            var isBuffer = (response.body instanceof Buffer);

            if (response.body.length > 0 && !isBuffer){

                var data = response.body.replaceAll("'", "''");
                var insertQuery  = "INSERT INTO "+sri2PostgresClient.propertyConfig.targetTable+" VALUES ('"+chunk.key+"','"+data+"')";
                resourcesSyncInActualTransaction++;

                tx.query(insertQuery);

                if (count % sri2PostgresClient.propertyConfig.queriesPerTransaction == 0){

                    tx.commit(function(){
                        tx.begin();
                        resourcesSync += resourcesSyncInActualTransaction;
                        resourcesSyncInActualTransaction = 0;
                        stream.resume();
                    });
                }else{
                    stream.resume();
                }

            }else{
                stream.resume();
            }
        });
    });

    stream.on('end',function(){

        //TODO review async calls with the last element. 'end' event is being called first that the last 'data' event
        tx.commit(function(){
            resourcesSync += resourcesSyncInActualTransaction;
            deferred.resolve({resourcesSync: resourcesSync, resourcesNotSync: count-resourcesSync});
        });
    });

    return deferred.promise;
};

Client.prototype.saveResourcesInProperty = function(propertyConfig,callback){

    var deferred = Q.defer();

    // Delete all content from new database
    this.deleteFromTable(propertyConfig)
        .then(this.readFromTable)
        .then(function(response){
            deferred.resolve({resourcesSync: response.resourcesSync, resourcesNotSync: response.resourcesNotSync});
        }).fail(function(error){
            deferred.reject(error);
        });

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

// export the class
module.exports = Client;