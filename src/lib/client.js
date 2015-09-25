/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');
var pg = require('pg');
var Q = require('q');
var Transaction = require('pg-transaction');
var QueryStream = require('pg-query-stream');
var retry = require('retry');

// Constructor
function Client (config) {

    config = config || {};

    if (!config.hasOwnProperty('baseApiUrl')) {
        throw new Error('Api is not defined.');
    }

    this.baseApiUrl = config.baseApiUrl;
    this.functionApiUrl = config.functionApiUrl;
    this.apiCredentials = config.hasOwnProperty('credentials') ? config.credentials : {};
    this.apiRetries = config.hasOwnProperty('apiRetries') ? config.apiRetries : 2;

    this.dbUser = config.dbUser;
    this.dbPassword = config.dbPassword;
    this.database = config.database;
    this.dbPort = config.dbPort;
    this.dbHost = config.dbHost;
    this.dbSsl = config.hasOwnProperty('dbSsl') ? config.dbSsl : false;
    this.dbTable = config.dbTable;

    this.apiTimeOut = config.hasOwnProperty('apiTimeOut') ? config.apiTimeOut : 0;

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
    var operation = retry.operation({retries: this.apiRetries});
    var self = this;

    this.apiCredentials.open_timeout = this.apiTimeOut;


    operation.attempt(function(attempt){

        if(attempt > 1){
            console.log("getApiContent retry attempt: "+attempt);
        }

        needle.get(self.baseApiUrl+self.functionApiUrl,self.apiCredentials, function (error,response) {

            if (operation.retry(error)) {
                return;
            }

            if (error) {
                return deferred.reject(operation.mainError());
            }

            //Doing this bind to keep Client instance reference.
            this.Client = self;
            deferred.resolve(response);
        });
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

            if (typeof  jsonData.body.results == 'undefined'){

                console.warn("SRI2POSTGRES: Retry operation for: " + client.baseApiUrl+client.functionApiUrl);
                return Q.fcall(function () {
                    return client.functionApiUrl;
                });

            }else{
                var composeObject = {filter: filter,jsonData: jsonData};
                return insertResources(composeObject);
            }

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

    console.log("SRI2POSTGRES: calling saveResources");
    recurse(filter,clientCopy);

    deferred.promise.nodeify(callback);
    return deferred.promise;
};


Client.prototype.deleteFromTable = function(propertyConfig){

    var deferred = Q.defer();

    var clientInstance = this;

    var deletionQuery = "DELETE FROM "+propertyConfig.targetTable;
    console.log("SRI2POSTGRES: deleteFromTable :: Started");
    this.postgresClient.query(deletionQuery, function (err) {
        console.log("SRI2POSTGRES: deleteFromTable :: end");
        if (err) {
            console.log("SRI2POSTGRES: deleteFromTable :: ERROR " + err);
            deferred.reject(new Error(err));
        }else{
            console.log("SRI2POSTGRES: deleteFromTable :: SUCCESS");
            clientInstance.propertyConfig = propertyConfig;
            deferred.resolve(clientInstance);
        }
    });

    return deferred.promise;
};


Client.prototype.readFromTable = function(sri2PostgresClient){

    var deferred = Q.defer();

    var database = new pg.Client({
        user: sri2PostgresClient.dbUser,
        password: sri2PostgresClient.dbPassword,
        database: sri2PostgresClient.database,
        port: sri2PostgresClient.dbPort,
        host: sri2PostgresClient.dbHost,
        ssl: sri2PostgresClient.dbSsl
    });

    console.log("SRI2POSTGRES: readFromTable :: Connecting to database");

    database.connect(function(error){

        console.log("SRI2POSTGRES: readFromTable :: Successfully Connected to database");

        if (error){
            console.log("SRI2POSTGRES: ERROR in readFromTable: " + error);
            return deferred.reject(error);
        }

        var sqlQuery = "SELECT key, "+sri2PostgresClient.propertyConfig.propertyName+" AS link FROM "+sri2PostgresClient.dbTable+" WHERE 1000000 = $1 ";
        var query = new QueryStream(sqlQuery, [1000000]);
        var stream = sri2PostgresClient.postgresClient.query(query);
        var count = 0;
        var resourcesSync = 0;
        var resourcesSyncInActualTransaction = 0;

        stream.on('data',function(chunk){

            stream.pause();
            count++;
            console.log("SRI2POSTGRES: readFromTable :: Asking content_as_text for: " + chunk.link);
            sri2PostgresClient.baseApiUrl = chunk.link;
            sri2PostgresClient.functionApiUrl = '';

            sri2PostgresClient.getApiContent().then(function(response){

                console.log("SRI2POSTGRES: readFromTable :: Obtained content_as_text for: " +response);

                var isBuffer = (response.body instanceof Buffer);

                if (response.body.length > 0 && !isBuffer){

                    console.log("SRI2POSTGRES: readFromTable :: preparing INSERT for " +chunk.key);

                    var data = response.body.replaceAll("'", "''");
                    var insertQuery  = "INSERT INTO "+sri2PostgresClient.propertyConfig.targetTable+" VALUES ('"+chunk.key+"',E'"+data+"')";
                    resourcesSyncInActualTransaction++;

                    database.query(insertQuery,function(queryError,response){


                        if (queryError){
                            console.log("SRI2POSTGRES: readFromTable :: ERROR INSERTING "+chunk.key+ ": "+queryError);
                        }else{
                            console.log("SRI2POSTGRES: readFromTable :: INSERT SUCCESSFULLY for " +chunk.key);
                        }
                        resourcesSync += resourcesSyncInActualTransaction;
                        stream.resume();
                    });
                }else{
                    console.log("SRI2POSTGRES: readFromTable :: AVOID inserting " +chunk.key);
                    console.log("SRI2POSTGRES: response.body.length: " + response.body.length + " - isBuffer: " + isBuffer );
                    stream.resume();
                }
            }).fail(function(getApiContentError){
                console.log("SRI2POSTGRES: readFromTable :: ERROR getApiContentError for " +chunk.key);
                console.log(getApiContentError);
                stream.resume();
            });
        });

        stream.on('end',function(){
            console.log("SRI2POSTGRES: readFromTable :: end stream");
            //TODO review async calls with the last element. 'end' event is being called first that the last 'data' event
            deferred.resolve({resourcesSync: resourcesSync, resourcesNotSync: count-resourcesSync});
        });
    });

    return deferred.promise;
};

Client.prototype.saveResourcesInProperty = function(propertyConfig,callback){

    var deferred = Q.defer();

    console.log("SRI2POSTGRES: saveResourcesInProperty :: Started");
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

Client.prototype.saveResourcesInPropertyWithoutTableDeletion = function(propertyConfig,callback){

    var deferred = Q.defer();

    console.log("SRI2POSTGRES: saveResourcesInPropertyWithoutTableDeletion :: Started");

    this.propertyConfig = propertyConfig;

        this.readFromTable(this)
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