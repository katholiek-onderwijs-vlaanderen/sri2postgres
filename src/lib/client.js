/**
 * Created by pablo on 23/07/15.
 */

var needle = require('needle');
var pg = require('pg');
var Q = require('q');
var Transaction = require('pg-transaction');
var QueryStream = require('pg-query-stream');
var retry = require('retry');
var sriClient;

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
    this.logging = config.hasOwnProperty('logging') ? config.logging : false;
    this.dbTable = config.dbTable;
    this.resourceType = config.hasOwnProperty('resourceType') ? config.resourceType : 'document';
    this.requiredByRoot = config.hasOwnProperty('requiredByRoot') ? config.requiredByRoot : undefined;

    this.encodeURL = config.hasOwnProperty('encodeURL') ? config.encodeURL : true;

    this.apiTimeOut = config.hasOwnProperty('apiTimeOut') ? config.apiTimeOut : 0;

    this.lastSync = null;
    this.postgresClient = null;

    this.connectionString = config.hasOwnProperty('connectionString') ? config.connectionString : '';

    this.sriClientParams = config.hasOwnProperty('sriClientParams') ? config.sriClientParams : {};
    this.sriClientOptions = config.hasOwnProperty('sriClientOptions') ? config.sriClientOptions : {};

    const configuration = {
        baseUrl: config.baseApiUrl
    };

    if (this.apiCredentials.username) {
        configuration.username = this.apiCredentials.username;
        configuration.password = this.apiCredentials.password;
        configuration.headers = this.apiCredentials.headers;
        if (this.logging) {
            configuration.logging = 'debug';
        }
    }

    sriClient = require('@kathondvla/sri-client/node-sri-client')(configuration);

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
                insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+value+"','"+this.Client.resourceType+"')";
                tx.query(insertQuery);
                inserted++;

                if (typeof this.Client.requiredByRoot != 'undefined' ){
                    var insertRootQuery  = "INSERT INTO "+this.Client.requiredByRoot.table+" VALUES ('"+key+"','"+this.Client.requiredByRoot.key+"','"+this.Client.resourceType+"')";
                    tx.query(insertRootQuery);
                }
            }
        }else{
            //process all of them
            var key = jsonData.body.results[i].$$expanded.key;
            var stringifiedJson = JSON.stringify(jsonData.body.results[i].$$expanded);
            stringifiedJson = stringifiedJson.replaceAll("'", "''");
            insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+stringifiedJson+"','"+this.Client.resourceType+"')";
            tx.query(insertQuery);
            inserted++;

            if (typeof this.Client.requiredByRoot != 'undefined' ){
                var insertRootQuery  = "INSERT INTO "+this.Client.requiredByRoot.table+" VALUES ('"+key+"','"+this.Client.requiredByRoot.key+"','"+this.Client.resourceType+"')";
                tx.query(insertRootQuery);
            }
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

    if (this.postgresClient == null && this.connectionString != '') {

        var self = this;

        pg.connect(this.connectionString, function (databaseConnectionError, dbClient, done) {
            self.postgresClient = dbClient;
            next(databaseConnectionError);
        });

    }else{

        if ( this.postgresClient == null){
            this.createPostgresClient();
        }

        this.postgresClient.connect(function(err) {
            next(err);
        });
    }
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

Client.prototype.getURL = function(){

    var url = this.functionApiUrl;

    if ( this.encodeURL ){
        url =  encodeURI(url);
    }

    return url;
};


Client.prototype.logMessage = function(message) {

    if ( this.logging ){
        console.log(message);
    }

};


Client.prototype.getApiContent = function(next) {

    var deferred = Q.defer();
    var self = this;

    sriClient.get(this.getURL(),this.sriClientParams,this.sriClientOptions)
      .then(function(result){

          this.Client = self;

          var response = {
              body: result,
              statusCode: 200
          };
          deferred.resolve(response);
      })
      .catch(function(error){
          deferred.reject({
              statusMessage: error,
              statusCode: 400
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

            if (jsonData.statusCode != 200){
                client.logMessage("SRI2POSTGRES: Error "+jsonData.statusCode+" when getting: " + client.baseApiUrl+client.functionApiUrl + " | Error Message: " + jsonData.statusMessage);
                deferred.reject(jsonData.statusMessage);
            }else{
                var composeObject = {filter: filter,jsonData: jsonData};
                return insertResources(composeObject);
            }

        }).then(function(nextPage){

            if (nextPage === undefined){
                client.updateDateSync();
                clientCopy.postgresClient.end();
                deferred.resolve({resourcesSync: totalSync,resourcesNotSync: totalNotSync });
            }else{
                client.logMessage("SRI2POSTGRES: saveResources nextPage: " + nextPage);
                client.functionApiUrl = nextPage;
                recurse(filter,client);
            }
        }).fail(function(error){
            clientCopy.postgresClient.end();
            deferred.reject(error);
        });
    }

    this.logMessage("SRI2POSTGRES: calling saveResources");
    recurse(filter,clientCopy);

    deferred.promise.nodeify(callback);
    return deferred.promise;
};


Client.prototype.deleteFromTable = function(propertyConfig){

    var deferred = Q.defer();

    var clientInstance = this;

    var deletionQuery = "DELETE FROM "+propertyConfig.targetTable+ " WHERE type = '"+clientInstance.resourceType+"'";

    this.logMessage("SRI2POSTGRES: deleteFromTable :: Started");

    this.postgresClient.query(deletionQuery, function (err) {

        clientInstance.logMessage("SRI2POSTGRES: deleteFromTable :: end");

        if (err) {
            clientInstance.logMessage("SRI2POSTGRES: deleteFromTable :: ERROR " + err);
            deferred.reject(new Error(err));
        }else{

            clientInstance.logMessage("SRI2POSTGRES: deleteFromTable :: SUCCESS");

            clientInstance.propertyConfig = propertyConfig;
            deferred.resolve(clientInstance);
        }
    });

    return deferred.promise;
};

var saveError = function (key,link,code,message,database,table,type){
    var deferred = Q.defer();

    var insertEmptyTextQuery = "INSERT INTO "+table+" VALUES ('"+key+"',E'','"+type+"'); ";
    var errorInsertQuery  = "INSERT INTO content_as_text_errors VALUES ('"+key+"','"+link+"','"+code+"','"+message+"')";
    database.query(insertEmptyTextQuery+errorInsertQuery,function(queryError){
        if (queryError){
            console.error(message + " " +code);
            console.error(key);
            console.error(link);
            console.error("--*--");
        }
        deferred.resolve();
    });

    return deferred.promise;
};

Client.prototype.readFromTable = function(sri2PostgresClient){

    var deferred = Q.defer();

    sri2PostgresClient.logMessage("SRI2POSTGRES: readFromTable :: Connecting to database");

    pg.connect(sri2PostgresClient.connectionString, function (error, database, done) {

        if (error){
            sri2PostgresClient.logMessage("SRI2POSTGRES: ERROR in readFromTable: " + error);
            return deferred.reject(error);
        }

        var offset = sri2PostgresClient.propertyConfig.hasOwnProperty('offset') ? sri2PostgresClient.propertyConfig.offset : 0;
        var limit = sri2PostgresClient.propertyConfig.hasOwnProperty('limit') ? sri2PostgresClient.propertyConfig.limit : 1000000;

        // SELECT key, obj->>'href' as link FROM jsonb, jsonb_array_elements(value->'attachments') obj WHERE type = 'curriculumzill' AND obj->>'type' = 'CONTENT_AS_TEXT' ORDER BY key LIMIT 5000 OFFSET 0

        var sqlQuery = "SELECT key, "+sri2PostgresClient.propertyConfig.propertyName+" AS link";
        sqlQuery += " FROM "+sri2PostgresClient.dbTable+" "+sri2PostgresClient.propertyConfig.fromExtraConditions;
        sqlQuery += " WHERE type = '"+sri2PostgresClient.resourceType+"' "+sri2PostgresClient.propertyConfig.whereExtraConditions;
        sqlQuery += " ORDER BY key LIMIT $1 OFFSET "+offset;
        var query = new QueryStream(sqlQuery, [limit]);
        var stream = sri2PostgresClient.postgresClient.query(query);
        var count = 0;
        var resourcesSync = 0;
        var queue = 0;

        function handleStreamFlow(){
            if (stream.readable){
                queue--;
                stream.resume();
            }else{
                database.end();
                deferred.resolve({resourcesSync: resourcesSync, resourcesNotSync: count-resourcesSync});
            }
        }

        stream.on('data',function(chunk){

            stream.pause();
            count++;
            queue++;

            var originalLink = chunk.link;
            var res = originalLink.split("/");
            var sourceName = res[res.length-1];
            sourceName = encodeURIComponent(sourceName);
            var componentUrl = "/" + res[1] + "/" +res[2] + "/" + sourceName;

            sri2PostgresClient.functionApiUrl = componentUrl;

            sri2PostgresClient.getApiContent().then(function(response){

                //console.log("SRI2POSTGRES: readFromTable :: Obtained content_as_text for: " + chunk.link);

                if (response.statusCode == 200 ){

                    var isBuffer = (response.body instanceof Buffer);

                    if (response.body.length > 0 && !isBuffer){

                        //console.log("SRI2POSTGRES: readFromTable ["+count+"] :: preparing INSERT for " +chunk.key);

                        var data = response.body.replaceAll("'", "''");
                        // After replacing ' -> '' there are still cases where \'' brake the query, so
                        // we need to transform \'' -> '' to correctly insert it.
                        data = data.replaceAll("\\''", "''");

                        var insertQuery  = "INSERT INTO "+sri2PostgresClient.propertyConfig.targetTable+" VALUES ('"+chunk.key+"',E'"+data+"','"+sri2PostgresClient.resourceType+"')";

                        database.query(insertQuery,function(queryError){

                            if (queryError){
                                saveError(chunk.key,chunk.link,0,queryError.message,database,sri2PostgresClient.propertyConfig.targetTable,sri2PostgresClient.resourceType);
                            }else{
                                resourcesSync++;
                                sri2PostgresClient.logMessage("SRI2POSTGRES: readFromTable :: [ "+resourcesSync+"/"+count+" ]  INSERT SUCCESSFULLY for " +chunk.key);
                            }

                            handleStreamFlow();

                        });
                    }else{

                        var message = isBuffer ? 'response.body instanceof Buffer' : 'response.body is empty';
                        saveError(chunk.key,chunk.link,response.statusCode,message,database,sri2PostgresClient.propertyConfig.targetTable,sri2PostgresClient.resourceType)
                            .then(handleStreamFlow);
                    }
                }else{
                    //statusCode != 200 => Error
                    saveError(chunk.key,chunk.link,response.statusCode,response.statusMessage,database,sri2PostgresClient.propertyConfig.targetTable,sri2PostgresClient.resourceType)
                        .then(handleStreamFlow);
                }

            }).fail(function(getApiContentError){
                saveError(chunk.key,chunk.link,getApiContentError.code,getApiContentError.message,database,sri2PostgresClient.propertyConfig.targetTable,sri2PostgresClient.resourceType)
                    .then(handleStreamFlow);
            });
        });

        stream.on('end',function(){
            if (queue == 0){
                database.end();
                deferred.resolve({resourcesSync: resourcesSync, resourcesNotSync: count-resourcesSync});
            }
        });
    });

    return deferred.promise;
};

Client.prototype.saveResourcesInProperty = function(propertyConfig,callback){

    var deferred = Q.defer();

    var self = this;

    this.logMessage("SRI2POSTGRES: saveResourcesInProperty :: Started");
    this.deleteFromTable(propertyConfig)
        .then(this.readFromTable)
        .then(function(response){
            self.postgresClient.end();
            deferred.resolve({resourcesSync: response.resourcesSync, resourcesNotSync: response.resourcesNotSync});
        }).fail(function(error){
            self.postgresClient.end();
            deferred.reject(error);
        });

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

Client.prototype.saveResourcesInPropertyWithoutTableDeletion = function(propertyConfig,callback){

    var deferred = Q.defer();

    this.logMessage("SRI2POSTGRES: saveResourcesInPropertyWithoutTableDeletion :: Started");

    this.propertyConfig = propertyConfig;
    var self = this;

        this.readFromTable(this)
            .then(function(response){
                self.postgresClient.end();
                deferred.resolve({resourcesSync: response.resourcesSync, resourcesNotSync: response.resourcesNotSync});
            }).fail(function(error){
                self.postgresClient.end();
                deferred.reject(error);
            });

    deferred.promise.nodeify(callback);
    return deferred.promise;
};

// export the class
module.exports = Client;