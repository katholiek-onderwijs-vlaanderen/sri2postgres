/**
 * Created by pablo on 23/07/15.
 */

const request = require('requestretry');
const moment = require('moment');
const io = require('socket.io-client');


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

    this.logging = config.hasOwnProperty('logging') ? config.logging : false;
    this.dbTable = config.dbTable;
    this.resourceType = config.hasOwnProperty('resourceType') ? config.resourceType : 'document';
    this.requiredByRoot = config.hasOwnProperty('requiredByRoot') ? config.requiredByRoot : undefined;

    this.encodeURL = config.hasOwnProperty('encodeURL') ? config.encodeURL : true;

    this.apiTimeOut = config.hasOwnProperty('apiTimeOut') ? config.apiTimeOut : 0;

    this.connectionString = config.hasOwnProperty('connectionString') ? config.connectionString : '';

    this.postgresClient = config.db;
    this.apiHeaders = config.headers;
}


Client.prototype.init = async function(){
    const sql = `SELECT EXISTS (
                   SELECT 1
                   FROM   information_schema.tables 
                   WHERE  table_schema = '${process.env.POSTGRES_SCHEMA}'
                   AND    table_name = 'sri2postgres_last_sync'
                   );`
    const [ {exists} ] = (await this.postgresClient.result(sql)).rows;
    if (!exists) {
        console.log('Table "sri2postgres_last_sync" does not exist yet => create');
        try {
            const sql = `CREATE TABLE sri2postgres_last_sync (
                           "resource" text PRIMARY KEY,
                           "timestamp" text not null
                         );`;
            const query_reply = await this.postgresClient.result(sql)            
        } catch (err) {
            console.log(err);
            throw 'sri2postgres_last_sync.creation.failed';
        }            
    }
}


var totalSync = 0;
var totalNotSync = 0;

// String.prototype.replaceAll = function(search, replace) {
//     if (replace === undefined) {
//         return this.toString();
//     }
//     return this.split(search).join(replace);
// };

// var insertResources = async function(composeObject, client) {

//     var jsonData = composeObject.jsonData;
//     var inserted = 0;

//     try {
//         await client.postgresClient.tx(tx => {
//             const promiseList = jsonData.body.results.reduce( (promiseList, resource) => {
//                 //first check if there is a filter
//                 if (composeObject.filter){

//                     if ( composeObject.filter.isValid(resource) ){

//                         var key = composeObject.filter.getKeyFrom(resource);
//                         var value = composeObject.filter.getValueFrom(resource);
//                         value = value.replaceAll("'", "''");
//                         insertQuery  = "INSERT INTO "+client.dbTable+" VALUES ('"+key+"','"+value+"','"+client.resourceType+"')";
//                         promiseList.push(tx.query(insertQuery));
//                         inserted++;

//                         if (typeof client.requiredByRoot != 'undefined' ){
//                             var insertRootQuery  = "INSERT INTO "+client.requiredByRoot.table+" VALUES ('"+key+"','"+client.requiredByRoot.key+"','"+client.resourceType+"')";
//                             promiseList.push(tx.query(insertRootQuery));
//                         }
//                     }
//                 }else{
//                     //process all of them
//                     var key = resource.$$expanded.key;
//                     console.log(`KEY: ${key}`)
//                     var stringifiedJson = JSON.stringify(resource.$$expanded);
//                     stringifiedJson = stringifiedJson.replaceAll("'", "''");
//                     insertQuery  = "INSERT INTO "+client.dbTable+" VALUES ('"+key+"','"+stringifiedJson+"','"+client.resourceType+"')";
//                     promiseList.push(tx.query(insertQuery));
//                     inserted++;

//                     if (typeof client.requiredByRoot != 'undefined' ){
//                         var insertRootQuery  = "INSERT INTO "+client.requiredByRoot.table+" VALUES ('"+key+"','"+client.requiredByRoot.key+"','"+client.resourceType+"')";
//                         promiseList.push(tx.query(insertRootQuery));
//                     }
//                 }
//                 return promiseList
//             }, []);

//             return tx.batch(promiseList); 
//         });
//         totalSync += Number(inserted);
//     } catch(err) {
//         client.logMessage(`SRI2POSTGRES: DB Error during INSERT: ${err}`);
//         client.logMessage(err)
//         totalNotSync += Number(composeObject.jsonData.body.results.length);
//     }
//     return jsonData.body.$$meta.next;
// };

// var updateData = function(jsonData){
//     var deferred = Q.defer();
//     var key = jsonData.body.key;
//     var updateQuery  = "UPDATE "+this.Client.dbTable+" SET details = '"+JSON.stringify(jsonData.body)+"' WHERE key = '"+key+"'";

//     this.Client.postgresClient.query(updateQuery, function (error, result) {
//         if (error) {
//             deferred.reject(new Error(error));
//         } else {
//             deferred.resolve(result);
//         }
//     });

//     return deferred.promise;
// };

// //private method
// var insertData = function(jsonData) {

//     var deferred = Q.defer();
//     var key = jsonData.body.key;
//     var insertQuery  = "INSERT INTO "+this.Client.dbTable+" VALUES ('"+key+"','"+JSON.stringify(jsonData.body)+"')";

//     this.Client.postgresClient.query(insertQuery, function (error, result) {

//         //error.code == 23505 UNIQUE VIOLATION
//         if (error && error.code == 23505) {

//             updateData(jsonData).then(function(response){
//                 deferred.resolve(response);
//             }).fail(function(error){
//                 deferred.reject(new Error(error));
//             });

//         } else {
//             deferred.resolve(result);
//         }
//     });

//     return deferred.promise;
// };


// //Creating-NodeJS-modules-with-both-promise-and-callback-API-support-using-Q
// Client.prototype.saveResource = function(table,callback) {

//     var deferred = Q.defer();

//     if ( !this.dbTable && !table){
//         deferred.reject("table must be passed.");
//     }else{

//         if (table) {
//             this.dbTable = table;
//         }

//         this.getApiContent().then(insertData).then(function(response){
//             this.Client.updateDateSync();
//             deferred.resolve(response);
//         }).fail(function(error){
//             deferred.reject(error);
//         });
//     }

//     deferred.promise.nodeify(callback);
//     return deferred.promise;
// };

Client.prototype.getURL = function(){

    var url = this.baseApiUrl+this.functionApiUrl;

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


Client.prototype.getApiContent = function() {
    return request.get({url: this.getURL(), auth: this.apiCredentials, headers: this.apiHeaders, json: true})
};




const removeDollarFields = (obj) => {
    Object.keys(obj).forEach( property => {
        if (property.startsWith("$$") && property!='$$meta') {
            delete obj[property]
        } else  {
            if (obj.property !== null && typeof obj.property === 'object') {
                removeDollarFields(obj[property])
            }
        }        
    })
    return obj
}


const handlePage = async function(client, count) {
    "use strict";

    var jsonData;
    try {
        jsonData = await client.getApiContent();
    } catch (err) {
        client.logMessage("SRI2POSTGRES: Error when getting: " + client.baseApiUrl+client.functionApiUrl + " | Error: " );
        console.error(err)
        console.error(err.stack)
        throw 'fetch.not.ok'
    }
    

    if (jsonData.statusCode != 200) {
        client.logMessage("SRI2POSTGRES: Error "+jsonData.statusCode+" when getting: " + client.baseApiUrl+client.functionApiUrl + " | Error Message: " + jsonData.statusMessage);
        console.log(jsonData.body)
        throw 'fetch.not.ok'
    } else if (jsonData.body.results.length > 0) {
        
        var sql = `INSERT INTO ${client.dbTable} VALUES\n`
            sql += jsonData.body.results.map( e => {
                            const key = e.$$expanded.key;
                            const stringifiedJson = JSON.stringify(removeDollarFields(e.$$expanded)).replace(/\'/g, `''`);
                            return `('${key}', '${stringifiedJson}','${client.resourceType}')`
                        }).join(',\n')
            sql += `ON CONFLICT (key) DO UPDATE 
                     SET (key, value, type) = (EXCLUDED.key, EXCLUDED.value, EXCLUDED.type)`;

        try {
            const query_reply = await client.postgresClient.result(sql)
            if (query_reply.rowCount != jsonData.body.results.length) {
                console.log(`\n\nWARNING: INSERT count mismatch !`)    
                console.log(`for query: ${sql}`)
                console.log(`${query_reply.rowCount} <-> ${res.body.results.length}\n\n`)
            }
        } catch (err) {
            console.log(`\n\nSQL INSERT failed: ${err}`)
            console.log(`for query: ${sql}\n\n`)

            console.log(client.dbTable)

            process.exit(1)
        }        
    } 
    count += jsonData.body.results.length

    var nextPage = jsonData.body.$$meta.next;
    if (nextPage === undefined) {
        console.log(`NO NEXT PAGE => RETURNING (${client.dbTable})`)
        return { next: false, count: count}
    } else {
        console.log(`NEXT PAGE: ${nextPage} (${count}) - (${client.dbTable})`)
        client.functionApiUrl = decodeURIComponent(nextPage);
        return { next: true, count: count}
    }
}


Client.prototype.saveResources = async function(){

    const client = this
    let count = 0

    try {
        const sql = `INSERT INTO sri2postgres_last_sync (resource, timestamp) 
                     VALUES ('${client.dbTable}', '${moment().toISOString()}')
                     ON CONFLICT (resource) DO UPDATE 
                     SET (resource, timestamp) = (EXCLUDED.resource, EXCLUDED.timestamp)`
        const query_reply = await client.postgresClient.result(sql)
    } catch (err) {
        console.log('ERR: Updating last_sync_time failed:')
        console.log(err)
        throw 'updating.last.sync.time.failed'
    }

    await Client.prototype.deleteResources(client)

    let next = true
    while (next) {
        ({ next, count } = await handlePage(client, count))
    }

    return count
};


Client.prototype.getLastSync = async function() {
    const client = this;
    let lastSync = null;
    try {
        const sql = `SELECT timestamp FROM sri2postgres_last_sync WHERE resource='${client.dbTable}'`
        rows = (await client.postgresClient.result(sql)).rows;
        if (rows.length > 0) {
            const { timestamp: lastSync } = rows[0];
            return lastSync;
        }
    } catch (err) {
        console.log('ERR: Retrieving last_sync_time failed:')
        console.log(err)
        throw 'retrieving.last.sync.time.failed'
    }
}



Client.prototype.deltaSync = async function(lastSync = null) {

    const client = this;
    let count = 0;
    let functionApiUrlCpy = null;
    if (lastSync == null) {
        lastSync = await client.getLastSync();
    }

    if (lastSync != null) {
        functionApiUrlCpy = client.functionApiUrl
        client.functionApiUrl += (client.functionApiUrl.includes('?') ? '&' : '?')
                                    + `modifiedSince=${ moment(lastSync).subtract(10, 'seconds').toISOString()}`
        console.log(client.functionApiUrl)
    } else {
        // do full sync -> delete all resources
        await Client.prototype.deleteResources(client)
    }

    try {
        sql = `INSERT INTO sri2postgres_last_sync (resource, timestamp) 
                     VALUES ('${client.dbTable}', '${moment().toISOString()}')
                     ON CONFLICT (resource) DO UPDATE 
                     SET (resource, timestamp) = (EXCLUDED.resource, EXCLUDED.timestamp)`
        const query_reply = await client.postgresClient.result(sql)
    } catch (err) {
        console.log('ERR: Updating last_sync_time failed:')
        console.log(err)
        throw 'updating.last.sync.time.failed'
    }

    let next = true
    while (next) {
        ({ next, count } = await handlePage(client, count))
    }

    if (functionApiUrlCpy!=null) {
        client.functionApiUrl = functionApiUrlCpy
    }

    return count
};



Client.prototype.deleteResources = async function(clientInstance){
    const deletionQuery = `DELETE FROM ${clientInstance.dbTable}`;
    clientInstance.logMessage("SRI2POSTGRES: deleteResources :: Started");
    await clientInstance.postgresClient.query(deletionQuery)
    clientInstance.logMessage("SRI2POSTGRES: deleteResources :: SUCCESS");
};





Client.prototype.installBroadcastListeners = async function(db) {
    const client = this;
    const broadcastUrl = "https://vsko-audit-broadcast-api-test.herokuapp.com/" // TODO: get this from config?


    var socket = io.connect( broadcastUrl );

    socket.on( 'connect', function ( ) {
        console.log( "CONNECTED" );
        socket.emit('join', "/persons");

    } );

    socket.on( 'disconnect', function ( ) {
        console.log( "DISCONNECTED" );
    } );

    socket.on('update',  async function ( data ) {
        console.log("New data arrived: " + data);
        const lastSync = await client.getLastSync();

        if (moment(data.timestamp) > moment(lastSync)) {
            console.log('Initiating delta sync.')
            await client.deltaSync(lastSync)
        } else {
            console.log('No sync needed.')
            console.log(data.timestamp)
            console.log(lastSync)
        }
    } );

}




// Client.prototype.deleteFromTable = function(propertyConfig){

//     var deferred = Q.defer();

//     var clientInstance = this;

//     var deletionQuery = "DELETE FROM "+propertyConfig.targetTable+ " WHERE type = '"+clientInstance.resourceType+"'";

//     this.logMessage("SRI2POSTGRES: deleteFromTable :: Started");

//     this.postgresClient.query(deletionQuery, function (err) {

//         clientInstance.logMessage("SRI2POSTGRES: deleteFromTable :: end");

//         if (err) {
//             clientInstance.logMessage("SRI2POSTGRES: deleteFromTable :: ERROR " + err);
//             deferred.reject(new Error(err));
//         }else{

//             clientInstance.logMessage("SRI2POSTGRES: deleteFromTable :: SUCCESS");

//             clientInstance.propertyConfig = propertyConfig;
//             deferred.resolve(clientInstance);
//         }
//     });

//     return deferred.promise;
// };

// var saveError = function (key,link,code,message,database,table,type){
//     var deferred = Q.defer();

//     var insertEmptyTextQuery = "INSERT INTO "+table+" VALUES ('"+key+"',E'','"+type+"'); ";
//     var errorInsertQuery  = "INSERT INTO content_as_text_errors VALUES ('"+key+"','"+link+"','"+code+"','"+message+"')";
//     database.query(insertEmptyTextQuery+errorInsertQuery,function(queryError){
//         if (queryError){
//             console.error(message + " " +code);
//             console.error(key);
//             console.error(link);
//             console.error("--*--");
//         }
//         deferred.resolve();
//     });

//     return deferred.promise;
// };

// Client.prototype.readFromTable = function(sri2PostgresClient){

//     var deferred = Q.defer();
//     var offset = sri2PostgresClient.propertyConfig.hasOwnProperty('offset') ? sri2PostgresClient.propertyConfig.offset : 0;
//     var limit = sri2PostgresClient.propertyConfig.hasOwnProperty('limit') ? sri2PostgresClient.propertyConfig.limit : 1000000;

//     // SELECT key, obj->>'href' as link FROM jsonb, jsonb_array_elements(value->'attachments') obj WHERE type = 'curriculumzill' AND obj->>'type' = 'CONTENT_AS_TEXT' ORDER BY key LIMIT 5000 OFFSET 0

//     var sqlQuery = "SELECT key, "+sri2PostgresClient.propertyConfig.propertyName+" AS link";
//     sqlQuery += " FROM "+sri2PostgresClient.dbTable+" "+sri2PostgresClient.propertyConfig.fromExtraConditions;
//     sqlQuery += " WHERE type = '"+sri2PostgresClient.resourceType+"' "+sri2PostgresClient.propertyConfig.whereExtraConditions;
//     sqlQuery += " ORDER BY key LIMIT $1 OFFSET "+offset;
//     var query = new QueryStream(sqlQuery, [limit]);
//     var stream = sri2PostgresClient.postgresClient.query(query);
//     var count = 0;
//     var resourcesSync = 0;
//     var queue = 0;

//     function handleStreamFlow(){
//         if (stream.readable){
//             queue--;
//             stream.resume();
//         }else{
//             deferred.resolve({resourcesSync: resourcesSync, resourcesNotSync: count-resourcesSync});
//         }
//     }

//     stream.on('data',function(chunk){

//         stream.pause();
//         count++;
//         queue++;

//         var originalLink = chunk.link;
//         var res = originalLink.split("/");
//         var sourceName = res[res.length-1];
//         sourceName = encodeURIComponent(sourceName);
//         var componentUrl = "/" + res[1] + "/" +res[2] + "/" + sourceName;

//         sri2PostgresClient.functionApiUrl = componentUrl;

//         sri2PostgresClient.getApiContent().then(function(response){

//             //console.log("SRI2POSTGRES: readFromTable :: Obtained content_as_text for: " + chunk.link);

//             if (response.statusCode == 200 ){

//                 var isBuffer = (response.body instanceof Buffer);

//                 if (response.body.length > 0 && !isBuffer){

//                     //console.log("SRI2POSTGRES: readFromTable ["+count+"] :: preparing INSERT for " +chunk.key);

//                     var data = response.body.replaceAll("'", "''");
//                     // After replacing ' -> '' there are still cases where \'' brake the query, so
//                     // we need to transform \'' -> '' to correctly insert it.
//                     data = data.replaceAll("\\''", "''");

//                     var insertQuery  = "INSERT INTO "+sri2PostgresClient.propertyConfig.targetTable+" VALUES ('"+chunk.key+"',E'"+data+"','"+sri2PostgresClient.resourceType+"')";

//                     database.query(insertQuery,function(queryError){

//                         if (queryError){
//                             saveError(chunk.key,chunk.link,0,queryError.message,database,sri2PostgresClient.propertyConfig.targetTable,sri2PostgresClient.resourceType);
//                         }else{
//                             resourcesSync++;
//                             sri2PostgresClient.logMessage("SRI2POSTGRES: readFromTable :: [ "+resourcesSync+"/"+count+" ]  INSERT SUCCESSFULLY for " +chunk.key);
//                         }

//                         handleStreamFlow();

//                     });
//                 }else{

//                     var message = isBuffer ? 'response.body instanceof Buffer' : 'response.body is empty';
//                     saveError(chunk.key,chunk.link,response.statusCode,message,database,sri2PostgresClient.propertyConfig.targetTable,sri2PostgresClient.resourceType)
//                         .then(handleStreamFlow);
//                 }
//             }else{
//                 //statusCode != 200 => Error
//                 saveError(chunk.key,chunk.link,response.statusCode,response.statusMessage,database,sri2PostgresClient.propertyConfig.targetTable,sri2PostgresClient.resourceType)
//                     .then(handleStreamFlow);
//             }

//         }).fail(function(getApiContentError){
//             saveError(chunk.key,chunk.link,getApiContentError.code,getApiContentError.message,database,sri2PostgresClient.propertyConfig.targetTable,sri2PostgresClient.resourceType)
//                 .then(handleStreamFlow);
//         });
//     });

//     stream.on('end',function(){
//         if (queue == 0){
//             deferred.resolve({resourcesSync: resourcesSync, resourcesNotSync: count-resourcesSync});
//         }
//     });

//     return deferred.promise;
// };

// Client.prototype.saveResourcesInProperty = function(propertyConfig,callback){

//     var deferred = Q.defer();

//     var self = this;

//     this.logMessage("SRI2POSTGRES: saveResourcesInProperty :: Started");
//     this.deleteFromTable(propertyConfig)
//         .then(this.readFromTable)
//         .then(function(response){
//             deferred.resolve({resourcesSync: response.resourcesSync, resourcesNotSync: response.resourcesNotSync});
//         }).fail(function(error){
//             deferred.reject(error);
//         });

//     deferred.promise.nodeify(callback);
//     return deferred.promise;
// };

// Client.prototype.saveResourcesInPropertyWithoutTableDeletion = function(propertyConfig,callback){

//     var deferred = Q.defer();

//     this.logMessage("SRI2POSTGRES: saveResourcesInPropertyWithoutTableDeletion :: Started");

//     this.propertyConfig = propertyConfig;
//     var self = this;

//         this.readFromTable(this)
//             .then(function(response){
//                 deferred.resolve({resourcesSync: response.resourcesSync, resourcesNotSync: response.resourcesNotSync});
//             }).fail(function(error){
//                 deferred.reject(error);
//             });

//     deferred.promise.nodeify(callback);
//     return deferred.promise;
// };

// export the class
module.exports = Client;