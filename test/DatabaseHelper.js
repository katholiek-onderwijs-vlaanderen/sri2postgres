/**
 * Created by pablo on 03/08/15.
 */
var pg = require('pg');
var Q = require('q');

function DatabaseHelper(config){
    this.config = config;
}

DatabaseHelper.prototype.executeQuery = function (query, done) {

    var deferred = Q.defer();

    var conString = "postgres://"+this.config.dbUser+":"+this.config.dbPassword+"@"+this.config.dbHost+":"+this.config.dbPort+"/"+this.config.database;
    var localDatabaseClient = new pg.Client(conString);

    localDatabaseClient.connect(function (err) {
        if (err) {
            deferred.reject(new Error(err));
        }

        localDatabaseClient.query(query, function (err,result) {
            if (err) {
                deferred.reject(new Error(err));
            }
            localDatabaseClient.end();
            deferred.resolve(result);
        });
    });

    deferred.promise.nodeify(done);
    return deferred.promise;
};


module.exports = DatabaseHelper;