/**
 * Created by pablo on 03/08/15.
 */
var pg = require('pg');

function DatabaseHelper(config){
    this.config = config;
}

DatabaseHelper.prototype.executeQuery = function (query, done) {

    var conString = "postgres://"+this.config.dbUser+":"+this.config.dbPassword+"@"+this.config.dbHost+":"+this.config.dbPort+"/"+this.config.database;
    var localDatabaseClient = new pg.Client(conString);

    localDatabaseClient.connect(function (err) {
        if (err) {
            throw new Error("Could not connect to local database");
        }

        localDatabaseClient.query(query, function (err) {
            if (err) {
                return console.error('error running query', err);
            }
            localDatabaseClient.end();
            done();
        });
    });
};


module.exports = DatabaseHelper;