/**
 * Created by pablo on 29/07/15.
 */

var Client = require('./../src/lib/client.js');
var pg = require('pg');
var chai = require("chai");
var should = require( 'chai' ).should();
var chaiAsPromised = require( 'chai-as-promised' );

chai.use(chaiAsPromised);

//Set up Database schema and table
//Perform test with asserts
//delete created database schema and table

executeQuery = function (query, done) {

    var conString = "postgres://admin:admin@localhost:5433/postgres";
    var localDatabaseClient = new pg.Client(conString);

    localDatabaseClient.connect(function (err) {
        if (err) {
            throw new Error("Could not connect to local database");
        }

        localDatabaseClient.query(query, function (err, result) {
            if (err) {
                return console.error('error running query', err);
            }
            localDatabaseClient.end();
            done();
        });
    });
};

var config = {
    apiUrl : "http://dump.getpostman.com/status",
    dbUser: "admin",
    dbPassword: "admin",
    database: "postgres",
    dbPort: "5433",
    dbHost: "localhost"
};

describe('sri2postgres save content',function(){

    //VERY important adding done parameter to make async function wait
    before(function(done){
        var creationQuery = "CREATE SCHEMA sri2postgres AUTHORIZATION admin; SET search_path TO sri2postgres; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,details jsonb);";
        executeQuery(creationQuery,done);
    });

    var sri2postgres = new Client(config);

    it('should throw an error if not table is defined',function(done){
        sri2postgres.saveContent().should.be.rejected.and.notify(done);
    });

    it('persist JSON from api to configured postgres table',function(done){
        sri2postgres.saveContent('tableName').should.eventually.have.property("status").equal("ok").and.notify(done);
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2postgres CASCADE;";
        executeQuery(dropQuery,done);
    });
});