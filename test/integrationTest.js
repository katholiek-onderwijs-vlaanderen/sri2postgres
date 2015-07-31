/**
 * Created by pablo on 29/07/15.
 */

var Client = require('./../src/lib/client.js');
var pg = require('pg');
var chai = require("chai");
var should = require( 'chai' ).should();
var chaiAsPromised = require( 'chai-as-promised' );
var fs = require('fs');

chai.use(chaiAsPromised);

var configurationFile = './test/config.json';
var config = JSON.parse(fs.readFileSync(configurationFile));

//Set up Database schema and table
//Perform test with asserts
//delete created database schema and table

executeQuery = function (query, done) {

    var conString = "postgres://"+config.dbUser+":"+config.dbPassword+"@"+config.dbHost+":"+config.dbPort+"/"+config.database;
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

describe('sri2postgres save content',function(){

    //VERY important adding done parameter to make async function wait
    before(function(done){
        var creationQuery = "CREATE SCHEMA sri2postgres AUTHORIZATION "+config.dbUser+"; SET search_path TO sri2postgres; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,details jsonb);";
        executeQuery(creationQuery,done);
    });

    var sri2postgres = new Client(config);

    it('should throw an error if not table is defined',function(done){
        sri2postgres.saveContent().should.be.rejected.and.notify(done);
    });

    it('persist JSON from api to configured postgres table',function(done){
        sri2postgres.connect(function (error) {
            sri2postgres.saveContent('sri2postgres.jsonb').should.eventually.have.property("rowCount").equal(1).and.notify(done);
        });
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2postgres CASCADE;";
        executeQuery(dropQuery,done);
    });
});