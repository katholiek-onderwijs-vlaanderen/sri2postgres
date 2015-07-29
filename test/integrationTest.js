/**
 * Created by pablo on 29/07/15.
 */
var expect  = require("chai").expect;
var Client = require('./../src/lib/client.js');
var pg = require('pg');

//Set up Database schema and table
//Perform test with asserts
//delete created database schema and table


var executeQuery = function (query,done) {

    var conString = "postgres://admin:admin@localhost:5433/postgres";
    var localDatabaseClient =  new pg.Client(conString);

    localDatabaseClient.connect(function(err) {
        if(err) {
            throw new Error("Could not connect to local database");
        }else{

            localDatabaseClient.query(query, function(err, result) {
                if(err) {
                    return console.error('error running query', err);
                }
                localDatabaseClient.end();
                done();
            });
        }
    });
};


describe('sri2postgres save content',function(){

    //VERY important adding done parameter to make async function wait
    before(function(done){
        var creationQuery = "CREATE SCHEMA sri2postgres AUTHORIZATION admin; SET search_path TO sri2postgres; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,details jsonb);";
        executeQuery(creationQuery,done);
    })

    it('persist JSON from api to configured postgres table',function(done){

        //console.log("ACT: extract content from api and save it");
        //console.log("ASSERT: query local database to check if content was saved");

        var config = {
            apiUrl : "http://date.jsontest.com/",
            dbUser: "admin",
            dbPassword: "admin",
            database: "postgres",
            dbPort: "5433",
            dbHost: "localhost"
        }

        var sri2postgres = new Client(config);

        sri2postgres.saveContent("parameter table")
            .then(function (result) {
                console.log(result);
            })
            .fail(function (error) {

            });

        done();
    })

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2postgres CASCADE;";
        executeQuery(dropQuery,done);
    });
})