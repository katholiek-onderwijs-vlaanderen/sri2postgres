/**
 * Created by pablo on 03/08/15.
 */
var Client = require('./../src/lib/client.js');
var DatabaseHelper = require('./DatabaseHelper.js');
var chai = require("chai");
var should = require( 'chai' ).should();
var chaiAsPromised = require( 'chai-as-promised' );
var fs = require('fs');

chai.use(chaiAsPromised);

var configurationFile = './test/config.json';
var config = JSON.parse(fs.readFileSync(configurationFile));
var databaseHelper = new DatabaseHelper(config);


describe('sri2postgres save an array of resources',function(){

    before(function(done){
        var creationQuery = "CREATE SCHEMA sri2postgres AUTHORIZATION "+config.dbUser+"; SET search_path TO sri2postgres; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,details jsonb);";
        databaseHelper.executeQuery(creationQuery,done);
    });

    config.apiUrl = 'http://api.vsko.be/schools?expand=FULL';
    config.dbTable = 'sri2postgres.jsonb';
    var sri2postgres = new Client(config);

    it('persist JSON from api to configured postgres table',function(done){

        sri2postgres.connect(function () {
            sri2postgres.saveResources().should.eventually.have.property("rowCount").equal(1).and.notify(done);
        });
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2postgres CASCADE;";
        databaseHelper.executeQuery(dropQuery,done);
    });
});