/**
 * Created by pablo on 07/08/15.
 */
var Client = require('./../../src/lib/client.js');
var DatabaseHelper = require('./../DatabaseHelper.js');
var chai = require("chai");
var expect  = require("chai").expect;
var should = require( 'chai' ).should();
var chaiAsPromised = require( 'chai-as-promised' );
var fs = require('fs');

chai.use(require('chai-datetime'));
chai.use(chaiAsPromised);

var configurationFile = './test/config.json';
var config = JSON.parse(fs.readFileSync(configurationFile));
var databaseHelper = new DatabaseHelper(config);

describe('sri2Postgres read an url property from jsonb ', function(){

    //First store all api content into a database
    //then pass the property where the url is stored.
    //save one by one its data into a new table.
    //assert new table is full filled.

    config.baseApiUrl = "https://vsko-content-api.herokuapp.com";
    config.functionApiUrl = "/content?limit=500";
    config.dbTable = 'sri2postgres.jsonb';
    var sri2postgres = new Client(config);

    before(function(done) {
        var creationQuery = "CREATE SCHEMA sri2postgres AUTHORIZATION " + config.dbUser + "; SET search_path TO sri2postgres; " +
            "DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,value jsonb);" +
            "DROP TABLE IF EXISTS jsonb_content_as_text CASCADE; CREATE TABLE jsonb_content_as_text (key uuid unique,value text);";
        databaseHelper.executeQuery(creationQuery,done);
    });

    beforeEach(function(done){

        this.timeout(0);

        sri2postgres.connect(function () {
            sri2postgres.saveResources().then(function(){
                done();
            });
        });
    });

    it('should save the data content in passed table',function(done){

        this.timeout(0);

        var propertyConfig = {
            propertyName : "value->'attachments'->1->>'externalUrl'",
            targetTable: "sri2postgres.jsonb_content_as_text",
            queriesPerTransaction: 500
        };

        sri2postgres.saveResourcesInProperty(propertyConfig)
            .should.eventually.have.property("resourcesSync").to.be.at.least(1).and.notify(done);
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2postgres CASCADE;";
        databaseHelper.executeQuery(dropQuery,done);
    });

});