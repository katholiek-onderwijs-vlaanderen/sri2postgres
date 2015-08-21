/**
 * Created by pablo on 12/08/15.
 */
var Client = require('./../../src/lib/client.js');
var CustomFilter = require('./../customFilter.js');
var DatabaseHelper = require('./../DatabaseHelper.js');
var chai = require("chai");
var expect  = require("chai").expect;
var should = require( 'chai' ).should();
var chaiAsPromised = require( 'chai-as-promised' );
var fs = require('fs');

chai.use(chaiAsPromised);

var configurationFile = './test/config.json';
var config = JSON.parse(fs.readFileSync(configurationFile));
var databaseHelper = new DatabaseHelper(config);


describe('sri2postgres.saveResources with filter',function(){

    config.baseApiUrl = "https://vsko-content-api-test.herokuapp.com";
    config.functionApiUrl = "/content?limit=100";
    config.dbTable = 'sri2postgres.jsonb';
    var sri2postgres = new Client(config);
    var syncResourcesWithoutFilter = 0;

    before(function(done) {
        var creationQuery = "CREATE SCHEMA sri2postgres AUTHORIZATION " + config.dbUser + "; SET search_path TO sri2postgres; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,value jsonb);";
        databaseHelper.executeQuery(creationQuery,done);
    });

    beforeEach(function(done){

        this.timeout(0);

        sri2postgres.connect(function () {
            sri2postgres.saveResources().then(function(result){
                syncResourcesWithoutFilter = result.resourcesSync;
                done();
            });
        });

    });

    it('should persist less resources than sri2postgres.saveResources without filter',function(done){

        this.timeout(0);

        var filterObject = new CustomFilter();

        //we have to tell sri2postgres to start over
        sri2postgres.functionApiUrl = "/content?limit=100";

        sri2postgres.deleteFromTable({targetTable: config.dbTable}).then(function(){
            sri2postgres.saveResources(filterObject).then(function(result){
                expect(syncResourcesWithoutFilter).to.be.above(result.resourcesSync);
                done();
            });
        });
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2postgres CASCADE;";
        databaseHelper.executeQuery(dropQuery,done);
    });
});