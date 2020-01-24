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


describe('sri2db.saveResources with filter',function(){

    config.baseApiUrl = "https://vsko-content-api-test.herokuapp.com";
    config.functionApiUrl = "/content?limit=100";
    config.dbTable = 'sri2db.jsonb';
    var sri2db = new Client(config);
    var syncResourcesWithoutFilter = 0;

    before(function(done) {
        var creationQuery = "CREATE SCHEMA sri2db AUTHORIZATION " + config.dbUser + "; SET search_path TO sri2db; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,value jsonb);";
        databaseHelper.executeQuery(creationQuery,done);
    });

    beforeEach(function(done){

        this.timeout(0);

        sri2db.connect(function () {
            sri2db.saveResources().then(function(result){
                syncResourcesWithoutFilter = result.resourcesSync;
                done();
            });
        });

    });

    it('should persist less resources than sri2db.saveResources without filter',function(done){

        this.timeout(0);

        var filterObject = new CustomFilter();

        //we have to tell sri2db to start over
        sri2db.functionApiUrl = "/content?limit=100";

        sri2db.deleteFromTable({targetTable: config.dbTable}).then(function(){
            sri2db.saveResources(filterObject).then(function(result){
                expect(syncResourcesWithoutFilter).to.be.above(result.resourcesSync);
                done();
            });
        });
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2db CASCADE;";
        databaseHelper.executeQuery(dropQuery,done);
    });
});