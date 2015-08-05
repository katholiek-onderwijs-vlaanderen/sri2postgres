/**
 * Created by pablo on 03/08/15.
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


describe('sri2postgres save an array of resources',function(){

    config.baseApiUrl = "https://vsko-content-api.herokuapp.com";
    config.functionApiUrl = "/content?limit=500";
    config.dbTable = 'sri2postgres.jsonb';
    var sri2postgres = new Client(config);
    var resourcesCount = 0;
    var startedDateTime = new Date();

    before(function(done) {
        var creationQuery = "CREATE SCHEMA sri2postgres AUTHORIZATION " + config.dbUser + "; SET search_path TO sri2postgres; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,details jsonb);";
        databaseHelper.executeQuery(creationQuery,done);
    });

    beforeEach(function(done){

        this.timeout(0);

        sri2postgres.getApiContent().then(function(result){
            resourcesCount = result.body.$$meta.count;
            done();
        });

    });

    it('persist JSON from api to configured postgres table',function(done){
        this.timeout(0);

        sri2postgres.connect(function () {
            sri2postgres.saveResources().then(function(result){
                expect(resourcesCount).to.equal(result.resourcesSync+result.resourcesNotSync);
                done();
            });
        });
    });

    it ('should saved last sync time',function(done){
        startedDateTime.should.beforeTime(sri2postgres.lastSync)
        done();
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2postgres CASCADE;";
        databaseHelper.executeQuery(dropQuery,done);
    });
});