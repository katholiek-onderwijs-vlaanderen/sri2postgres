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


describe('sri2db save an array of resources',function(){

    var offset = 106300;

    config.baseApiUrl = "https://vsko-content-api-test.herokuapp.com";
    config.functionApiUrl = "/content?limit=100&offset="+offset;
    config.dbTable = 'sri2db.jsonb';
    var sri2db = new Client(config);
    var resourcesCount = 0;
    var startedDateTime = new Date();

    before(function(done) {
        var creationQuery = "CREATE SCHEMA sri2db AUTHORIZATION " + config.dbUser + "; SET search_path TO sri2db; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,value jsonb);";
        databaseHelper.executeQuery(creationQuery,done);
    });

    beforeEach(function(done){

        this.timeout(0);

        sri2db.getApiContent().then(function(result){
            resourcesCount = Number(result.body.$$meta.count) - Number(offset);
            done();
        });

    });

    it('persist JSON from api to configured postgres table',function(done){
        this.timeout(0);

        sri2db.connect(function () {

            sri2db.deleteFromTable({targetTable: config.dbTable}).then(function(){

                sri2db.saveResources().then(function(result){
                    expect(resourcesCount).to.equal(result.resourcesSync+result.resourcesNotSync);
                    done();
                });
            });
        });
    });

    it ('should saved last sync time',function(done){
        startedDateTime.should.beforeTime(sri2db.lastSync)
        done();
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2db CASCADE;";
        databaseHelper.executeQuery(dropQuery,done);
    });
});