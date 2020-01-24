/**
 * Created by pablo on 29/07/15.
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

describe('sri2db save content',function(){

    this.timeout(0);

    //VERY important adding done parameter to make async function wait
    before(function(done){
        var creationQuery = "CREATE SCHEMA sri2db AUTHORIZATION "+config.dbUser+"; SET search_path TO sri2db; DROP TABLE IF EXISTS jsonb CASCADE; CREATE TABLE jsonb (key uuid unique,details jsonb);";
        databaseHelper.executeQuery(creationQuery,done);
    });

    config.baseApiUrl = 'http://api.vsko.be';
    config.functionApiUrl ='/schools/a2aaf576-a3a4-11e3-ace8-005056872b95';
    var sri2db = new Client(config);

    it('should throw an error if not table is defined',function(done){
        sri2db.saveResource().should.be.rejected.and.notify(done);
    });

    it('persist JSON from api to configured postgres table',function(done){

        sri2db.connect(function () {
            sri2db.dbTable = 'sri2db.jsonb';
            sri2db.saveResource().should.eventually.have.property("rowCount").equal(1).and.notify(done);
        });
    });

    it('should update the same resource if it is saved again',function(done){
        sri2db.saveResource().should.not.be.rejected.and.notify(done);
    });

    after(function(done) {
        var dropQuery= "DROP SCHEMA IF EXISTS sri2db CASCADE;";
        databaseHelper.executeQuery(dropQuery,done);
    });
});