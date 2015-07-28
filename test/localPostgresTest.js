/**
 * Created by pablo on 28/07/15.
 */
var expect  = require("chai").expect;
var pg = require('pg');
var Client = require('./../src/lib/client.js');

describe('Connecting to a correct Postgres DataBase',function(){
    it('should respond with no error', function (done) {

        var config = {
            apiUrl : "http://date.jsontest.com/",
            dbUser: "admin",
            dbPassword: "admin",
            database: "postgres",
            dbPort: "5433",
            dbHost: "localhost"
        }

        var sri2postgres = new Client(config);

        sri2postgres.connect(function (error) {
            expect(error).to.equal(null);
            done();
        });
    });
});