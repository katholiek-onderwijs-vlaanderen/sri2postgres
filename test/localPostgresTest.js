/**
 * Created by pablo on 28/07/15.
 */
var expect  = require("chai").expect;
var Client = require('./../src/lib/client.js');
var fs = require('fs');

var configurationFile = './test/config.json';
var config = JSON.parse(fs.readFileSync(configurationFile));

describe('Connecting to a correct Postgres DataBase',function(){
    it('should respond with no error', function (done) {

        var sri2postgres = new Client(config);

        sri2postgres.connect(function (error) {
            expect(error).to.equal(null);
            done();
        });
    });
});