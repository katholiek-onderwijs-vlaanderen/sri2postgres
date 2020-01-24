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

        var sri2db = new Client(config);

        sri2db.connect(function (error) {
            expect(error).to.equal(null);
            done();
        });
    });
});