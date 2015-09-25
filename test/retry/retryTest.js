/**
 * Created by pablo on 21/09/15.
 */
var Client = require('./../../src/lib/client.js');
var expect  = require("chai").expect;
var express = require('express');

var createSri2PostgresInstance = function (config) {

    return new Client(config);
};

describe('Accessing invalid json Api', function() {

    this.timeout(0);

    it('should respond to GET', function (done) {

        var app = express();

        app.get('/', function (req, res) {
            res.json({key:'value'});
        });

        app.listen(3000, function () {

            //apiTimeOut depends on your machine, you have to run a few times to find a proper number

            var config = {
                baseApiUrl : " http://localhost:3000/",
                functionApiUrl : "",
                apiTimeOut: 8
            }

            var sri2postgres = createSri2PostgresInstance(config);

            sri2postgres.getApiContent(function (error,response) {
                expect(response.statusCode).to.equal(200);
                done();
            });
        });
    });
});