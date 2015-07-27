/**
 * Created by pablo on 27/07/15.
 */
var expect  = require("chai").expect;
var express = require('express');
var Client = require('./../src/lib/client.js');

var createSri2PostgresInstance = function (config) {

    return new Client(config);
};

describe('Accesing local json Api', function() {

    it('should respond to GET', function (done) {

        var app = express();

        app.get('/', function (req, res) {
            res.json({key:'value'});
        });

        app.listen(3000, function () {

            var config = {
                apiUrl : " http://localhost:3000/"
            }
            var sri2postgres = createSri2PostgresInstance(config);

            sri2postgres.getApiContent(function (error,response) {
                expect(response.statusCode).to.equal(200);
                done();
            });

        });

    })
});