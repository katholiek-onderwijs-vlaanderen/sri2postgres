/**
 * Created by pablo on 22/07/15.
 */
var assert = require("assert");
var expect  = require("chai").expect;
var Client = require('./../src/lib/client.js');

var createSri2PostgresInstance = function (config) {

    return new Client(config);
};

describe('Accesing date json Api', function() {

    describe('passing null URL', function(){

        it('throws an error', function(){
            var config = {};
            expect(createSri2PostgresInstance.bind(config)).to.throw(Error);
        })
    })

    it('should respond to GET', function (done) {

        var config = {
            apiUrl : "http://date.jsontest.com/"
        }
        var sri2postgres = createSri2PostgresInstance(config);

        sri2postgres.getApiContent(function (error,response) {
            expect(response.statusCode).to.equal(200);
            done();
        });
    })
});