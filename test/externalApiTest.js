/**
 * Created by pablo on 22/07/15.
 */
var expect  = require("chai").expect;
var Client = require('./../src/lib/client.js');

var createSri2PostgresInstance = function (config) {

    return new Client(config);
};

describe('Accessing external json date Api', function() {

    describe('passing null URL', function(){

        it('throws an error', function(){
            var config = {};
            expect(createSri2PostgresInstance.bind(config)).to.throw(Error);
        })
    })

    it('should respond to GET', function (done) {

        var config = {
            apiUrl : "http://dump.getpostman.com/status"
        }
        var sri2postgres = createSri2PostgresInstance(config);

        sri2postgres.getApiContent(function (error,response) {
            expect(response.statusCode).to.equal(200);
            done();
        });
    })



    describe('with basic auth',function(){

        it('should respond OK with valid credentials', function (done) {

            var config = {
                apiUrl : "http://dump.getpostman.com/auth/basic",
                credentials: { username: 'postman', password: 'password' }
            }

            var sri2postgres = createSri2PostgresInstance(config);

            sri2postgres.getApiContent(function (error,response) {
                expect(response.statusCode).to.equal(200);
                done();
            });
        })

        it('should return 401 error with invalid username and password',function(done){

            var config = {
                apiUrl : "http://dump.getpostman.com/auth/basic",
                credentials: { username: 'bad.user', password: 'bad.passowrd' }
            }

            var sri2postgres = createSri2PostgresInstance(config);

            sri2postgres.getApiContent(function (error,response) {
                expect(response.statusCode).to.equal(401);
                done();
            });
        })
    });


});