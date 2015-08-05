# sri2postgres

This is a module to sync any SRI interface to one database table in a postgres 9.4 instance.
The SRI specification can [be found here][sri-specs].


# Installing

Installation is simple using npm :

    $ cd [your_project]
    $ npm install --save sri2postgres


#About

The main idea of this module is to read all resources from an Api and store them into a database in json format.
A resource is basically a json object with attributes, like an school or a file or maybe a car.


# Usage

Start by requiring the module in your code. 
    
    var Sri2postgres = require('sri2postgres');
    
Then you will need to tell which api and which database trough a config file:

    var config    = {
        "baseApiUrl" : "http://dump.getpostman.com",
        "functionApiUrl" : "/status",
        "dbUser": "admin",
        "dbPassword": "admin",
        "database": "postgres",
        "dbPort": "5432",
        "dbHost": "localhost",
        "dbSsl": false
    }

    var client = new Sri2postgres(config);

Now we are ready to start sync resources.


## Function Definitions

Below is a description of the different types of functions that you can use.
It describes the inputs and outputs of the different functions.
Most of these function return a [Q promise][kriskowal-q].


### connect

This is a simple wrapper for pg.connect:

    sri2postgres.connect(function (error) {
        //once it is connected you can call others sri2postgres methods
    });


### saveResource

This is a q promise method and it allows you to save just ONE resource to the designed table. It perform an SQL UPSERT 

    sri2postgres.saveResource()
    
So if the resource already exit in the table it will be updated. Yo can try yourself by doing:

    sri2postgres.saveResource().then(sri2postgres.saveResource);

### saveResources

This is a q promise method that saves ALL resources from given api.
First this method DELETE all content from the designed table. Then does an SQL TRANSACTION with n INSERT statements for each page of the API.
Lets show you an example:

Suppose we want to save all customers. Then our config json will look like:

    config.baseApiUrl = " http://api.mine.org/";
    config.functionApiUrl = "/customers?limit=500";
    config.dbTable = "shema.table";
    ...

It is very important to understand that "limit" parameter determines the "n" number for INSERT statements of each TRANSACTION.
If just one INSERT query cannot be done, the whole TRANSACTION (in this 500 resources) will not be saved.
Once a TRANSACTION is finish the method will ask for the following page to the api ( in this case by asking http://api.mine.org/customers?limit=500&offset=500) until there are no more resource to save.
Finally it tells you:

- How many resources were saved
- How many resources were NOT saved

    sri2postgres.connect(function () {
        sri2postgres.saveResources().then(function(result){
            result.resourcesSync
            result.resourcesNotSync
        });
    });

In addition you can ask the client when last sync was:

    sri2postgres.lastSync


# Tests
In order to run tests locally it is needed to install postgres 9.4 or superior in your machine.
see http://stackoverflow.com/a/29415300

Then you will need a user and pass for accessing your local postgres database
see also http://www.cyberciti.biz/faq/howto-add-postgresql-user-account/

We use mocha for testing. You will find a config.json file in test folder:

    {
      "baseApiUrl" : "http://dump.getpostman.com",
      "functionApiUrl" : "/status",
      "dbUser": "admin",
      "dbPassword": "admin",
      "database": "postgres",
      "dbPort": "5432",
      "dbHost": "localhost",
      "dbSsl": false
    }

All postgres related test takes its configuration from that file. So make sure it is corrected full filled.

After that you are available to run test doing:

    $ cd [your_project]
    $ mocha --recursive
    
and you will see:

    Accessing external json Api
        ✓ should respond to GET (403ms)
        passing null URL
            ✓ throws an error
        with basic auth
            ✓ should respond OK with valid credentials (355ms)
            ✓ should return 401 error with invalid username and password (364ms)
    
    sri2postgres save content
        ✓ should throw an error if not table is defined
        ✓ persist JSON from api to configured postgres table (807ms)
        ✓ should update the same resource if it is saved again (782ms)
    
    Accessing local json Api
        ✓ should respond to GET
    
    Connecting to a correct Postgres DataBase
        ✓ should respond with no error
    
    sri2postgres save an array of resources
        ✓ persist JSON from api to configured postgres table (34780ms)
        ✓ should saved last sync time
