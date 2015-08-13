# sri2postgres

This is a module to sync any SRI interface to one database table in a postgres 9.4 instance.
The SRI specification can [be found here](https://github.com/dimitrydhondt/sri).


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
Most of these function return a [Q promise](https://github.com/kriskowal/q).


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

##### About the table

sri2postgres assumes that the schema.table you set in config follows this structure:

    CREATE TABLE table (key uuid unique,value jsonb);

It is also very important to understand that "limit" parameter determines the "n" number for INSERT statements of each TRANSACTION.
If just one INSERT query cannot be done, the whole TRANSACTION (in this 500 resources) will not be saved.
Once a TRANSACTION is finish the method will ask for the following page to the api ( in this case by asking http://api.mine.org/customers?limit=500&offset=500) until there are no more resource to save.
Finally it tells you:

- How many resources were saved.
- How many resources were NOT saved.

Code example:

    sri2postgres.connect(function () {
        sri2postgres.saveResources().then(function(result){
            result.resourcesSync
            result.resourcesNotSync
        });
    });

In addition you can ask the client when last sync was:

    sri2postgres.lastSync

### saveResources(customFilter)

sometimes you would like not to save all resources from an api, but someones that apply to one or more conditions.
In this cases saveResources receives a customFilter object that works like an java interface. It MUST have a method isValid(resource) that return true or false.
Let's show you an example:

If your resources are cars like:

    [
        {
            brand : 'Ford',
            model: 'Focus',
            color: 'RED'
        },
        {
            brand : 'Chevrolet',
            model: 'Camaro',
            color: 'YELLOW'
        }
    ]

And you want to save just RED cars then you have to write your customFilter like:

    function CustomFilter (){
    
        this.isValid = function (resource){
    
            return resource.color == 'RED';
        };
    };
    
    module.exports = CustomFilter;

As you KNOW the resource you will be able to write your own filter as complex as you need.
Usage can be:

        var filterObject = new CustomFilter();

        sri2postgres.saveResources(filterObject).then(function(result){
            result.resourcesSync
        });
        
###saveResourcesInProperty

After calling saveResources() this method will allow you to get the content from a specific property (nested objects).
We assume this property has as value a link to an api to get a resource.

In the following example we want to obtain the content that engine property points to.

    [
        {
            key: uuid-1,
            brand : 'Ford',
            model: 'Focus',
            color: 'RED',
            engine: 'https://myengine.api.com/engine-uuid-2
        },
        {
            key: uuid-2
            brand : 'Chevrolet',
            model: 'Camaro',
            color: 'YELLOW',
            engine: 'https://myengine.api.com/engine-uuid-3
        }
    ]

Before calling saveResourcesInProperty  we need to tell sri2postgres which attribute and which new table is going to use for this purpose.
        
        var propertyConfig = {
            propertyName : "value->'engine'",
            targetTable: "schema.engine_table",
            queriesPerTransaction: 20
        };

Again a table like this will be required:

    CREATE TABLE engine_table (key uuid unique,value jsonb);

sri2postgres will insert into the targetTable in a Transaction way. So you can set how many insert perform in each transaction.
Again if just ONE INSERT fails the whole transaction will fail. Fortunately, sri2postgres will abort the current transaction and continue with a new one up to finish.

So this code:

    var propertyConfig = {
        propertyName : "value->'engine'",
        targetTable: "schema.car_engine",
        queriesPerTransaction: 20
    };

    sri2postgres.saveResources().then(function(){
    
        sri2postgres.saveResourcesInProperty(propertyConfig).then(function(result){
            result.resourcesSync
            result.resourcesNotSync
        });
    });

Will Store:

    CAR
    +--------------+-------------------------------------------------------------------------------------------------------------------+
    | key          | value                                                                                                             |
    +--------------+-------------------------------------------------------------------------------------------------------------------+
    | uuid-1       | {key: uuid-1,brand : 'Ford',model: 'Focus',color: 'RED',engine: 'https://myengine.api.com/engine-uuid-2}          |
    | uuid-2       | {key: uuid-2,brand : 'Chevrolet',model: 'Camaro',color: 'YELLOW',engine: 'https://myengine.api.com/engine-uuid-3} |
    +--------------+-------------------------------------------------------------------------------------------------------------------+

    CAR_ENGINE
    +--------------+-------------------------------------------------+
    | key          | value                                           |
    +--------------+-------------------------------------------------+
    | uuid-1       | {key: 'engine-uuid-2', type : '1.6', HP: '150'} |
    | uuid-2       | {key: 'engine-uuid-3', type : '2.0', HP: '200'} |
    +--------------+-------------------------------------------------+
    

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
        ✓ should respond to GET (526ms)
        passing null URL
            ✓ throws an error
        with basic auth
            ✓ should respond OK with valid credentials (360ms)
            ✓ should return 401 error with invalid username and password (365ms)
    
    sri2postgres save content
        ✓ should throw an error if not table is defined
        ✓ persist JSON from api to configured postgres table (765ms)
        ✓ should update the same resource if it is saved again (574ms)
    
    Accessing local json Api
        ✓ should respond to GET
    
    Connecting to a correct Postgres DataBase
        ✓ should respond with no error
    
    sri2postgres save an array of resources
        ✓ persist JSON from api to configured postgres table (51263ms)
        ✓ should saved last sync time
    
    sri2postgres.saveResources with filter
        ✓ should persist less resources than sri2postgres.saveResources without filter (61595ms)
    
    sri2Postgres read an url property from jsonb 
        ✓ should save the data content in passed table (219054ms)
    
    13 passing (7m)
