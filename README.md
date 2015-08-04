# sri2postgres
This is a module to sync any SRI interface to one or more database tables in a postgres 9.4 instance

# About
This library is under construction. Will be available soon.

# Tests
In order to run tests it is needed to install postgres 9.4 or superior in your machine.
see http://stackoverflow.com/a/29415300

Then you will need a user and pass for accessing your local postgres database
see also http://www.cyberciti.biz/faq/howto-add-postgresql-user-account/

We use mocha for testing. You will find a config.json file in test folder:

    {
      "apiUrl" : "http://dump.getpostman.com/status",
      "dbUser": "admin",
      "dbPassword": "admin",
      "database": "postgres",
      "dbPort": "5433",
      "dbHost": "localhost",
      "dbSsl": false
    }

All postgres related test takes its configuration from that file. So make sure it is corrected full filled.

After that you are available to run test doing:

    $ cd [your_project]
    $ mocha
    
and you will see:

    Accessing external json date Api
        ✓ should respond to GET (529ms)
    passing null URL
        ✓ throws an error
    with basic auth
        ✓ should respond OK with valid credentials (333ms)
        ✓ should return 401 error with invalid username and password (373ms)
    sri2postgres save content
        ✓ should throw an error if not table is defined
        ✓ persist JSON from api to configured postgres table (382ms)
    Accessing local json Api
        ✓ should respond to GET
    Connecting to a correct Postgres DataBase
        ✓ should respond with no error
    
    8 passing (2s)
