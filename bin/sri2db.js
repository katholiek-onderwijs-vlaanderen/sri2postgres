#!/usr/bin/env node

/**
 * This comand-line tool uses a js module for configuration (see config.local.js.example)
 * By default it will execute the syncType as specified for the given API in that config file,
 * but it can be overridden with --synctype <type> to execute the same sync on all the configured
 * API's (to run a nightly/weekly full sync for example)
 * By default it will try to use ./config.js as the config-file, and fallback on config.local.js
 * for local development purposes, but the config file to use can be set using --config
 * 
 * Oh yes, you can also keep it running, listening to audit-broadcast events to trigger deltaSyncs
 * with the option --listen
 * 
 * That's about it...
 */
const { Sri2Db, Sri2DbMulti } = require('../src/lib/client');
const { elapsedTimeString, elapsedTimeCalculationsToString } = require('../src/lib/utils');
const program = require('commander');



program.version('1.0.0');
program
  // .option('-v, --verbose', 'output verbosely')
  .option('-c, --config <configfile>', 'use the given configfile', './config.js')
  .option('-s, --synctype <type>', 'tell which type of sync to run (configuredSync|fullSync|deltaSync|safeDeltaSync)', 'configuredSync')
  .option('-l, --listen', 'listen to audit/broadcast server (socket.io) and trigger deltaSyncs on update');

program.parse(process.argv);


let config = null;

try {
  config = require(program.config);
} catch (e) {
  // no config found, try LOCAL config
  console.log(`Config file ${program.config} not found!\n`);
  program.outputHelp();
  process.exit(-1);
}

async function main() {
  console.log();
  try {
    const beforeSync = Date.now();

    const isMultiSync = !!config.baseConfig;

    if (!isMultiSync) { // assume simgle API config file
      console.log(`Syncing a single API ${config.api.path}`);
    }
    else { // assume multiple API's
      console.log(`Syncing ${config.overwrites.length} API's`);
    }

    const client = isMultiSync ? Sri2DbMulti(config) : Sri2Db(config);

    if (program.listen) {
      // console.log('Trying to install the broadcast listeners...');
      client.installBroadCastListeners();
    }

    let retVal = 0;
    if (program.synctype !== 'none') {
      if (isMultiSync) {
        const results = await client[program.synctype]();

        console.log('****************************************************************************************************');
        console.log('****                                RESULTS                                                     ****');
        console.log('****************************************************************************************************');
        const messages = results.map((r, i) => (
          r.isFulfilled
            ? `${r.value.config.api.path}: ${r.value.config.syncMethod} of ${r.value.amount} resources took ${elapsedTimeCalculationsToString(r.value)}`
            : `${config.overwrites[i].api.path}: ${program.synctype === 'configuredSync' ? config.overwrites[i].syncMethod || config.baseConfig.syncMethod || 'fullSync' : program.synctype} FAILED with reason: ${
              r.reason && r.reason.stack && r.reason.message ? `\n  ${r.reason.stack}` : JSON.stringify(r.reason)
            }`
        ));
        messages.forEach((m) => console.log(m));

        retVal = results.every((r) => r.isFulfilled) ? 0 : -1;

        console.log(`Syncing ${config.overwrites.length} API's took ${elapsedTimeString(beforeSync, 'm', config.overwrites.length, 'm', Date.now())}`);
      } else {
        try {
          const result = await client[program.synctype]();

          console.log('****************************************************************************************************');
          console.log('****                                RESULTS                                                     ****');
          console.log('****************************************************************************************************');
          console.log(JSON.stringify(result));
        } catch (e) {
          console.log(`Sync of ${config.overwrites[i].api.path} FAILED because ${JSON.stringify(e)}`);
        }
        console.log(`Syncing a single API took ${elapsedTimeString(beforeSync, 'm')}`);
      }

    }

    if (!program.listen) {
      process.exit(retVal);
    }
} catch (e) {
    console.log('\n\n\n');
    console.error('Something went wrong while syncing API\'s into the database', e);
    process.exit(-2);
  }
}

main();
