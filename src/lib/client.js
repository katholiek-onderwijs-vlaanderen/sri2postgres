/**
* Created by pablo in 2015
* Modified by johannes in 2018-2019
* Mostly rewritten by fre in 2019
*/

const util = require('util');
const clonedeep = require('lodash.clonedeep');
const io = require('socket.io-client');
const sriClientFactory = require('@kathondvla/sri-client/node-fetch'); // node-sri-client
const SriClientError = require('@kathondvla/sri-client/sri-client-error');
const pAll = require('p-all');
const pSettle = require('p-settle');
const jsonmergepatch = require('json-merge-patch');

const {
  removeDollarFields,
  hashCode,
  fixResourceForStoring,
  setExpandOnPath,
  elapsedTimeCalculations,
  elapsedTimeString,
  translateApiResponseToArrayOfResources,
} = require('./utils');



/**
 * It will add the proper input parameters to the request object, and return the '... IN (...)'
 * part of the query string (IN over multiple columns is not supported in MSSQL)
 *
 * @param request sql request object
 * @param {string} columnName sql table column name
 * @param {string} paramNamePrefix prefix for parameter name
 * @param type parameter type
 * @param {Array<string>} values an array of values
 *
 * @return the 'colmn IN ( @p1, @pé, ... )' part of the query
 */
function mssqlParameterizeQueryForIn(request, columnName, parameterNamePrefix, type, values) {
  const parameterNames = values.map((v, i) => `${parameterNamePrefix}${i}`);
  values.forEach((v, i) => request.input(parameterNames[i], type, v));
  return `[${columnName}] IN (${parameterNames.map(n => `@${n}`).join(',')})`;
}


/**
 * It will add the proper input parameters to the request object, and return the
 * 'VALUES (...),(...),...' part of the query string
 *
 * @param request sql request object
 * @param {string} columnName sql table column name
 * @param {string} paramNamePrefix prefix for parameter name
 * @param type parameter type
 * @param {Array<Array<string>>} values an array of arrays of values (all the tuples to be inserted)
 *
 * @return the 'colmn IN ( @p1, @pé, ... )' part of the query
 */
function mssqlParameterizeQueryForInsertValues(request, columnNames, parameterNamePrefix, types, tuples) {
  const parameterNames = tuples.map(
    (tuple, i) => tuple.map((value, j) => `${parameterNamePrefix}_${i}_${j}`),
  );
  tuples.forEach(
    (tuple, i) => tuple.forEach((value, j) => request.input(parameterNames[i][j], types[j], value)),
  );
  return `(${columnNames.map(c => `[${c}]`).join(',')}) VALUES ${parameterNames
    .map(tuple => `(${tuple.map(n => `@${n}`).join(',')})`)
    .join(',')}`;
}

/**
 * A helper containing some methods to interact with the database.
 *
 * The helper should support multiple DBs like postgres and mssql.
 */
const dbs = {};
const dbFactory = function dbFactory(configObject = {}) {
  // make a deep clone so the initialization object cannot be tampered with anymore afterwards
  const config = clonedeep(configObject);

  const mssql = ['mssql'].includes(config.type.toLowerCase()) ? require('mssql') : null;
  const pg = ['pg', 'postgres', 'postgresql'].includes(config.type.toLowerCase()) ? require('pg-promise')({ capSQL: true }) : null;
  if (!mssql && !pg) {
    throw new Error('Configuration problem, dbconfig.type must be either mssql or pg');
  }

  // We might support different read and write tables later, but for now
  // if the user gives us a table, that's the one we'll use
  if (!config.readTable && config.table) {
    // eslint-disable-next-line no-param-reassign
    config.writeTable = config.table;
    // eslint-disable-next-line no-param-reassign
    config.readTable = config.table;
  }

  if (!config.maxBulkSize) {
    config.maxBulkSize = 10000;
  }

  const lastSyncTimesTableName = 'sri2db_synctimes';
  const tempTablePrefix = Math.random().toString(26).substring(5);
  const tempTableNameForUpdates = `${mssql ? '##' : ''}sri2db_${tempTablePrefix}_updates`;
  const tempTableNameForDeletes = `${mssql ? '##' : ''}sri2db_${tempTablePrefix}_deletes`;
  const tempTableNameForSafeDeltaSync = `${mssql ? '##' : ''}sri2db_${tempTablePrefix}_safedeltasync`;
  const tempTableNameForSafeDeltaSyncInserts = `${mssql ? '##' : ''}sri2db_${tempTablePrefix}_safedeltasyncinserts`;

  function log() { console.log(`[${tempTablePrefix}]`, ...arguments); }

  // first we need to know which columns exist in order to execute the right queries
  let initialized = false;
  let baseUrlColumnExists = null;
  let pathColumnExists = null;
  let resourceTypeColumnExists = null;
  let columnsForUpserts = null; // STRING (will depend on whether baseUrlColumnExists and pathColumnExists)
  let columnsForDeletes = null; // STRING (will depend on whether baseUrlColumnExists and pathColumnExists)


  // const sqlSnippets = mssql
  //   ? { // mssql snippets
  //     path: 'LEFT(href, LEN(href) - CHARINDEX(\'/\',REVERSE(href)))',
  //   }
  //   : { // pg snippets
  //     path: 'substring(href from \'^((\/[A-Za-z]+)+)\/[^\/$]*$\')',
  //   };


  /**
   *
   * @param {*} transaction
   * @param {*} query the query string containing '<at-symbol>paramName' parts for the named params
   *  and in the case of postgres \${paramName} (prefixed with a backslash when used inside a
   *  JS template string)
   * @param {*} params if mssql: an array of { name: '', type: mssql.VarChar, value: ... }
   *   for postgres the type is not needed
   * @param {bool} explain if true will run the query prepended with 'explain analyze' first,
   *   and print the results
   */
  async function doQuery(transaction, queryString, params = [], explain = false) {
    if (explain && !queryString.trim().toLowerCase().startsWith('explain')) {
      const results = await doQuery(transaction, `EXPLAIN ${queryString}`, params, false);
      console.log('\n   # -------- ANALYZING THE FOLLOWING QUERY:\n   #');
      queryString.split('\n').forEach((r) => {
        console.log(`   # ${r}`);
      });
      results.rows.forEach((r) => {
        console.log(`   # ${r['QUERY PLAN']}`);
      });
      console.log('\n');
    }

    if (mssql) {
      try {
        const request = transaction.request();
        params.forEach(p => request.input(p.name, p.type, p.value));
        const result = await request.query(queryString);

        return result.recordset || result;
      } catch (e) {
        console.error('Error in doQuery', e);
        throw e;
      }
    } if (pg) {
      try {
        const pgParams = {};
        params.forEach(p => pgParams[p.name] = p.value);

        const result = await transaction.result(queryString, pgParams);
        return result.command === 'SELECT' ? result.rows : result;
      } catch (e) {
        console.error('Error in doQuery', e);
        throw e;
      }
    }
  }

  /**
   * Specifically for sri2db's table layout containing these columns:
   * (href, modified, jsondata, key, type [OPTIONAL], baseurl [OPTIONAL], path [OPTIONAL])
   *
   * @param {*} dbTransaction
   * @param {*} records
   * @param {*} forDeletion will influence the available columns, because we don't need the entire record if the record is to be deleted
   *  (only the href will do if all api's have their own table, otherwise baseUrl and path might be relevant too)
   * @param {string} tableName
   */
  let columnSetForDeletes = null;
  let columnSetForUpdates = null;
  async function doBulkInsert(dbTransaction, records, forDeletion = false, tableName = config.writeTable) {
    if (records && records.length === 0) {
      return 0;
    }
    // do multiple bulk inserts when the number of records the user wants to insert exceeds maxBulkSize
    if (records && records.length > config.maxBulkSize) {
      const recordsFirstPart = records.slice(0, config.maxBulkSize);
      const recordsSecondPart = records.slice(config.maxBulkSize);
      const retValFirstPart = await doBulkInsert(dbTransaction, recordsFirstPart, forDeletion, tableName);
      const retValSecondPart = await doBulkInsert(dbTransaction, recordsSecondPart, forDeletion, tableName);
      return retValFirstPart + retValSecondPart;
    }

    const beforeInsert = Date.now();

    let retVal = 0;
    if (mssql) {
      const mssqlTableName = `[${tableName}]`;
      try {
        const table = new mssql.Table(mssqlTableName);
        table.create = false; // don't try to create the table
        table.columns.add('href', mssql.VarChar(1024), { nullable: false, length: 1024 });
        if (baseUrlColumnExists) table.columns.add('baseurl', mssql.VarChar(1024), { nullable: false, length: 1024 });
        if (pathColumnExists) table.columns.add('path', mssql.VarChar(1024), { nullable: false, length: 1024 });

        if (!forDeletion) {
          table.columns.add('key', mssql.VarChar(100), /* mssql.UniqueIdentifier */ { nullable: false, length: 100 });
          table.columns.add('modified', mssql.DateTime, { nullable: false });
          table.columns.add('jsonData', mssql.NVarChar('max'), { nullable: true, length: 'max' });

          if (resourceTypeColumnExists) table.columns.add('resourceType', mssql.VarChar(100), { nullable: false, length: 100 });
        }

        if (forDeletion) {
          records.forEach((r) => {
            const addParams = [r];
            if (baseUrlColumnExists) addParams.push(config.baseUrl);
            if (pathColumnExists) addParams.push(config.path);

            table.rows.add(...addParams);
          });
        } else {
          // TODO: in case expand=NONE this will not work, cfr. pg version to fix this
          records.forEach((r) => {
            const addParams = [r.$$meta.permalink];
            if (baseUrlColumnExists) addParams.push(config.baseUrl);
            if (pathColumnExists) addParams.push(config.path);

            addParams.push(r.key);
            addParams.push(new Date(r.$$meta.modified));
            addParams.push(JSON.stringify(r));
            if (resourceTypeColumnExists) addParams.push(r.$$meta.type);

            table.rows.add(...addParams);
          });
        }

        // console.error( "    setup request" )
        const request = await dbTransaction.request();
        // console.log( "    do bulk" )
        const bulkResult = await request.bulk(table);
        // console.log( "    BULK INSERT RESULT = ", bulkResult )
        retVal = bulkResult.rowsAffected;
      } catch (err) {
        console.log(`Storing ${records.length} records ${''} in ${tableName} failed (is the table name correct, does the schema match with what we expect?)`, err);
        throw err;
      }
    } if (pg) {
      const pgTableName = tableName;

      // generate our set of columns, to be created only once, and then shared/reused,
      if ((!forDeletion && !columnSetForUpdates) || (forDeletion && !columnSetForDeletes)) {
        // to let it cache up its formatting templates for high performance:
        const columsArray = forDeletion ? ['href'] : ['href', 'key', 'modified', 'jsondata'];
        if (baseUrlColumnExists) columsArray.push('baseurl');
        if (pathColumnExists) columsArray.push('path');

        if (!forDeletion) {
          if (resourceTypeColumnExists) columsArray.push('resourcetype');
        }

        if (forDeletion) {
          columnSetForDeletes = new pg.helpers.ColumnSet(columsArray, { table: pgTableName });
        } else {
          columnSetForUpdates = new pg.helpers.ColumnSet(columsArray, { table: pgTableName });
        }
      }

      const columnSet = forDeletion ? columnSetForDeletes : columnSetForUpdates;

      // data input values:
      const values = records.map((r) => {
        if (forDeletion) {
          const value = {
            href: r,
          };
          if (baseUrlColumnExists) value.baseurl = config.baseUrl;
          if (pathColumnExists) value.path = config.path;

          return value;
        // eslint-disable-next-line no-else-return
        } else {
          let value = null;
          if (typeof r === 'string') {
            // THIS IS THE CASE WHEN expand=NONE
            value = {
              href: r,
              key: r.substring(r.lastIndexOf('/') + 1),
              modified: null,
              jsondata: null,
            };
            if (resourceTypeColumnExists) value.resourcetype = null;
            if (baseUrlColumnExists) value.baseurl = config.baseUrl;
            if (pathColumnExists) value.path = config.path;
          } else {
            value = {
              href: r.$$meta.permalink,
              key: r.key,
              modified: new Date(r.$$meta.modified),
              jsondata: JSON.stringify(r),
            };
            if (resourceTypeColumnExists) value.resourcetype = r.$$meta.type;
            if (baseUrlColumnExists) value.baseurl = config.baseUrl;
            if (pathColumnExists) value.path = config.path;
          }
          return value;
        }
      });

      // generating a multi-row insert query:
      const query = pg.helpers.insert(values, columnSet);

      // executing the query:
      const result = await dbTransaction.result(query);
      retVal = result.rowCount;
    }

    console.log(`  [doBulkInsert] Inserted ${retVal} rows (${forDeletion ? columnsForDeletes : columnsForUpserts}) into ${tableName} in ${elapsedTimeString(beforeInsert, 'ms', retVal, 's')}`);
    return retVal;
  }


  async function getTableColumns(dbTransaction, tableName) {
    if (mssql) {
      return doQuery(
        dbTransaction,
        `select schema_name(tab.schema_id) as schema_name,
            tab.name as table_name,
            col.column_id,
            col.name as column_name,
            t.name as data_type,
            col.max_length,
            col.precision
        from sys.tables as tab
            inner join sys.columns as col
                on tab.object_id = col.object_id
            left join sys.types as t
            on col.user_type_id = t.user_type_id
        where schema_name(tab.schema_id) = @schemaName AND tab.name = @tableName
        order by schema_name,
            table_name,
            column_id;
        `,
        [
          { name: 'schemaName', type: mssql.VarChar, value: config.schema },
          { name: 'tableName', type: mssql.VarChar, value: tableName },
        ],
      );
      // eslint-disable-next-line no-else-return
    } else {
      return doQuery(dbTransaction, `
        SELECT
          table_schema as schema_name,
          table_name as table_name,
          ordinal_position as column_id,
          column_name as column_name,
          udt_name as data_type,
          character_maximum_length as max_length,
          numeric_precision as precision
        FROM
          information_schema.COLUMNS
        WHERE
          table_schema = \${schemaName} AND TABLE_NAME = \${tableName}
        ORDER BY table_schema, table_name, ordinal_position;
          ;
      `,
      [
        { name: 'schemaName', value: config.schema },
        { name: 'tableName', value: tableName },
      ]);
    }
  }

  /**
   * Will create a table to store the last sync times, that will be used to retrieve
   * the last sync date depending on the sync type (FULL, DELTA, SAFEDELTA, ...):
   *
   * | tablename | baseurl | path | synctype | timestamp |
   *
   * @param {*} dbTransaction
   */
  async function createLastSyncTimesTableIfNecessary(dbTransaction) {
    return doQuery(
      dbTransaction,
      (
        mssql
          ? `IF OBJECT_ID(N'[${config.schema}].[${lastSyncTimesTableName}]') IS NULL
            BEGIN
              CREATE TABLE [${config.schema}].[${lastSyncTimesTableName}] (
                tablename varchar(1024) NOT NULL,
                baseurl varchar(1024) NOT NULL,
                path varchar(1024) NOT NULL,
                synctype varchar(64) NOT NULL,
                lastmodified bigint NOT NULL,
                syncstart bigint
              );
            END;
            -- And add the necessary indexes for fast retrieval
            `
          : `CREATE TABLE IF NOT EXISTS ${config.schema}.${lastSyncTimesTableName} (
              tablename varchar(1024) NOT NULL,
              baseurl varchar(1024) NOT NULL,
              path varchar(1024) NOT NULL,
              synctype varchar(64) NOT NULL,
              lastmodified bigint NOT NULL,
              syncstart bigint
            );
            -- And add the necessary indexes for fast retrieval
            DO $$
              BEGIN
                IF NOT EXISTS (
                  SELECT conrelid
                  FROM pg_constraint
                  WHERE  conname = '${lastSyncTimesTableName}_unique_tablename_baseurl_path_synctype'
                )
                THEN
                  ALTER TABLE ${config.schema}.${lastSyncTimesTableName} ADD CONSTRAINT ${lastSyncTimesTableName}_unique_tablename_baseurl_path_synctype
                  UNIQUE (tablename, baseurl, path, synctype);
                END IF;
              END;
            $$
            `
      ),
    );
  }

  /**
   * Get connection from pool and open a transaction on it
   */
  async function openTransaction() {
    const confAsString = `${config.type}|${config.host}|${config.database}|${config.username}|${hashCode(config.password)}`;

    let poolOrConnection = null;
    if (mssql) {
      if (!dbs[confAsString]) {
        const mssqlconfig = {
          server: config.host,
          database: config.database,
          user: config.username,
          password: config.password,
          pool: {
            max: 10,
            min: 0,
            idleTimeoutMillis: 60 * 60 * 1000, // 1 hour
            connectionTimeout: 60 * 60 * 1000, // 1 hour
          },
          requestTimeout: 60 * 60 * 1000, // 1 hour
        };

        poolOrConnection = new mssql.ConnectionPool(mssqlconfig);
        // poolOrConnection.on('error', (err) => {
        //   console.error('connection pool error', err);
        // });
        await poolOrConnection.connect();

        dbs[confAsString] = poolOrConnection;
      } else {
        poolOrConnection = dbs[confAsString];
      }
    } else if (pg) {
      if (!dbs[confAsString]) {
        const pgconfig = {
          ...config,
          user: config.username,
          // pool: {
          max: 10,
          min: 0,
          idleTimeoutMillis: 10 * 60 * 1000,
          connectionTimeout: 60 * 60 * 1000, // 1 hour
          connectionTimeoutMillis: 60 * 60 * 1000, // 1 hour
          query_timeout: 60 * 60 * 1000,
          keepAlive: true,
          // },
          capSQL: true, // capitalize all generated SQL
        };

        dbs[confAsString] = pg(pgconfig);
      }

      const db = dbs[confAsString];
      poolOrConnection = await db.connect();
    }

    let transaction = null;
    if (mssql) {
      transaction = poolOrConnection.transaction();
      await transaction.begin();
    } else if (pg) {
      // await poolOrConnection.none('SET AUTOCOMMIT = OFF');
      await poolOrConnection.none('BEGIN');
      transaction = poolOrConnection;
    }

    if (!initialized) {
      const tableColumns = await getTableColumns(transaction, config.writeTable);
      const tableColumnNames = tableColumns.map(c => c.column_name);
      columnsForUpserts = mssql ? `[${tableColumnNames.join('],[')}]` : tableColumnNames.join();
      console.log('TableColumns', columnsForUpserts);
      resourceTypeColumnExists = !!tableColumns.find(e => e.column_name.toLowerCase() === 'resourcetype');
      baseUrlColumnExists = !!tableColumns.find(e => e.column_name.toLowerCase() === 'baseurl');
      pathColumnExists = !!tableColumns.find(e => e.column_name.toLowerCase() === 'path');

      const tableColumnNamesForDeletes = ['href'];
      if (baseUrlColumnExists) tableColumnNamesForDeletes.push('baseurl');
      if (pathColumnExists) tableColumnNamesForDeletes.push('path');
      columnsForDeletes = mssql ? `[${tableColumnNamesForDeletes.join('],[')}]` : tableColumnNamesForDeletes.join();


      const createLastSyncTimesTableResults = await createLastSyncTimesTableIfNecessary(transaction);

      if (mssql) {
        // TODO?
      } else if (pg) {
        transaction.query(`SET search_path TO ${config.schema},public;`);
      }

      initialized = true;
    }

    return transaction;
  }

  /**
   * commit transaction and return connection to the pool
   */
  async function commitTransaction(transaction) {
    if (mssql && transaction) {
      return transaction.commit();
    } if (pg && transaction) {
      await transaction.none('COMMIT');
      await transaction.done();
      // poolOrConnection = null;
      return true;
    }
    return 0;
  }

  /**
   * rollback transaction and return connection to the pool
   */
  async function rollbackTransaction(transaction) {
    if (mssql) {
      return transaction.rollback();
    }
    if (pg) {
      await transaction.none('ROLLBACK');
      await transaction.done();
      return true;
    }
    return 0;
  }


  const db = {
    /**
     * In deltasync(...) this date will be updated, taking into account the duration
     * of the sync operation in order to avoid skipping resources unexpectedly.
     * It might be unsafe when getting it from the database, because in that case
     * it will simply find the most recent 'modified'.
     * If the api changes during a sync, you can actually 'forget' to fetch some records
     * when only syncing from that most recently known modified date.
     * So it's probably best to use setLastSyncDate first and take plenty of overlap
     * in 'cold start' situations.
     *
     * @returns the last sync date from the most recent sync this process has run,
     *   or (on a cold start) null if DB is empty and the most recent 'modified' otherwise
     */
    getLastSyncDates: async function getLastSyncDates(syncType, transaction = null) {
      const myTransaction = transaction || await openTransaction();

      try {
        console.log(`    getLastSyncDate ${syncType} sync on ${config.readTable} for ${config.path}`);

        if (mssql) {
          const result = await doQuery(myTransaction,
            `select lastmodified, syncstart
            from [${config.schema}].[${lastSyncTimesTableName}]
            where tablename = @tableName
              AND baseurl = @baseUrl
              AND path = @path
              AND synctype = @syncType
            `,
            [
              { name: 'tableName', type: mssql.VarChar, value: config.table },
              { name: 'baseUrl', type: mssql.VarChar, value: config.baseUrl },
              { name: 'path', type: mssql.VarChar, value: config.path },
              { name: 'syncType', type: mssql.VarChar, value: syncType },
            ]);
          return result.length > 0 ? { lastModified: Number.parseInt(result[0].lastmodified), syncStart: Number.parseInt(result[0].syncstart) } : null;

          // const result = await doQuery(myTransaction,
          //   `select [modified]
          //   from [${config.schema}].[${config.readTable}]
          //   where 1=1
          //     ${baseUrlColumnExists ? 'AND baseurl = @baseUrl' : ''}
          //     ${pathColumnExists ? 'AND path = @path' : ''}
          //   order by [modified] DESC
          //   OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY`, // where order by d DESC limit 1
          //   [
          //     { name: 'baseUrl', value: config.baseUrl, type: mssql.VarChar },
          //     { name: 'path', value: config.path, type: mssql.VarChar },
          //   ]);
          // return result.length > 0 ? result[0].modified : null;
        }
        if (pg) {
          const result = await doQuery(myTransaction,
            `select lastmodified, syncstart
            from ${config.schema}.${lastSyncTimesTableName}
            where tablename = \${tableName}
              AND baseurl = \${baseUrl}
              AND path = \${path}
              AND synctype = \${syncType}
            `,
            [
              { name: 'tableName', value: config.table },
              { name: 'baseUrl', value: config.baseUrl },
              { name: 'path', value: config.path },
              { name: 'syncType', value: syncType },
            ]);
          return result.length > 0 ? { lastModified: Number.parseInt(result[0].lastmodified), syncStart: result[0].syncstart } : null;

          // const result = await doQuery(myTransaction,
          //   `select modified
          //   from ${config.schema}.${config.readTable}
          //   where 1=1
          //     ${baseUrlColumnExists ? 'AND baseurl = ${baseUrl}' : ''}
          //     ${pathColumnExists ? 'AND path = ${path}' : ''}
          //   order by modified DESC
          //   LIMIT 1`,
          //   [
          //     { name: 'baseUrl', value: config.baseUrl },
          //     { name: 'path', value: config.path },
          //   ]);
          // return result.length > 0 ? result[0].modified : null;
        }
        return null;
      } catch (e) {
        if (!transaction) db.rollbackTransaction(myTransaction);
        throw new Error('Something went wrong while trying to query the DB', e);
      } finally {
        if (!transaction) await db.commitTransaction(myTransaction);
      }
    },
    /**
     * Store the last sync date for the given syncType (like DELTA, FULL, SAFEDELTA) on the DB
     * so it can be retrieved later with getLastSyncDate
     *
     * @param {String} syncType for example DELTA, FULL, SAFEDELTA
     * @param {Date} lastSyncDate
     * @param {*} transaction (OPTIONAL)
     */
    setLastSyncTimestamps: async function setLastSyncTimestamps(syncType, lastModified, syncStart, transaction = null) {
      const myTransaction = transaction || await openTransaction();
      try {
        console.log(`    setLastSyncDate ${syncType} sync on ${config.readTable} for ${config.path}`);

        if (mssql) {
          const result = await doQuery(myTransaction,
            `DELETE FROM [${config.schema}].[${lastSyncTimesTableName}] WHERE tablename = @tableName AND baseurl = @baseUrl AND path = @path AND synctype = @syncType;
            INSERT INTO [${config.schema}].[${lastSyncTimesTableName}] (tablename, baseurl, path, synctype, lastmodified, syncstart)
            VALUES (@tableName, @baseUrl, @path, @syncType, @lastModified, @syncStart);
            `,
            [
              { name: 'tableName', type: mssql.VarChar, value: config.table },
              { name: 'baseUrl', type: mssql.VarChar, value: config.baseUrl },
              { name: 'path', type: mssql.VarChar, value: config.path },
              { name: 'syncType', type: mssql.VarChar, value: syncType },
              { name: 'lastModified', type: mssql.BigInt, value: lastModified },
              { name: 'syncStart', type: mssql.BigInt, value: syncStart },
            ]);
          return true; // result.length > 0 ? result[0].modified : null;
        }
        if (pg) {
          const result = await doQuery(myTransaction,
            `DELETE FROM ${config.schema}.${lastSyncTimesTableName} WHERE tablename = \${tableName} AND baseurl = \${baseUrl} AND path = \${path} AND synctype = \${syncType};
            INSERT INTO ${config.schema}.${lastSyncTimesTableName} as t (tablename, baseurl, path, synctype, lastmodified, syncstart)
            VALUES (\${tableName}, \${baseUrl}, \${path}, \${syncType}, \${lastModified}, \${syncStart});
            `,
            // `INSERT INTO ${config.schema}.${lastSyncTimesTableName} as t (tablename, baseurl, path, synctype, lastmodified, syncstart)
            // VALUES (\${tableName}, \${baseUrl}, \${path}, \${syncType}, \${lastModified}, \${syncStart})
            // ON CONFLICT (tablename, baseurl, path, synctype)
            //   DO UPDATE
            //   SET lastmodified = \${lastModified}, syncstart = \${syncStart}
            //   WHERE t.tablename = \${tableName} AND t.baseurl = \${baseUrl} AND t.path = \${path} AND t.synctype = \${syncType}
            // `,
            [
              { name: 'tableName', value: config.table },
              { name: 'baseUrl', value: config.baseUrl },
              { name: 'path', value: config.path },
              { name: 'syncType', value: syncType },
              { name: 'lastModified', value: lastModified },
              { name: 'syncStart', value: syncStart },
            ]);
          return result.rowCount;
        }
        return null;
      } catch (e) {
        if (!transaction) db.rollbackTransaction(myTransaction);
        throw new Error('Something went wrong while trying to set last sync date on the DB', e);
      } finally {
        if (!transaction) await db.commitTransaction(myTransaction);
      }
    },


    checkIfTableExists: async function checkIfTableExists(transaction) {
      try {
        let retVal = false;
        if (mssql) {
          retVal = await doQuery(transaction, `SELECT CASE WHEN
                EXISTS (
                  SELECT 1
                  FROM   INFORMATION_SCHEMA.TABLES
                  WHERE  TABLE_SCHEMA = @schemaName
                  AND    TABLE_NAME = @tableName
                )
              THEN CAST(1 AS BIT)
              ELSE CAST(0 AS BIT)
              END AS [exists];`,
          [
            { name: 'schemaName', type: mssql.VarChar, value: config.schema },
            { name: 'tableName', type: mssql.VarChar, value: config.table },
          ]);
        } else if (pg) {
          retVal = await doQuery(transaction, `SELECT EXISTS (
            SELECT 1
            FROM   pg_tables
            WHERE  schemaname = \${schemaName}
            AND    tablename = \${tableName}
          );`,
          [
            { name: 'schemaName', value: config.schema },
            { name: 'tableName', value: config.table },
          ]);
        }
        return retVal[0].exists;
      } catch (e) {
        console.log('[checkIfTableExists] FAILED', e, e.stack);
        throw new Error('checkIfTableExists failed', e);
      }
    },

    /**
     * This creates the temp tables if they don't exist yet or empties them if they
     * do exist already!
     *
     * @param {*} transaction
     */
    createTempTables: async function createTempTables(transaction) {
      try {
        if (mssql) {
          const makeCreateTempTableString = (tblName, forDeletes) => `
              IF OBJECT_ID(N'tempdb..${tblName}') IS NULL
              BEGIN
                SELECT TOP 0 ${forDeletes ? columnsForDeletes : '*'}
                INTO [${tblName}]
                FROM [${config.schema}].[${config.writeTable}];
              END;
              TRUNCATE TABLE [${tblName}];`;

          // TEST
          // const testResults = await (await transaction.request())
          //   .query(`SELECT OBJECT_ID(N'tempdb..${tempTableNameForUpdates}') as tableId`);
          // console.log('testResults', testResults.recordset[0]);
          const beforeCreateUpdatesTable = new Date();
          await doQuery(transaction, makeCreateTempTableString(tempTableNameForUpdates, false));
          console.log(`  Created temporary table for updated rows (${tempTableNameForUpdates}) in ${elapsedTimeString(beforeCreateUpdatesTable, 'ms')}\t${config.path}`);

          const beforeCreateDeletesTable = new Date();
          await doQuery(transaction, makeCreateTempTableString(tempTableNameForDeletes, true));
          console.log(`  Created temporary table for deleted hrefs (${tempTableNameForDeletes}) in ${elapsedTimeString(beforeCreateDeletesTable, 'ms')}\t${config.path}`);

          const beforeCreateSafeDeltaSyncTable = new Date();
          await doQuery(transaction, makeCreateTempTableString(tempTableNameForSafeDeltaSync, true));
          console.log(`  Created temporary table for safe delta sync all hrefs (${tempTableNameForSafeDeltaSync}) in ${elapsedTimeString(beforeCreateSafeDeltaSyncTable, 'ms')}\t${config.path}`);

          const beforeCreateSafeDeltaSyncInsertsTable = new Date();
          await doQuery(transaction, makeCreateTempTableString(tempTableNameForSafeDeltaSyncInserts, false));
          console.log(`  Created temporary table for safe delta sync rows to be inserted ((${tempTableNameForSafeDeltaSyncInserts})) in ${elapsedTimeString(beforeCreateSafeDeltaSyncInsertsTable, 'ms')}\t${config.path}`);
        } else if (pg) {
          const makeCreateTempTableString = (tblName, forDeletes) => `
              CREATE GLOBAL TEMPORARY TABLE IF NOT EXISTS ${tblName}
              AS SELECT ${forDeletes ? columnsForDeletes : '*'} 
                 FROM ${config.schema}.${config.writeTable}
                 LIMIT 0;
              TRUNCATE ${tblName};`;

          const beforeCreateUpdatesTable = new Date();
          await doQuery(transaction, makeCreateTempTableString(tempTableNameForUpdates, false));
          console.log(`  Created temporary table for updated rows (${tempTableNameForUpdates}) in ${elapsedTimeString(beforeCreateUpdatesTable, 'ms')}\t${config.path}`);

          const beforeCreateDeletesTable = new Date();
          await doQuery(transaction, makeCreateTempTableString(tempTableNameForDeletes, true));
          console.log(`  Created temporary table for deleted rows (${tempTableNameForDeletes}) in ${elapsedTimeString(beforeCreateDeletesTable, 'ms')}\t${config.path}`);

          const beforeCreateSafeDeltaSyncTable = new Date();
          await doQuery(transaction, makeCreateTempTableString(tempTableNameForSafeDeltaSync, true));
          console.log(`  Created temporary table for safe delta sync all rows (${tempTableNameForSafeDeltaSync}) in ${elapsedTimeString(beforeCreateSafeDeltaSyncTable, 'ms')}\t${config.path}`);

          const beforeCreateSafeDeltaSyncInsertsTable = new Date();
          await doQuery(transaction, makeCreateTempTableString(tempTableNameForSafeDeltaSyncInserts, false));
          console.log(`  Created temporary table for safe delta sync rows to be inserted (${tempTableNameForSafeDeltaSyncInserts}) in ${elapsedTimeString(beforeCreateSafeDeltaSyncInsertsTable, 'ms')}\t${config.path}`);
        }
      } catch (e) {
        console.log('Creating temp tables failed', e, e.stack);
        throw new Error('Creating temp tables failed', e);
      }
    },
    /**
     * Copies the updates and deletes tables, safe delta sync will need some extra steps after this one...
     * @param {*} transaction
     * @param {boolean} fullSync
     */
    copyTempTablesDataToWriteTable: async function copyTempTablesDataToWriteTable(transaction, fullSync = false) {
      try {
        if (mssql) {
          const beforeDelete = Date.now();
          // const deleteResults = await deleteRequest.query(`DELETE FROM [${config.schema}].[${config.writeTable}]
          //   WHERE EXISTS (select 1 from [${tempTableNameForDeletes}] AS t where t.[key] = [${config.schema}].[${config.writeTable}].[key] AND t.resourceType = [${config.schema}].[${config.writeTable}].resourceType)`);

          const fullSyncDeletesAll = config.preferUpdatesOverInserts === undefined ? false : !config.preferUpdatesOverInserts;
          const fullSyncDeleteQuery = fullSyncDeletesAll
            ? `DELETE w
              FROM [${config.schema}].[${config.writeTable}] w
              WHERE 1=1
                ${baseUrlColumnExists ? 'AND w.baseurl = @baseurl' : ''}
                ${pathColumnExists ? 'AND w.path = @path' : ''}
            `
            : `DELETE w FROM [${config.schema}].[${config.writeTable}] w
              WHERE NOT EXISTS (
                SELECT 1
                FROM [${tempTableNameForUpdates}] i
                WHERE i.[href] = w.[href]
                  ${baseUrlColumnExists ? 'AND i.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND i.path = w.path' : ''}
              )
              ${baseUrlColumnExists ? 'AND w.baseurl = @baseurl' : ''}
              ${pathColumnExists ? 'AND w.path = @path' : ''}
            `;


          const deltaSyncDeleteQuery = `
            DELETE w FROM [${config.schema}].[${config.writeTable}] w
            INNER JOIN [${tempTableNameForDeletes}] t
              ON t.[href] = w.[href]
                ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                ${pathColumnExists ? 'AND t.[path] = w.[path]' : ''}
          `;

          const deleteQuery = fullSync ? fullSyncDeleteQuery : deltaSyncDeleteQuery;

          const deleteResults = await doQuery(
            transaction,
            deleteQuery,
            fullSync
              ? [
                { name: 'baseurl', type: mssql.VarChar, value: config.baseUrl },
                { name: 'path', type: mssql.VarChar, value: config.path },
              ]
              : [],
          );
          console.log(`  -> Deleted ${deleteResults.rowsAffected[0]} rows from ${config.writeTable} in ${elapsedTimeString(beforeDelete, 's', deleteResults.rowsAffected[0])}`);

          if (fullSync && fullSyncDeletesAll) {
            console.log(`  -> No updates needed because the full sync deleted all records first`);
          } else {
            const beforeUpdate = Date.now();
            const updateResults = await doQuery(transaction, `UPDATE w
              SET w.modified = t.modified, w.jsonData = t.jsonData
              FROM [${tempTableNameForUpdates}] t
              INNER JOIN [${config.schema}].[${config.writeTable}] w
                ON t.[href] = w.[href]
                  ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND t.path = w.path' : ''}
            `);
            console.log(`  -> Updated ${updateResults.rowsAffected[0]} rows from ${config.writeTable} in ${elapsedTimeString(beforeUpdate, 's', updateResults.rowsAffected[0])}`);
          }

          const beforeInsert = Date.now();
          // some records can appear multiple times if the result set changes while we are fetching
          // the pages, so try to remove doubles before inserting (take the one with most recent
          // modified)

          const insertQueryBase = `
            INSERT INTO [${config.schema}].[${config.writeTable}](
              href, [key], modified, jsonData
              ${resourceTypeColumnExists ? ', resourcetype' : ''}
              ${baseUrlColumnExists ? ', baseurl' : ''}
              ${pathColumnExists ? ', path' : ''}
            )
            SELECT t.href, t.[key], t.modified, t.jsonData
              ${resourceTypeColumnExists ? ', t.resourcetype' : ''}
              ${baseUrlColumnExists ? ', t.baseurl' : ''}
              ${pathColumnExists ? ', t.path' : ''}
            FROM (SELECT *,
                    ROW_NUMBER() over (partition by
                        ${baseUrlColumnExists ? 'baseurl,' : ''}
                        ${pathColumnExists ? 'path,' : ''}
                        href
                      ORDER BY modified DESC) as rowNumber
                  FROM [${tempTableNameForUpdates}]) t
            WHERE t.rowNumber = 1`;

          let insertQueryExtra = '';
          if (fullSync && fullSyncDeletesAll) {
            console.log('  (insert query won\'t check if row already exists, because we have deleted all rows first)');
          } else {
            insertQueryExtra = `
              AND NOT EXISTS (
                select 1 from [${config.schema}].[${config.writeTable}] w
                where t.[href] = w.[href]
                  ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND t.path = w.path' : ''}
                )
            `;
          }

          const insertQuery = insertQueryBase + insertQueryExtra;

          const insertResults = await doQuery(transaction, insertQuery);
          console.log(`  -> Inserted ${insertResults.rowsAffected[0]} rows into ${config.writeTable} in ${elapsedTimeString(beforeInsert, 's', insertResults.rowsAffected[0])}`);
        } else if (pg) {
          const w = `${config.schema}.${config.writeTable}`;

          // const tableResults = await doQuery(
          //   transaction,
          //   `SELECT 'deletes' as table, count(*) FROM ${tempTableNameForDeletes}
          //   UNION ALL
          //   SELECT 'updates' as table, count(*) FROM ${tempTableNameForUpdates}
          //   UNION ALL
          //   SELECT 'safedelta keys' as table, count(*) FROM ${tempTableNameForSafeDeltaSync}
          //   UNION ALL
          //   SELECT 'safedelta inserts' as table, count(*) FROM ${tempTableNameForSafeDeltaSyncInserts}
          //   `,
          // );
          // console.log(`  -> Nr of rows per temp table: ${JSON.stringify(tableResults)}`);


          const beforeDelete = Date.now();

          const fullSyncDeletesAll = !config.preferUpdatesOverInserts;
          const fullSyncDeleteQuery = fullSyncDeletesAll
            ? `DELETE FROM ${config.schema}.${config.writeTable} w
              WHERE 1=1
                ${baseUrlColumnExists ? 'AND baseurl = ${baseUrl}' : ''}
                ${pathColumnExists ? 'AND path = ${path}' : ''}
            `
            : `DELETE FROM ${config.schema}.${config.writeTable} w
              WHERE NOT EXISTS (
                SELECT 1 FROM ${tempTableNameForUpdates} t
                WHERE t.href = w.href
                  ${baseUrlColumnExists ? 'AND t.baseurl = w.baseUrl' : ''}
                  ${pathColumnExists ? 'AND t.path = w.path' : ''}
              )
              ${baseUrlColumnExists ? 'AND baseurl = ${baseUrl}' : ''}
              ${pathColumnExists ? 'AND path = ${path}' : ''}
            `;

          // delete first seems only useful for full syncs
          // (because in that the delete can be done without a joini)
          const deltaSyncDeletesUpdatedRowsFirst = false; // !config.preferUpdatesOverInserts;
          const deltaSyncDeleteQuery = deltaSyncDeletesUpdatedRowsFirst
            ? `DELETE FROM ${config.schema}.${config.writeTable} w
              USING ${tempTableNameForDeletes} t
              WHERE w.href = t.href
                ${baseUrlColumnExists ? 'AND w.baseurl = t.baseurl' : ''}
                ${pathColumnExists ? 'AND w.path = t.path' : ''};

              DELETE FROM ${config.schema}.${config.writeTable} w
              USING ${tempTableNameForUpdates} t
              WHERE w.href = t.href
                ${baseUrlColumnExists ? 'AND w.baseurl = t.baseurl' : ''}
                ${pathColumnExists ? 'AND w.path = t.path' : ''};
            `
            : `DELETE FROM ${config.schema}.${config.writeTable} w
            USING ${tempTableNameForDeletes} t
            WHERE w.href = t.href
              ${baseUrlColumnExists ? 'AND w.baseurl = t.baseurl' : ''}
              ${pathColumnExists ? 'AND w.path = t.path' : ''}
            `;

          const deleteResults = await doQuery(
            transaction,
            fullSync ? fullSyncDeleteQuery : deltaSyncDeleteQuery,
            fullSync
              ? [
                { name: 'baseUrl', value: config.baseUrl },
                { name: 'path', value: config.path },
              ]
              : [],
          );
          console.log(`  -> Deleted ${deleteResults.rowCount} rows from ${config.writeTable} in ${elapsedTimeString(beforeDelete, 's', deleteResults.rowCount)}`);

          if (fullSync && fullSyncDeletesAll) {
            console.log(`  -> No updates needed because the full sync deleted all records first`);
          }
          else if (!fullSync && deltaSyncDeletesUpdatedRowsFirst) {
            console.log(`  -> No updates needed because the delta sync deleted all updated records first`);
          } else {
            const beforeUpdate = Date.now();
            const updateResults = await doQuery(transaction, `UPDATE ${w}
              SET modified = t.modified, jsonData = t.jsonData
              FROM ${tempTableNameForUpdates} t
              WHERE ${w}.href = t.href
                ${baseUrlColumnExists ? `AND ${w}.baseurl = t.baseurl` : ''}
                ${pathColumnExists ? `AND ${w}.path = t.path` : ''}
            `);
            console.log(`  -> Updated ${updateResults.rowCount} rows from ${config.writeTable} in ${elapsedTimeString(beforeUpdate, 's', updateResults.rowCount)}`);
          }

          const beforeInsert = Date.now();

          // some records can appear multiple times if the result set changes while we are fetching
          // the pages, so try to remove doubles before inserting (take the one with most recent
          // modified)
          const insertQueryBase = `INSERT INTO ${w}(
              href, key, modified, jsonData
              ${resourceTypeColumnExists ? ', resourcetype' : ''}
              ${baseUrlColumnExists ? ', baseurl' : ''}
              ${pathColumnExists ? ', path' : ''}
            )
            SELECT t.href, t.key, t.modified, t.jsonData
              ${resourceTypeColumnExists ? ', resourcetype' : ''}
              ${baseUrlColumnExists ? ', baseurl' : ''}
              ${pathColumnExists ? ', path' : ''}
            FROM (SELECT *,
                    ROW_NUMBER() OVER (partition by
                      ${baseUrlColumnExists ? 'baseurl,' : ''}
                      ${pathColumnExists ? 'path,' : ''}
                      href
                      ORDER BY modified DESC) as rowNumber
                  FROM ${tempTableNameForUpdates}) t
            WHERE t.rowNumber = 1
          `;

          let insertQueryExtra = '';
          if (fullSync && fullSyncDeletesAll) {
            console.log('  (insert query won\'t check if row already exists, because we have deleted all rows first)');
          } else {
            insertQueryExtra = `
              AND NOT EXISTS (
                select 1 from ${w} w
                where t.href = w.href
                  ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND t.path = w.path' : ''}
              )
            `;
          }

          const insertQuery = insertQueryBase + insertQueryExtra;

          const insertResults = await doQuery(transaction, insertQuery);
          console.log(`  -> Inserted ${insertResults.rowCount} rows into ${config.writeTable} in ${elapsedTimeString(beforeInsert, 's', insertResults.rowCount)}`);
        }
      } catch (e) {
        console.log('copyTempTablesDataToWriteTable failed', e, e.stack);
        throw new Error('copyTempTablesDataToWriteTable failed', e);
      }
      return 0;
    },
    /**
     * For safesync: REMOVE all records from the DB whose href is not in 'tempTableNameForSafeDeltaSync'
     *  and INSERT all records in 'tempTableNameForSafeDeltaSyncInserts'
     * @param {*} transaction
     */
    copySafeSyncTempTablesDataToWriteTable: async function copySafeSyncTempTablesDataToWriteTable(transaction) {
      try {
        if (mssql) {
          const beforeDelete = Date.now();
          const deleteResults = await doQuery(
            transaction,
            `DELETE w FROM [${config.schema}].[${config.writeTable}] w
              WHERE NOT EXISTS (
                SELECT 1
                FROM [${tempTableNameForSafeDeltaSync}] i
                WHERE i.[href] = w.[href]
                  ${baseUrlColumnExists ? 'AND i.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND i.path = w.path' : ''}
               )
            `,
            [
              { name: 'baseUrl', value: config.baseUrl, type: mssql.VarChar },
              { name: 'path', value: config.path, type: mssql.VarChar },
            ],
          );

          console.log(`  -> Deleted ${deleteResults.rowsAffected[0]} rows from ${config.writeTable} in ${elapsedTimeString(beforeDelete, 's', deleteResults.rowsAffected[0])}`);

          const beforeInsert = Date.now();

          // some records can appear multiple times if the result set changes while we are fetching
          // the pages, so try to remove doubles before inserting (take the one with most recent
          // modified)
          const insertQuery = `
            INSERT INTO [${config.schema}].[${config.writeTable}](
              href, [key], modified, jsonData
              ${resourceTypeColumnExists ? ', resourcetype' : ''}
              ${baseUrlColumnExists ? ', baseurl' : ''}
              ${pathColumnExists ? ', path' : ''}
            )
            SELECT t.href, t.[key], t.modified, t.jsonData
              ${resourceTypeColumnExists ? ', t.resourcetype' : ''}
              ${baseUrlColumnExists ? ', t.baseurl' : ''}
              ${pathColumnExists ? ', t.path' : ''}
            FROM (SELECT *,
                    ROW_NUMBER() over (partition by
                        ${baseUrlColumnExists ? 'baseurl,' : ''}
                        ${pathColumnExists ? 'path,' : ''}
                        href
                      ORDER BY modified DESC) as rowNumber
                  FROM [${tempTableNameForSafeDeltaSyncInserts}]) t
            WHERE t.rowNumber = 1
              AND NOT EXISTS (
                select 1 from [${config.schema}].[${config.writeTable}] w
                where t.[href] = w.[href]
                  ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND t.path = w.path' : ''}
                )`;
          const insertResults = await doQuery(transaction, insertQuery);
          console.log(`  -> Inserted ${insertResults.rowsAffected[0]} rows into ${config.writeTable} in ${elapsedTimeString(beforeInsert, 's', insertResults.rowsAffected[0])}`);
          return insertResults.rowsAffected[0] + deleteResults.rowsAffected[0];
        } if (pg) {
          const beforeDelete = Date.now();
          const deleteResults = await doQuery(
            transaction,
            `DELETE FROM ${config.schema}.${config.writeTable}
              WHERE (${columnsForDeletes}) NOT IN (
                SELECT ${columnsForDeletes} FROM ${tempTableNameForSafeDeltaSync}
              )
              ${baseUrlColumnExists ? 'AND baseurl = ${baseUrl}' : ''}
              ${pathColumnExists ? 'AND path = ${path}' : ''}
            `,
            [
              { name: 'baseUrl', value: config.baseUrl },
              { name: 'path', value: config.path },
            ],
          );
          console.log(`  -> Deleted ${deleteResults.rowCount} rows from ${config.writeTable} in ${elapsedTimeString(beforeDelete, 's', deleteResults.rowCount)}`);

          const beforeInsert = Date.now();

          // some records can appear multiple times if the result set changes while we are fetching
          // the pages, so try to remove doubles before inserting (take the one with most recent
          // modified)
          const w = `${config.schema}.${config.writeTable}`;
          const insertResults = await transaction.result(`INSERT INTO ${w}(
              href, key, modified, jsonData
              ${resourceTypeColumnExists ? ', resourcetype' : ''}
              ${baseUrlColumnExists ? ', baseurl' : ''}
              ${pathColumnExists ? ', path' : ''}
            )
            SELECT t.href, t.key, t.modified, t.jsonData
              ${resourceTypeColumnExists ? ', resourcetype' : ''}
              ${baseUrlColumnExists ? ', baseurl' : ''}
              ${pathColumnExists ? ', path' : ''}
            FROM (SELECT *,
                    ROW_NUMBER() over (partition by
                        ${baseUrlColumnExists ? 'baseurl,' : ''}
                        ${pathColumnExists ? 'path,' : ''}
                        href
                      ORDER BY modified DESC) as rowNumber
                  FROM ${tempTableNameForSafeDeltaSyncInserts}) t
            WHERE t.rowNumber = 1
              AND NOT EXISTS (
                  select 1 from ${w} w
                  where t.href = w.href
                    ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                    ${pathColumnExists ? 'AND t.path = w.path' : ''}
                )`);
          console.log(`  -> Inserted ${insertResults.rowCount} rows into ${config.writeTable} in ${elapsedTimeString(beforeInsert, 's', insertResults.rowCount)}`);
          return insertResults.rowCount + deleteResults.rowCount;
        }
      } catch (e) {
        console.log(e.stack);
        throw new Error('copySafeSyncTempTablesDataToWriteTable failed', e);
      }
      return 0;
    },
    /**
     * For safesync: find all hrefs that should be but aren't currently in the DB
     * (by comparing what's in table 'tempTableNameForSafeDeltaSync' against 'writeTable')
     * @param {*} transaction
     */
    findSafeSyncMissingHrefs: async function findSafeSyncMissingHrefs(transaction) {
      try {
        if (mssql) {
          const beforeQuery = Date.now();
          const request = await transaction.request();
          const query = `SELECT [href] FROM ${tempTableNameForSafeDeltaSync} s
            WHERE NOT EXISTS (
                SELECT 1
                FROM [dbo].[I_API_Test_Fre] i
                WHERE i.[href] = s.[href]
                  ${baseUrlColumnExists ? 'AND i.baseurl = s.baseurl' : ''}
                  ${pathColumnExists ? 'AND i.path = s.path' : ''}
               )
            `;
            // (${columnsForDeletes}) NOT IN (
            //   SELECT ${columnsForDeletes} FROM [${config.schema}].[${config.writeTable}]
            // )

          const results = await doQuery(transaction, query);
          console.log(`  -> Returned ${results.length} rows from ${config.writeTable} in ${elapsedTimeString(beforeQuery, 's', results.length)}`);
          return results.map(r => r.href);
        } if (pg) {
          const beforeQuery = Date.now();
          const query = `SELECT t.href 
            FROM ${tempTableNameForSafeDeltaSync} t 
              LEFT JOIN sri2db_test s ON s.href = t.href 
                ${baseUrlColumnExists ? 'AND s.baseurl = t.baseUrl' : ''}
                ${pathColumnExists ? 'AND s.path = t.path' : ''}
            WHERE s.href IS NULL
            `;
          const params = [];

          const results = await doQuery(transaction, query, params);
          console.log(`  -> Returned ${results.length} rows from ${config.writeTable} in ${elapsedTimeString(beforeQuery, 's', results.length)}`);
          return results.map(r => r.href);
        }
      } catch (e) {
        console.log(e.stack);
        throw new Error('findSafeSyncMissingHrefs failed', e);
      }
      return 0;
    },
    /**
     * Insert the rows to insert or update into a temp table
     * @param {*} apiResults array of api resources
     */
    saveSafeSyncMissingApiResultsToDb: async function saveSafeSyncMissingApiResultsToDb(apiResults, transaction = null) {
      const myTransaction = transaction || await openTransaction();

      try {
        // const beforeInsertForUpdate = Date.now();
        const nrOfInsertedRowsForUpdate = await doBulkInsert(
          myTransaction,
          apiResults, // .filter(ar => !ar.$$meta.deleted),
          false,
          tempTableNameForSafeDeltaSyncInserts,
        );
        // console.log(`  ${nrOfInsertedRowsForUpdate} rows inserted in ${elapsedTimeString(beforeInsertForUpdate, 'ms', nrOfInsertedRowsForUpdate, 's')}`);
        return nrOfInsertedRowsForUpdate;
      } catch (err) {
        console.error('Problem updating rows', err, err.stack);
        if (!transaction) db.rollbackTransaction(myTransaction);
        throw new Error('Problem updating rows', err);
      } finally {
        if (!transaction) await db.commitTransaction(myTransaction);
      }
    },
    /**
     * Insert the rows to insert or update into a temp table
     * @param {*} apiResults array of api resources
     */
    saveApiResultsToDb: async function saveApiResultsToDb(apiResults, transaction = null) {
      const myTransaction = transaction || await openTransaction();

      try {
        // const beforeInsertForUpdate = Date.now();
        const nrOfInsertedRowsForUpdate = await doBulkInsert(
          myTransaction,
          apiResults,
          false,
          tempTableNameForUpdates,
        );
        // console.log(`  ${nrOfInsertedRowsForUpdate} rows inserted in ${elapsedTimeString(beforeInsertForUpdate, 'ms', nrOfInsertedRowsForUpdate, 's')}`);
        return nrOfInsertedRowsForUpdate;
      } catch (err) {
        console.error('Problem updating rows', err, err.stack);
        if (!transaction) db.rollbackTransaction(myTransaction);
        throw new Error('Problem updating rows', err);
      } finally {
        if (!transaction) await db.commitTransaction(myTransaction);
      }
    },
    /**
     * Insert the hrefs of the resources that need to be deleted from the DB into a temp table
     * @param {*} hrefs array of strings (hrefs)
     */
    deleteApiResultsFromDb: async function deleteApiResultsFromDb(hrefs, transaction = null) {
      const myTransaction = transaction || await openTransaction();
      try {
        // const beforeInsertForDelete = Date.now();
        const nrOfInsertedRowsForDelete = await doBulkInsert(myTransaction, hrefs, true, tempTableNameForDeletes);
        // console.log(`  ${nrOfInsertedRowsForDelete} rows inserted in ${elapsedTimeString(beforeInsertForDelete, 'ms', nrOfInsertedRowsForDelete, 's')}`);
        return nrOfInsertedRowsForDelete;
      } catch (err) {
        console.error('Problem updating rows', err, err.stack);
        if (!transaction) db.rollbackTransaction(myTransaction);
        throw new Error('Problem updating rows', err);
      } finally {
        if (!transaction) await db.commitTransaction(myTransaction);
      }
    },

    /**
     * SafeDeltaSync takes all hrefs from a filtered list resource (like /persons?gender=FEMALE)
     * and then removes the ones not in the list from the snced DB, and fetches the newly added ones
     * from the API to add to the DB (basically: knowing if some record has been removed will be
     * figured out not by asking the API for deleted records, but by checking the entire list with
     * the DB. ALSO knowing if a record HAS BECOME PART of the list not because of a recent update
     * but because the LIST has changed due to changes in other resources).
     *
     * Insert the rows that the resource currently contains into a temp table
     * @param {*} hrefs array of strings (hrefs)
     */
    saveSafeSyncApiResultsToDb: async function saveSafeSyncApiResultsToDb(hrefs, transaction = null) {
      const myTransaction = transaction || await openTransaction();

      try {
        // const beforeInsertForDelete = Date.now();
        const nrOfInsertedRowsForSafeDeltaSync = await doBulkInsert(myTransaction, hrefs, true, tempTableNameForSafeDeltaSync);
        // console.log(`  ${nrOfInsertedRowsForDelete} rows inserted in ${elapsedTimeString(beforeInsertForDelete, 'ms', nrOfInsertedRowsForDelete, 's')}`);
        return nrOfInsertedRowsForSafeDeltaSync;
      } catch (err) {
        console.error('Problem updating rows', err, err.stack);
        if (!transaction) db.rollbackTransaction(myTransaction);
        throw new Error('Problem updating rows', err);
      } finally {
        if (!transaction) await db.commitTransaction(myTransaction);
      }
    },

    openTransaction,
    commitTransaction,
    rollbackTransaction,
  };

  return db;
};

/** **********************
 *   MAIN FACTORY        *
 *********************** */

/**
 * A new version trying to abstract the DB away (to be able to use postgres or mssql)
 * Also written in a more up-to-date way.
 * Also: just a factoryfunction that will return an object containing the right settings
 * instead of a class that you have to instantiate with the new keyword, and that has
 * issues like const f = myClassInstance.someFunction having trouble because when calling
 * f(), this won't be the same (because 'this' changes in that case)...
 *
 *
 * Also: ditch the lastSync table and just "select json.$$meta.modified from table order by
 * json.$$meta.modified DESC limit = 1"
 * (make sure that expression always has an index !!! Can we check this? If the query is
 *   slow we could at least generate a warning...)
 *
 * We have 2 factory functions now: one that creates a simple client, and one that creates
 * multiple clients in order to sync many API's at once with a simplified interface.
 */
function Sri2DbFactory(configObject = {}) {
  // make a deep clone so the initialization object cannot be tampered with anymore afterwards
  const config = clonedeep(configObject);

  if (!config.api) {
    throw new Error('[Sri2Db] invalid config object, config.api object missing');
  }
  if (!config.db) {
    throw new Error('[Sri2Db] invalid config object, config.db object missing');
  }

  if (!config.api.baseUrl) {
    throw new Error('api.baseUrl is not defined.');
  }

  const api = sriClientFactory({ ...config.api });

  const db = dbFactory({
    ...config.db,
    baseUrl: config.api.baseUrl,
    path: config.api.path,
  });


  /**
   * Gets a list from an sri api page-by-page (following the next links) and then
   * applies an asynchronous function to each batch received from the API.
   * The function will not work in parallel, so we wait to call the next one until
   * the current one has finished processing.
   *
   * @param {*} ayncFunctionToApply the function that will be applied to each 'page'
   *  we get from the API (following the next links). Its parameters will be
   *  - an array of api resource objects (or strings containing hrefs if expand=NONE)
   *  - isLastPage boolean indicating no more pages will follow
   *  - current page
   *  - nr of resources handled so far
   * @param {*} url
   * // @param {*} queryParams: am I going to support these?
   * @param {*} options sri-client options
   *  - wait: true (= default) means we won't fetch the next urls until ayncFunctionToApply has
   *    resolved, meaning the asyncFunctionToApply never runs in parallel
   *  - nextLinksBroken: true will auto-generate next page url with limit and offset instead of
   *    simply reading the next url from the response (default: false)
   */
  async function applyFunctionToList(asyncFunctionToApply, url, options = {}) {
    const limit = 500; // TODO get from url or some other setting
    let nextPath = url;
    let nextOffset = 0;
    const getListOptions = { ...options, raw: true };
    let nextJsonDataPromise = api.getRaw(url, {}, getListOptions); // api.getList(url, {}, getListOptions);
    let pageNum = 0;
    let count = 0;
    while (nextJsonDataPromise) {
      console.log(`Trying to get ${nextPath}`);
      // eslint-disable-next-line no-await-in-loop
      const jsonData = await nextJsonDataPromise;
      nextOffset += limit;
      if (jsonData.$$meta && jsonData.$$meta.next) {
        nextPath = options.nextLinksBroken
          ? `${url}&offset=${nextOffset}`
          : jsonData.$$meta.next;
      } else {
        nextPath = null;
      }
      // already start fetching the next url
      nextJsonDataPromise = nextPath ? api.getRaw(`${nextPath}`, {}, { ...options, raw: true }) : null;

      // apply async function to batch
      try {
        const jsonDataTranslated = translateApiResponseToArrayOfResources(jsonData);
        // eslint-disable-next-line no-await-in-loop
        await asyncFunctionToApply(jsonDataTranslated, nextJsonDataPromise === null, pageNum, count);
        count += jsonDataTranslated.length;
      } catch (e) {
        console.log('Error while trying to apply the given function to the current page', e.stack);
        throw e;
      }

      pageNum += 1;
    }
    return count;
  }

  async function getAllHrefs(hrefsToFetch, options = {}, batchPath = null) {
    if (hrefsToFetch.length === 0) return [];

    const keys = hrefsToFetch.map(h => h.substring(h.lastIndexOf('/') + 1));
    const basePath = hrefsToFetch[0].substring(0, hrefsToFetch[0].lastIndexOf('/'));

    /**
     *
     * @param {*} keys
     * @param {*} startOffset
     * @return { url: '<the url to fetch>', nextStartOffset: <offset to give to next call>, count: <nr of keys in url>}
     */
    function getNextPath(keys, startOffset, limit = 500) {
      let url = `${basePath}?limit=${limit}&keyIn=`;
      let i = startOffset;
      let count = 0;
      for (; url.length < 2048 && i < keys.length && count < limit; i++, count++) {
        const addComma = url.lastIndexOf('=') !== url.length - 1;
        url = url + (addComma ? ',' : '') + keys[i];
      }
      if (count === 0) {
        url = null;
      }
      return { nextPath: url, nextStartOffset: i, count };
    }

    let currentStartOffset = 0;
    let { nextPath, nextStartOffset, count } = getNextPath(keys, 0);
    const getListOptions = { ...options, raw: true };
    let nextJsonDataPromise = api.getList(nextPath, {}, getListOptions);
    let pageNum = 0;
    let totalCount = 0;
    const retVal = [];
    while (nextJsonDataPromise) {
      console.log(`[getAllHrefs] Trying to get ${count} hrefs starting from ${currentStartOffset}`);
      // eslint-disable-next-line no-await-in-loop
      const jsonData = await nextJsonDataPromise;
      const { nextPath: np, nextStartOffset: nso, count: c } = getNextPath(keys, nextStartOffset);
      currentStartOffset = nextStartOffset;
      nextPath = np;
      nextStartOffset = nso;
      count = c;

      // already start fetching the next url
      nextJsonDataPromise = nextPath ? api.getList(nextPath, {}, getListOptions) : null;

      totalCount += jsonData.length;
      pageNum += 1;
      retVal.push(...jsonData.map(r => r.$$expanded));
    }
    return retVal;
  }


  // Create the necessary functions
  const lastSyncDates = {};
  // const safetyWindow = 24 * 60 * 60 * 1000; // 24 hours
  /**
   * Gets the last known sync date for this process (fetch from DB - a safety window if none known or set)
   * @param {String} syncType can be FULL, DELTA, SAFEDELTA
   *   because we keep track of seperate last sync times per sync type
   * @param {*} rounded [OBSOLETE] = true will return the lower second (floor)
   *   if you need second instead of millisecond precision
   */
  const getLastSyncDates = async function getLastSyncDates(syncType = 'DELTA') {
    // console.log(`getLastSyncDate ${config.api.baseUrl}`);

    // store it in memory, but if it's not initialized, you might need
    // to do a single SELECT on the database
    lastSyncDates[syncType] = lastSyncDates[syncType]
      || await db.getLastSyncDates(syncType)
      || { lastModified: new Date('1900-01-01').getTime() /* .getTime() - safetyWindow */, syncStart: null };
    return lastSyncDates[syncType];

    // [OBSOLETE]
    // return rounded
    //   ? new Date(Math.floor((lastSyncDates[syncType].getTime() / 1000) * 1000))
    //   : lastSyncDates[syncType];
  };

  /**
   * Sets the last known sync date.
   *
   * @param {String} syncType can be FULL, DELTA, SAFEDELTA
   *   because we keep track of seperate last sync times per sync type
   * @param {Number} mostRecent$$MetaModifiedTimestamp the most recent $$meta.modified date in any
   *   of the synced resources (in API time)
   * @param {Number} mostRecentSyncStartTimestamp the most recent $$meta.modified date in any
   *   of the synced resources (in API time)
   */
  function setLastSyncTimestamps(syncType, mostRecent$$MetaModifiedTimestamp, mostRecentSyncStartTimestamp) {
    if (Number.isInteger(mostRecent$$MetaModifiedTimestamp)) {
      lastSyncDates[syncType] = {
        lastModified: mostRecent$$MetaModifiedTimestamp,
        syncStart: mostRecentSyncStartTimestamp,
      };
    } else {
      throw new Error('setLastSyncTimestamps expects an Integer parameter');
    }
  }


  let syncDonePromise = null;
  const isSyncRunning = function isSyncRunning() {
    return syncDonePromise && (syncDonePromise.settled === false || syncDonePromise.settled === undefined);
  };

  /**
   * The sync method is smart enough to only restart when the previous run has finished.
   * So they will never run in parallel. Calling sync twice, will just make the second promise
   * return later as it awaits until the first sync finishes before it starts.
   * Calling it three times, will return the SAME PROMISE as the second call!
   *
   * CRAP: I forgot to take into account that the function can be called with different parameters
   * (like fullsync, safedeltasync, deltasync)
   *
   * NEW STRATEGY: the sync method will simply reject if another sync is still running
   *
   * @param {*} modifiedSince if UNDEFINED it will use lastSyncDate and NULL will be equivalent to a full-sync?
   */
  // let queuedSyncPromise = null;
  // syncPromises = object where keys = paramsAsString and values = an array of promises (currentSync and queuedSync)
  // let syncPromises = {}
  const sync = async function sync(modifiedSince, safeDeltaSync = false) {
    // const paramsAsString = `${modifiedSince}|${safeDeltaSync}`
    // inner helper function that implements the actual sync
    async function innerSync() {
      // if we store this, better would be to use the DB's date:
      //  * MSSQL: select datediff_big(MILLISECOND, '1970-01-01', getutcdate())
      //  * POSTGRES: select (extract(epoch from now()) * 1000)::bigint as now;
      const beforeSync = Date.now();

      const isFullSync = modifiedSince === null;
      const isSafeDeltaSync = !isFullSync && safeDeltaSync;
      let syncTypeString = 'DELTA'; // unless it's something else
      if (isFullSync) syncTypeString = 'FULL';
      if (isSafeDeltaSync) syncTypeString = 'SAFEDELTA';

      console.log(`${isFullSync ? 'FULL' : 'DELTA'} sync ${config.api.baseUrl}${config.api.path}`);

      const previousSyncTimestamps = await getLastSyncDates(syncTypeString);
      let modifiedSinceString = null;
      if (modifiedSince === undefined) {
        modifiedSinceString = new Date(previousSyncTimestamps.lastModified)/* .subtract(10, 'seconds') */
          .toISOString();
      } else if (modifiedSince === null) {
        // => full sync
      } else {
        modifiedSinceString = new Date(modifiedSince).toISOString();
      }

      // const path = `${config.api.baseUrl}${config.api.path}${config.api.path.includes('?') ? '&' : '?'}$$meta.deleted=any&modifiedSince=${modifiedSinceString}`;
      const limit = config.api.limit || 500;
      // old version with &&meta.deleted=any
      // const firstPath = `${config.api.path}${config.api.path.includes('?') ? '&' : '?'}limit=${limit}&$$meta.deleted=any${modifiedSinceString ? `&modifiedSince=${modifiedSinceString}` : ''}`;

      const pathHasFilters = config.api.path.includes('?');
      const pathWithoutFilters = config.api.path.split('?')[0];
      const extraQueryParams = `limit=${limit}${modifiedSinceString ? `&modifiedSince=${modifiedSinceString}` : ''}`;
      const pathHasExpandFilter = pathHasFilters && config.api.path.includes('expand=');

      const updatedResourcesPath = `${config.api.path}${pathHasFilters ? '&' : '?'}${extraQueryParams}${pathHasExpandFilter ? '' : '&expand=FULL'}`;
      // Be careful: because of modifications to a resource, it might fall out of a filtered list
      // For example: after changing a person's gender, some person might disappear from
      // /persons?gender=FEMALE
      // So for deleted resources, we want to know about ALL deletions, not just the ones from
      // the configured filtered list, which is why we remove the url part after the first '?'
      const deletedResourcesPath = `${pathWithoutFilters}?${extraQueryParams}&$$meta.deleted=true&expand=NONE`;


      // in case of a SAFE sync (only relevant if pathHasFilters)
      // we also should remove 'Bo' from the table, if his/her gender was changed, and he/she now
      // doesn't belong to /persons?gender=FEMALE anymore
      // We will do this by getting only the keys that are part of the list, and then removing from
      // the db any record not in that list.
      // We should also figure out this way the ones that we didn't have in the DB yet, then fetch
      // and insert them
      const filteredNonExpandedPath = setExpandOnPath(`${config.api.path}${pathHasFilters ? '&' : '?'}limit=*`, 'NONE');


      let totalCount = 0;
      let lastModified = '';

      const dbTransaction = await db.openTransaction();
      try {
        if (!(await db.checkIfTableExists(dbTransaction))) {
          console.log(`WARNING: it seems like the table ${config.db.table} doesn't exist`);
          console.log(`Please run the following queries inside psql in order to create it.`);
          console.log(`
          CREATE TABLE ${config.db.schema}.${config.db.table} (href varchar, jsondata jsonb, modified timestamptz, key varchar, path varchar, baseurl varchar);
          CREATE UNIQUE INDEX ${config.db.table}_baseurl_path_href_idx ON ${config.db.schema}.${config.db.table} (baseurl, path, href);`);
        }

        // only needs to be awaited when we want to actually store stuff in the DB,
        // so we can start doing API requests before it is settled!
        const tempTablesInitializededPromise = db.createTempTables(dbTransaction);

        const beforeApiCalls = Date.now();

        // first delete (real deletions)
        if (!isFullSync && !isSafeDeltaSync && !config.api.deletedNotImplemented) {
          await applyFunctionToList(async (hrefs, isLastPage, pageNum, count) => {
            console.log(`[page ${pageNum}] Trying to store ${hrefs.length} records on the DB for deletion (${count} done so far)`);
            await tempTablesInitializededPromise; // make sure temp tables have been created by now
            await db.deleteApiResultsFromDb(hrefs, dbTransaction);
            // lastModified = resources.reduce(
            //   (acc, cur) => (cur.$$meta.modified > acc ? cur.$$meta.modified : acc),
            //   lastModified,
            // );
            // console.log('  Most recent lastmodified seen so far:', lastModified);
            console.log(`Synced ${count + hrefs.length} api resources so far in ${elapsedTimeString(beforeSync, 'm', count, 's')}.`);

            if (isLastPage) totalCount += count + hrefs.length;
          },
          deletedResourcesPath, { nextLinksBroken: config.api.nextLinksBroken });
        }

        // then insert or update
        await applyFunctionToList(async (resources, isLastPage, pageNum, count) => {
          console.log(`[page ${pageNum}] Trying to store ${resources.length} records on the DB for update/insert (${count} done so far)`);

          if (resources.some(r => typeof r === 'string')) {
            // THIS IS THE expand=NONE case where we only store an href
            // const resourcesToStore = resources.map(r => ({ $$meta: { permalink: r } }));
            await tempTablesInitializededPromise; // make sure temp tables have been created by now
            await db.saveApiResultsToDb(resources, dbTransaction);
          } else {
            // some old API's are missing $$meta.modified or even key
            const resourcesToStore = resources.map(r => fixResourceForStoring(r));
            await tempTablesInitializededPromise; // make sure temp tables have been created by now
            await db.saveApiResultsToDb(resourcesToStore, dbTransaction);
            lastModified = resourcesToStore.reduce(
              (acc, cur) => (cur.$$meta.modified > acc ? cur.$$meta.modified : acc),
              lastModified,
            );
          }
          // console.log('  Most recent lastmodified seen so far:', lastModified);
          console.log(`Synced ${count + resources.length} api resources so far in ${elapsedTimeString(beforeSync, 'm', count, 's')}.`);

          if (isLastPage) totalCount += count + resources.length;
        },
        updatedResourcesPath, { nextLinksBroken: config.api.nextLinksBroken });

        // UPDATE STUFF IN THE DB
        await tempTablesInitializededPromise; // make sure temp tables have been created by now
        await db.copyTempTablesDataToWriteTable(dbTransaction, isFullSync);

        // for the SAFE delta sync, there is some extra work left
        if (isSafeDeltaSync) {
          // then insert or update
          await applyFunctionToList(async (resources, isLastPage, pageNum, count) => {
            console.log(`[page ${pageNum}] Trying to store ${resources.length} records on the DB for safe delta sync (${count} done so far)`);
            await db.saveSafeSyncApiResultsToDb(resources, dbTransaction);
            console.log(`Synced ${count + resources.length} api hrefs so far in ${elapsedTimeString(beforeSync, 'm', count, 's')}.`);
          },
          filteredNonExpandedPath, { nextLinksBroken: config.api.nextLinksBroken });

          console.log('Trying to find missing hrefs in the DB that need to be fetched.');
          // fetch all records from the API that have become a part of the list recently,
          // but not because of a recent update of the resource itself (those would have been
          // found already with modifiedSince)
          const hrefsToFetch = await db.findSafeSyncMissingHrefs(dbTransaction)
          if (hrefsToFetch.length > 0) {
            console.log(`Trying to fetch ${hrefsToFetch.length} resources from API`);
            const beforeFetch = Date.now();
            const resourcesToStore = (await getAllHrefs(hrefsToFetch, {}, config.api.batchPath))
              .map(r => fixResourceForStoring(r));

            console.log(`Fetched ${resourcesToStore.length} resources from API in ${elapsedTimeString(beforeFetch, 'm', resourcesToStore.length, 's')}.`);
            totalCount += await db.saveSafeSyncMissingApiResultsToDb(
              resourcesToStore.map(r => (r.$$expanded || r)),
              dbTransaction,
            );
          } else {
            console.log(`No resources to fetch from API (${hrefsToFetch.length} missing hrefs)`);
          }
          // after filling the DB with the necessary info also copy the stuff in the temp
          // tables to the actual tables
          // = delete records from DB that are NOT IN THE LIST ANYMORE & insert MISSING records
          await db.copySafeSyncTempTablesDataToWriteTable(dbTransaction);
        }

        if (config.dryRun) {
          console.log('Not committing transaction because dryRun');
          db.rollbackTransaction(dbTransaction);
        } else {
          await db.commitTransaction(dbTransaction);
        }

        // only update internal lastSyncDate if method was called without specific modifiedSince
        if (!modifiedSince) {
          const syncDuration = Date.now() - beforeApiCalls;
          // * potentially we have seen results that were too recent (popping up while we were
          //   syncing), so subtract the duration of the sync * 1.01 (1.01 to overcompensate
          //   for any clock differences between our machine and the server) from the most recent
          //   'modified' date found in the list of results
          // * ALSO look at the delta between the start of the current and the previous sync,
          //   because we can safely add this delta to the previous modifiedSince (* 0.99 to
          //   overcompensate for any clock differences between our machine and the server)
          // => We'll take the highest value of both of these
          const timeBetweenThisAndPreviousSync = previousSyncTimestamps.syncStart ? (beforeSync - previousSyncTimestamps.syncStart) : 0;
          let newLastModifiedTimestamp = null;
          if (lastModified && lastModified.length > 0) {
            const prevLastModified = previousSyncTimestamps.lastModified;
            newLastModifiedTimestamp = Math.max(
              Date.parse(lastModified) - Math.round(syncDuration * 1.01),
              prevLastModified - Math.round(syncDuration * 1.01)
                + Math.round(timeBetweenThisAndPreviousSync * 0.99),
            );
            console.log(`Updating ${syncTypeString} LAST SYNC DATE from ${new Date(prevLastModified).toISOString()} to ${new Date(newLastModifiedTimestamp).toISOString()}`);
          } else {
            // no records found? no need to increase modifiedSince next time
            newLastModifiedTimestamp = previousSyncTimestamps.lastModified;
          }
          setLastSyncTimestamps(syncTypeString, newLastModifiedTimestamp, beforeSync);
          await db.setLastSyncTimestamps(syncTypeString, newLastModifiedTimestamp, beforeSync);

          console.log(`==== New ${syncTypeString} LAST SYNC DATES = { lastModified: ${new Date(newLastModifiedTimestamp).toISOString()}, syncStart: ${new Date(beforeSync).toISOString()} }`);
        }
        console.log(`Synced ${totalCount} api resources in ${elapsedTimeString(beforeSync, 'm', totalCount, 's')}.`);

        // elapsedTimeCalculations(beforeSync, 'm', totalCount, 's');
        return { ...elapsedTimeCalculations(beforeSync, 'm', totalCount, 's'), config: clonedeep(config) };
      } catch (e) {
        console.warn(`Problem while doing ${isFullSync ? 'FULL' : 'DELTA'} sync`, e, e.stack);
        if (e instanceof SriClientError) {
          console.warn(JSON.stringify(e, null, 2));
        }
        db.rollbackTransaction(dbTransaction);
        throw e;
      }
    }

    // another inner helper function to handle the promises when multiple syncs are being queued
    // if other syncs are still working return a new Promise that awaits all existing promises
    // async function smartSync() {
    //   // if another sync is already QUEUED, just return the same promise
    //   if (queuedSyncPromise) {
    //     console.log('(Previous sync still running, AND another one waiting in the queue already, returning that same Promise of the already waiting one, so I won\'t actually start yet another sync)');
    //     return queuedSyncPromise;
    //   }

    //   queuedSyncPromise = new Promise(async (resolve, reject) => {
    //     try {
    //       // if another sync is already running, wait for it to end, and only then start a new one
    //       if (syncDonePromise && !syncDonePromise.settled) {
    //         console.log('(Previous sync still running, waiting for it to finish...)');
    //         const beforePreviousSyncDone = Date.now();
    //         await syncDonePromise;
    //         console.log(`(Previous sync finished after ${elapsedTimeString(beforePreviousSyncDone, 'ms')})`);
    //       }
    //       const result = await innerSync();
    //       resolve(result);
    //     } catch (e) {
    //       reject(e);
    //     } finally {
    //       queuedSyncPromise = null;
    //     }
    //   });
    //   return queuedSyncPromise;
    // }

    // TODO: make sure any new call will return a Promise that awaits all other running syncs before
    //   starting, and will return the same Promise if another one with the same params is already
    //   scheduled in the future
    // const syncDonePromise = syncPromises[paramsAsString] ? syncPromises[paramsAsString][0] : null;
    // if (syncDonePromise && !syncDonePromise.settled) {
    //   syncPromises[paramsAsString][0] = smartSync();
    // } else {
    //   syncPromises[paramsAsString][0] = innerSync();
    // }
    if (isSyncRunning()) {
      return Promise.reject('Another sync is still running.');
    }

    syncDonePromise = innerSync(modifiedSince, safeDeltaSync).then((result) => {
      syncDonePromise.settled = true;
      return result;
    }).catch((e) => {
      syncDonePromise.settled = true;
      throw e;
    });
    return syncDonePromise;
  };


  /**
   *
   * @param {*} modifiedSince if UNDEFINED it will use lastSyncDate and NULL will be equivalent to a full-sync?
   */
  const deltaSync = async function deltaSync(modifiedSince) {
    return sync(modifiedSince);
  };

  /**
   *
   * @param {*} modifiedSince if UNDEFINED it will use lastSyncDate and NULL will be equivalent to a full-sync?
   */
  const safeDeltaSync = async function safeDeltaSync(modifiedSince) {
    return sync(modifiedSince, true);
  };

  /**
   * do a full sync getting all resources from the API and storing them
   * this means emptying the existing DB first so we don't keep any deleted records lying around !
   */
  const fullSync = async function fullSync() {
    return sync(null);
  };


  let socket = null;
  let retryConnectInterval;
  let retryBroadcastTriggeredSyncInterval;

  const uninstallBroadCastListeners = function uninstallBroadCastListeners() {
    if (socket) {
      socket.close();
      socket = null;
      clearInterval(retryConnectInterval);
    }
  };

  /**
   * Utility function, should/can only set up a websocket if the config tells you
   * what the broadcastUrl is
   */
  const installBroadCastListeners = function installBroadCastListeners(doSafeDeltaSync = false) {
    if (!config.broadcastUrl) {
      return null;
    }

    if (!socket || socket.disconnected) {
      socket = io.connect(config.broadcastUrl);

      retryConnectInterval = setInterval(() => {
        if (!socket || socket.disconnected) {
          console.log('Socket not connected, retry to setup the connection', config.broadcastUrl, config.api.path);
          uninstallBroadCastListeners();
          installBroadCastListeners();
        }
      }, 5000);

      socket.on('connect', async () => {
        console.log('CONNECTED to audit/broadcast, listening for updates...');

        // stop trying to connect
        clearInterval(retryConnectInterval);

        socket.emit('join', config.api.path.split('?')[0]);
      });

      socket.on('disconnect', () => {
        console.log('DISCONNECTED from audit/broadcast, trying to reconnect');

        // simple version reconnects when signalled the connection isgone
        uninstallBroadCastListeners();
        installBroadCastListeners(safeDeltaSync);

        // ALTERNATIVE: retry strategy to set up the connection again?
        // retryConnectInterval = setInterval(10000, () => {
        //   if (!socket.isSocketConnected()) {
        //     installBroadCastListeners();
        //   }
        // });
      });

      socket.on('update', async (data) => {
        console.log(`--- Audit/broadcast sent us a new message: ${util.inspect(data)}, requesting new delta sync.`);

        const syncMethod = doSafeDeltaSync ? safeDeltaSync : deltaSync;
        try {
          await syncMethod();
        } catch (e) {
          if (!retryBroadcastTriggeredSyncInterval) {
            retryBroadcastTriggeredSyncInterval = setInterval(
              async () => {
                try {
                  await syncMethod();
                  clearInterval(retryBroadcastTriggeredSyncInterval);
                } catch (e2) {
                  console.error('Sync triggered by broadcast failed because:', e2);
                }
              },
              5000,
            );
          }
        }
      });
    }

    return socket;
  };


  const isSocketConnected = () => socket != null;

  const close = function close() {
    uninstallBroadCastListeners();
    // close db connections etc?
  };


  // for the configuredSync, if missing, full sync will be default
  const methodNameToMethodMap = {
    deltaSync,
    safeDeltaSync,
    fullSync,
  };
  const configuredSync = methodNameToMethodMap[config.syncMethod || 'fullSync'];
  if (!configuredSync) throw `Sync type '${client.config.syncType}' doesn't map to a method`;


  // Now create and return the object containing the necessary functions for the user to use
  return {
    getLastSyncDates,
    fullSync,
    deltaSync,
    safeDeltaSync,
    configuredSync,
    isSyncRunning,
    installBroadCastListeners,
    uninstallBroadCastListeners,
    isSocketConnected,
    close,
    config,
  };
}

/**
 * This helps if you need to sync multiple API's, because it has a base-config object,
 * and an array of other objects that overwrite (or fill in missing) some properties of
 * the base config.
 * That way you only have to provide most of the details once, and all api's will be
 * synced as soon as you call sync.
 * You can also control the level of concurrency between multiple syncs, allowing you
 * to run all syncs one by one or multiple in parallel.
 * @param {*} config STRUCTURE = {
 *    baseConfig: {...},
 *    concurrency: (>=1),
 *    overwrites: [ {
 *      (sparse sri2db config containing only the properties that differ
 *      from the sharedConfig)
 *    }, ... ]
 * }
 * @return an instance of the client that exposes the methods you can call
 *    the most important is sync(), that returns a promise containing an array of all results
 *    returned from the individual syncs. In case a sync fails, the result will contain the
 *    error, the value of the resolved promise otherwise.
 */
function Sri2DbMultiFactory(configObject = {}) {
  // make a deep clone so the initialization object cannot be tampered with anymore afterwards
  const config = clonedeep(configObject);
  if (!config.concurrency) {
    config.concurrency = 1;
  } else if (!Number.isInteger(config.concurrency) || config.concurrency <= 0) {
    throw new Error('Concurrency must be a postive integer.');
  }

  const sri2dbConfigs = config.overwrites.map(ow => jsonmergepatch.apply(clonedeep(config.baseConfig), ow));
  const sri2dbClients = sri2dbConfigs.map(c => Sri2DbFactory(c));

  async function sleep(timeout) {
    return new Promise((resolve, reject) => setTimeout(() => resolve(true), timeout));
  }

  // you'd want to hand this function's output to pAll
  function allMethods(methodName) {
    const tasks = sri2dbClients.map(
      c =>
        // console.log('Function generator for', c.config.api.path);
        async () => {
          console.log('[Sri2DMulti] Starting', methodName, 'for', c.config.api.path);
          // await sleep(2000);
          return ((await pSettle([c[methodName]()]))[0]);
        }
      ,
    );
    return tasks;
  }

  async function runAllByName(methodName) {
    const tasks = allMethods(methodName);
    return pAll(tasks, { concurrency: config.concurrency });
  }

  const retVal = {
    sri2dbList: sri2dbClients,
  };

  Object.keys(sri2dbClients.length > 0 ? sri2dbClients[0] : {})
    .filter(k => sri2dbClients[0][k] instanceof Function)
    .forEach(k => retVal[k] = (async () => runAllByName(k)));

  return retVal;
  // {
  //   configuredSync: (async () => syncByName('configuredSync')),
  //   deltaSync: (async () => syncByName('deltaSync')),
  //   safeDeltaSync: (async () => syncByName('safeDeltaSync')),
  //   fullSync: (async () => syncByName('fullSync')),
  //   sri2dbList: sri2dbClients,
  // };
}


// export module.exports;
module.exports = {
  Sri2Db: Sri2DbFactory,
  Sri2DbMulti: Sri2DbMultiFactory,
};
