/**
* Created by pablo in 2015
* Modified by johannes in 2018-2019
* Mostly rewritten by fre in 2019
*/

// const request = require('requestretry');
const moment = require('moment');
const io = require('socket.io-client');
const sriClientFactory = require('@kathondvla/sri-client/node-sri-client');

/** **********************
 *   HELPER FUNCTIONS    *
 *********************** */

const removeDollarFields = (obj) => {
  Object.keys(obj).forEach((property) => {
    if (property.startsWith('$$') && property != '$$meta') {
      delete obj[property];
    } else if (obj.property !== null && typeof obj.property === 'object') {
      removeDollarFields(obj[property]);
    }
  });
  return obj;
};


/**
 * @param {Number} nrOfMilliseconds
 * @param {String} unit can be ms, s, m, h, d
 */
const msToOtherUnit = (milliseconds, unit) => {
  let elapsedInUnit;
  switch (unit) {
    case 'ms': elapsedInUnit = milliseconds; break;
    case 's': elapsedInUnit = milliseconds / 1000; break;
    case 'm': elapsedInUnit = milliseconds / 1000 / 60; break;
    case 'h': elapsedInUnit = milliseconds / 1000 / 60 / 60; break;
    case 'd': elapsedInUnit = milliseconds / 1000 / 60 / 60 / 24; break;
    default: elapsedInUnit = milliseconds; break;
  }
  return elapsedInUnit;
};

/**
 *
 * @param {Date} startDate
 * @param {String} unit can be ms, s, m, h, d
 * @param {Number} amount [optional] if present will add avg per second/minute/hour
 * @param {String} avgUnit [optional] if not set, same unit as above, but you can make it avg per m, h, d, ... if you want
 */
const elapsedTimeString = (startDate, unit = 'ms', amount, avgUnit) => {
  // eslint-disable-next-line no-param-reassign
  if (!avgUnit) avgUnit = unit;
  const elapsedMilliseconds = (Date.now() - startDate);
  const elapsedInUnit = msToOtherUnit(elapsedMilliseconds, unit);
  const elapsedInAvgUnit = msToOtherUnit(elapsedMilliseconds, avgUnit);
  const avgPerSecondPart = amount ? ` (${Math.round(amount / elapsedInAvgUnit)}/${avgUnit})` : '';
  return `${Math.round(elapsedInUnit * 100) / 100}${unit}${avgPerSecondPart}`;
};


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
const dbFactory = function dbFactory(config) {
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

  const tempTablePrefix = Math.random().toString(26).substring(5);
  const tempTableNameForUpdates = `${mssql ? '##' : ''}sri2db_${tempTablePrefix}_updates`;
  const tempTableNameForDeletes = `${mssql ? '##' : ''}sri2db_${tempTablePrefix}_deletes`;

  // first we need to know which columns exist in order to execute the right queries
  let initialized = false;
  let baseUrlColumnExists = null;
  let pathColumnExists = null;
  let resourceTypeColumnExists = null;


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
   * @param {*} query the query string containing '@paramName' parts for the named params
   * @param {*} params if mssql: an array of { name: '', type: mssql.VarChar, value: ... }
   *   for postgres the type is not needed
   */
  async function doQuery(transaction, queryString, params = []) {
    if (mssql) {
      try {
        const request = transaction.request();
        params.forEach(p => request.input(p.name, p.type, p.value));
        const result = await request.query(queryString);

        return result.recordset;
      } catch (e) {
        console.error('Error in doQuery', e);
        throw e;
      }
    } if (pg) {
      try {
        const pgParams = {};
        params.forEach(p => pgParams[p.name] = p.value);

        const result = await transaction.any(queryString, pgParams);
        return result;
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
  async function doBulkInsert(dbTransaction, records, forDeletion = false, tableName = config.writeTable) {
    if (records && records.length === 0) {
      return 0;
    }

    const beforeInsert = Date.now();

    let retVal = 0;
    if (mssql) {
      const mssqlTableName = `[${tableName}]`;
      try {
        const table = new mssql.Table(mssqlTableName);
        table.create = false; // don't try to create the table
        table.columns.add('href', mssql.VarChar(1024), { nullable: false, length: 1024 });
        if (baseUrlColumnExists) table.columns.add('baseurl', mssql.VarChar(100), { nullable: false, length: 100 });
        if (pathColumnExists) table.columns.add('path', mssql.VarChar(100), { nullable: false, length: 100 });

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
          records.forEach((r) => {
            const addParams = [r.$$meta.permalink];
            if (baseUrlColumnExists) addParams.push(config.baseUrl);
            if (pathColumnExists) addParams.push(config.path);

            addParams.push(r.key);
            addParams.push(new Date(r.$$meta.modified));
            addParams.push(JSON.stringify(removeDollarFields(r)));
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

      // our set of columns, to be created only once, and then shared/reused,
      // to let it cache up its formatting templates for high performance:
      const columsArray = forDeletion ? ['href'] : ['href', 'key', 'modified', 'jsondata'];
      if (baseUrlColumnExists) columsArray.push('baseurl');
      if (pathColumnExists) columsArray.push('path');

      if (!forDeletion) {
        if (resourceTypeColumnExists) columsArray.push('resourcetype');
      }

      const columnSet = new pg.helpers.ColumnSet(columsArray, { table: pgTableName });
      // TODO define columSet outside of the function so it will be reused !!!

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
          const value = {
            href: r.$$meta.permalink,
            key: r.key,
            modified: new Date(r.$$meta.modified),
            jsondata: JSON.stringify(removeDollarFields(r)),
          };
          if (resourceTypeColumnExists) value.resourcetype = r.$$meta.type;
          if (baseUrlColumnExists) value.baseurl = config.baseUrl;
          if (pathColumnExists) value.path = config.path;

          return value;
        }
      });

      // generating a multi-row insert query:
      const query = pg.helpers.insert(values, columnSet);

      // executing the query:
      const result = await dbTransaction.result(query);
      retVal = result.rowCount;
    }

    console.log(`  Inserted ${retVal} rows (${forDeletion ? 'hrefs only' : 'href, resourceType, key, modified, jsonData'}) in ${elapsedTimeString(beforeInsert, 'ms', retVal, 's')}`);
    return retVal;
  }


  async function getTableColumns(dbTransaction, tableName) {
    if (mssql) {
      return doQuery(dbTransaction, `
        select schema_name(tab.schema_id) as schema_name,
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
      ]);
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
   * Get connection from pool and open a transaction on it
   */
  let poolOrConnection = null;
  async function openTransaction() {
    if (poolOrConnection) {
      // do nothing but return the existing pool
    } else if (mssql) {
      const mssqlconfig = Object.assign(
        {
          server: config.host,
          database: config.database,
          user: config.username,
          password: config.password,
        },
        {
          pool: {
            max: 10,
            min: 0,
            idleTimeoutMillis: 30000,
            connectionTimeout: 60 * 60 * 1000, // 1 hour
          },
          requestTimeout: 30 * 60 * 1000, // 30 minutes
        },
      );

      poolOrConnection = new mssql.ConnectionPool(mssqlconfig);
      poolOrConnection.on('error', (err) => {
        console.error('connection pool error', err);
      });

      await poolOrConnection.connect();
    } else if (pg) {
      const pgconfig = Object.assign(config,
        {
          user: config.username,
          // pool: {
          max: 10,
          min: 0,
          idleTimeoutMillis: 30000,
          connectionTimeout: 60 * 60 * 1000, // 1 hour
          // },
          capSQL: true, // capitalize all generated SQL
        });

      const db = pg(pgconfig);

      poolOrConnection = await db.connect();
    }

    let transaction = null;
    if (mssql) {
      transaction = poolOrConnection.transaction();
      await transaction.begin();
    } else if (pg) {
      await poolOrConnection.none('BEGIN');
      transaction = poolOrConnection;
    }

    if (!initialized) {
      const tableColumns = await getTableColumns(poolOrConnection, config.writeTable);
      console.log('TableColumns', tableColumns.map(c => c.column_name).join());
      resourceTypeColumnExists = !!tableColumns.find(e => e.column_name.toLowerCase() === 'resourcetype');
      baseUrlColumnExists = !!tableColumns.find(e => e.column_name.toLowerCase() === 'baseurl');
      pathColumnExists = !!tableColumns.find(e => e.column_name.toLowerCase() === 'path');
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
      return transaction.none('COMMIT');
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
      return transaction.none('ROLLBACK');
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
    getLastSyncDate: async function getLastSyncDate(apiPath) {
      try {
        console.log(`    getLastSyncDate from ${config.readTable} for ${apiPath}`);
        const transaction = await openTransaction();

        // TODO take the API into account (where 'path' = apiPath) !!!
        // (so we need an expression index on 'cut-path-from-href' if we don't want to have yet another column
        // + this is safer than using resourceType and also allows us not to have to configure the resourceType (the API will tell us in $$meta)
        // Thinking about it: this should store the resourceListPath, because that is the one returning
        // the results and that is the one the modifiedSince applies to.
        // For example if we request /organisationalunits to the old VOS api we'll get a list returning
        // /schools/<guid>, /clbs/<guid>, but not a single /organisationalunits/<guid>, so it
        // would only work properly if we store which LIST returned this result (and multiple lists could
        // potentially return the same result)
        // this way, we could be syncing multiple list urls (incl. query params) into the same table
        // (and in some cases store multiple copies of the same resource). For example: /persons?gender=F
        // and /persons?birthDateBefore=2001-09-01
        // I am not saying that this is necessarily a good idea (non-unique indexes could impact performance),
        // but it would work.
        if (mssql) {
          const result = await doQuery(transaction, `
            select [modified]
            from [${config.schema}].[${config.readTable}]
            where 1=1
              ${baseUrlColumnExists ? `AND baseurl = '${config.baseUrl}'` : ''}
              ${pathColumnExists ? `AND path = '${apiPath}'` : ''}
            order by [modified] DESC
            OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY`); // where order by d DESC limit 1
          return result.length > 0 ? result[0].modified : null;
        }
        if (pg) {
          const result = await transaction.any(`
            select modified
            from ${config.schema}.${config.readTable}
            where 1=1
              ${baseUrlColumnExists ? `AND baseurl = '${config.baseUrl}'` : ''}
              ${pathColumnExists ? `AND path = '${apiPath}'` : ''}
            order by modified DESC
            LIMIT 1`);
          return result.length > 0 ? result[0].modified : null;
        }
        return null;
      } catch (e) {
        throw new Error('Something went wrong while trying to query the DB', e);
      }
    },
    /**
     *
     * @param {*} transaction
     */
    createTempTables: async function createTempTables(transaction) {
      const columnsForDeletes = `href${baseUrlColumnExists ? ', baseurl' : ''}${pathColumnExists ? ', path' : ''}`;
      try {
        if (mssql) {
          const makeCreateTempTableString = (tblName, forDeletes) => `
              IF OBJECT_ID(N'tempdb..${tblName}') IS NULL
              BEGIN
                SELECT TOP 0 ${forDeletes ? columnsForDeletes : '*'}
                INTO [${tblName}]
                FROM [${config.writeTable}];
              END;
              TRUNCATE TABLE [${tblName}];`;

          // TEST
          // const testResults = await (await transaction.request())
          //   .query(`SELECT OBJECT_ID(N'tempdb..${tempTableNameForUpdates}') as tableId`);
          // console.log('testResults', testResults.recordset[0]);

          const beforeCreateUpdatesTable = new Date();
          const createRequest1 = await transaction.request();
          const createResults1 = await createRequest1.query(
            makeCreateTempTableString(tempTableNameForUpdates, false),
          );
          console.log(`  Created temporary table for updated rows in ${elapsedTimeString(beforeCreateUpdatesTable, 'ms')}`);

          const beforeCreateDeletesTable = new Date();
          const createRequest2 = await transaction.request();
          const createResults2 = await createRequest1.query(
            makeCreateTempTableString(tempTableNameForDeletes, true),
          );
          console.log(`  Created temporary table for deleted rows in ${elapsedTimeString(beforeCreateDeletesTable, 'ms')}`);
        } else if (pg) {
          // const sql = 'CREATE TEMPORARY TABLE $tableName:name (resourceType VarChar(100) NOT NULL, [key] VarChar(100) NOT NULL, href VarChar(1024) NOT NULL, modified DateTime NOT NULL, jsonData BSON NULL)';
          const makeCreateTempTableString = (tblName, forDeletes) => `
            CREATE GLOBAL TEMPORARY TABLE IF NOT EXISTS ${tblName}
            AS SELECT ${forDeletes ? columnsForDeletes : '*'} FROM ${config.schema}.${config.writeTable};
            TRUNCATE ${tblName}`;

          const beforeCreateUpdatesTable = new Date();
          await transaction.none(
            makeCreateTempTableString(tempTableNameForUpdates, false),
          );
          console.log(`  Created temporary table for updated rows in ${elapsedTimeString(beforeCreateUpdatesTable, 'ms')}`);

          const beforeCreateDeletesTable = new Date();
          transaction.none(
            makeCreateTempTableString(tempTableNameForDeletes, true),
          );
          console.log(`  Created temporary table for deleted rows in ${elapsedTimeString(beforeCreateDeletesTable, 'ms')}`);
        }
      } catch (e) {
        console.log('Creating temp tables failed', e, e.stack);
        throw new Error('Creating temp tables failed', e);
      }
    },
    copyTempTablesDataToWriteTable: async function copyTempTablesDataToWriteTable(transaction, fullSync = false) {
      try {
        if (mssql) {
          const beforeDelete = Date.now();
          const deleteRequest = await transaction.request();
          // const deleteResults = await deleteRequest.query(`DELETE FROM [${config.schema}].[${config.writeTable}]
          //   WHERE EXISTS (select 1 from [${tempTableNameForDeletes}] AS t where t.[key] = [${config.schema}].[${config.writeTable}].[key] AND t.resourceType = [${config.schema}].[${config.writeTable}].resourceType)`);

          // TODO fullSync delete will do nothing if API contains no records (because then the updatestemptable will have no records, so we can't find the resourceType)
          const deleteQuery = fullSync
            ? `DELETE w
              FROM [${config.schema}].[${config.writeTable}] w
              WHERE 1=1
                ${baseUrlColumnExists ? `AND baseurl IN (SELECT DISTINCT baseurl FROM [${tempTableNameForUpdates}])` : ''}
                ${pathColumnExists ? `AND path  IN (SELECT DISTINCT path FROM [${tempTableNameForUpdates}])` : ''}
              `
            : `DELETE w FROM [${config.schema}].[${config.writeTable}] w
              INNER JOIN [${tempTableNameForDeletes}] t
                ON t.[href] = w.[href]
                  ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND t.[path] = w.[path]' : ''}
            `;

          const deleteResults = await deleteRequest.query(deleteQuery);
          console.log(`  -> Deleted ${deleteResults.rowsAffected[0]} rows from ${config.writeTable} in ${elapsedTimeString(beforeDelete, 's', deleteResults.rowsAffected[0])}`);

          const beforeUpdate = Date.now();
          const updateRequest = await transaction.request();
          const updateResults = await updateRequest.query(`UPDATE w
            SET w.href = t.href, w.modified = t.modified, w.jsonData = t.jsonData
            FROM [${config.schema}].[${config.writeTable}] w
            INNER JOIN [${tempTableNameForUpdates}] t
              ON t.[href] = w.[href]
                ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                ${pathColumnExists ? 'AND t.path = w.path' : ''}
          `);
          console.log(`  -> Updated ${updateResults.rowsAffected[0]} rows from ${config.writeTable} in ${elapsedTimeString(beforeUpdate, 's', updateResults.rowsAffected[0])}`);

          const beforeInsert = Date.now();
          const insertRequest = await transaction.request();

          // some records can appear multiple times if the result set changes while we are fetching
          // the pages, so try to remove doubles before inserting (take the one with most recent
          // modified)
          const insertResults = await insertRequest.query(`
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
            FROM (select *,
                  ROW_NUMBER() over (partition by
                        ${baseUrlColumnExists ? 'baseurl,' : ''}
                        ${pathColumnExists ? 'path,' : ''}
                        href
                      ORDER BY modified DESC) as rowNumber
                  from [${tempTableNameForUpdates}]) t
            WHERE t.rowNumber = 1
              AND NOT EXISTS (
                select 1 from [${config.schema}].[${config.writeTable}] w
                where t.[href] = w.[href]
                  ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND t.path = w.path' : ''}
                )`);
          console.log(`  -> Inserted ${insertResults.rowsAffected[0]} rows into ${config.writeTable} in ${elapsedTimeString(beforeInsert, 's', insertResults.rowsAffected[0])}`);
        } else if (pg) {
          const beforeDelete = Date.now();
          // TODO fullSync delete will do nothing if API contains no records (because then the updatestemptable will have no records, so we can't find the resourceType)
          // better solution would be to check for conf.api.baseUrl (cfr. getLastSyncDate)
          const deleteQuery = fullSync
            ? `DELETE FROM ${config.schema}.${config.writeTable} w
              WHERE 1=1
                ${baseUrlColumnExists ? `AND baseurl IN (SELECT DISTINCT baseurl FROM ${tempTableNameForUpdates})` : ''}
                ${pathColumnExists ? `AND path  IN (SELECT DISTINCT path FROM ${tempTableNameForUpdates})` : ''}
            `
            : `DELETE FROM ${config.schema}.${config.writeTable} w
                USING ${tempTableNameForDeletes} t
                WHERE t.href = w.href
                  ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                  ${pathColumnExists ? 'AND t.path = w.path' : ''}
            `;

          const deleteResults = await transaction.result(deleteQuery);
          console.log(`  -> Deleted ${deleteResults.rowCount} rows from ${config.writeTable} in ${elapsedTimeString(beforeDelete, 's', deleteResults.rowsCount)}`);

          const beforeUpdate = Date.now();
          const w = `${config.schema}.${config.writeTable}`;
          const updateResults = await transaction.result(`UPDATE ${w}
            SET href = t.href, modified = t.modified, jsonData = t.jsonData
            FROM ${tempTableNameForUpdates} t
            WHERE t.href = ${w}.href
              ${baseUrlColumnExists ? `AND t.baseurl = ${w}.baseurl` : ''}
              ${pathColumnExists ? `AND t.path = ${w}.path` : ''}
          `);
          console.log(`  -> Updated ${updateResults.rowCount} rows from ${config.writeTable} in ${elapsedTimeString(beforeUpdate, 's', updateResults.rowCount)}`);

          const beforeInsert = Date.now();

          // some records can appear multiple times if the result set changes while we are fetching
          // the pages, so try to remove doubles before inserting (take the one with most recent
          // modified)
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
            FROM ${tempTableNameForUpdates} t
            WHERE
              NOT EXISTS (
                  select 1 from ${w} w
                  where t.href = w.href
                    ${baseUrlColumnExists ? 'AND t.baseurl = w.baseurl' : ''}
                    ${pathColumnExists ? 'AND t.path = w.path' : ''}
                )`);
          console.log(`  -> Inserted ${insertResults.rowCount} rows into ${config.writeTable} in ${elapsedTimeString(beforeInsert, 's', insertResults.rowCount)}`);
        }
      } catch (e) {
        console.log(e.stack);
        throw new Error('copyTempTablesDataToWriteTable failed', e);
      }
      return 0;
    },
    /**
     * Update or insert the rows
     * @param {*} apiResults array of api resources
     */
    saveApiResultsToDb: async function saveApiResultsToDb(apiResults, transaction = null) {
      const myTransaction = transaction || await db.openTransaction();

      try {
        // const beforeInsertForUpdate = Date.now();
        const nrOfInsertedRowsForUpdate = await doBulkInsert(
          myTransaction,
          apiResults, // .filter(ar => !ar.$$meta.deleted),
          false,
          tempTableNameForUpdates,
        );
        // console.log(`  ${nrOfInsertedRowsForUpdate} rows inserted in ${elapsedTimeString(beforeInsertForUpdate, 'ms', nrOfInsertedRowsForUpdate, 's')}`);
      } catch (err) {
        console.error('Problem updating rows', err, err.stack);
        if (!transaction) db.rollbackTransaction(myTransaction);
        throw new Error('Problem updating rows', err);
      } finally {
        if (!transaction) await db.commitTransaction(myTransaction);
      }
    },
    /**
     * Update or insert the rows
     * @param {*} hrefs array of strings (hrefs)
     */
    deleteApiResultsFromDb: async function deleteApiResultsFromDb(hrefs, transaction = null) {
      const myTransaction = transaction || await db.openTransaction();

      try {
        // const beforeInsertForDelete = Date.now();
        const nrOfInsertedRowsForDelete = await doBulkInsert(myTransaction, hrefs, true, tempTableNameForDeletes);
        // console.log(`  ${nrOfInsertedRowsForDelete} rows inserted in ${elapsedTimeString(beforeInsertForDelete, 'ms', nrOfInsertedRowsForDelete, 's')}`);
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
 */
module.exports = function Sri2DbFactory(config = {}) {
  if (!config.api) {
    throw new Error('[Sri2Db] invalid config object, config.api object missing');
  }
  if (!config.db) {
    throw new Error('[Sri2Db] invalid config object, config.db object missing');
  }

  if (!config.api.baseUrl) {
    throw new Error('api.baseUrl is not defined.');
  }

  // the db module needs to know this too
  config.db.baseUrl = config.api.baseUrl;
  config.db.path = config.api.path;

  const api = sriClientFactory(config.api);

  const db = dbFactory(config.db);


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
    let nextJsonDataPromise = api.getList(url, {}, getListOptions);
    let pageNum = 0;
    let count = 0;
    while (nextJsonDataPromise) {
      console.log(`Trying to get ${nextPath}`);
      // eslint-disable-next-line no-await-in-loop
      const jsonData = await nextJsonDataPromise;
      nextOffset += limit;
      if (jsonData.next) {
        nextPath = options.nextLinksBroken
          ? `${url}&offset=${nextOffset}`
          : jsonData.next;
      } else {
        nextPath = null;
      }
      // already start fetching the next url
      nextJsonDataPromise = nextPath ? api.getList(`${nextPath}`, {}, { ...options, raw: true }) : null;

      // apply async function to batch
      try {
        const jsonDataTranslated = jsonData.length > 0 && jsonData[0].$$expanded
          ? jsonData.map(r => r.$$expanded)
          : jsonData.map(r => r.href);
        // eslint-disable-next-line no-await-in-loop
        await asyncFunctionToApply(jsonDataTranslated, nextJsonDataPromise === null, pageNum, count);
      } catch (e) {
        console.log('Error while trying to apply the given function to the current page', e.stack);
        throw e;
      }

      count += jsonData.length;
      pageNum += 1;
    }
    return count;
  }


  // Create the necessary functions
  let lastSyncDate = null;
  const safetyWindow = 24 * 60 * 60 * 1000; // 24 hours
  /**
   * Gets the last known sync date for this process (fetch from DB - a safety window if none known or set)
   * @param {*} rounded = true will return the lower second (floor)
   *   if you need second instead of millisecond precision
   */
  const getSafeLastSyncDate = async function getLastSyncDate(rounded = false) {
    // console.log(`getLastSyncDate ${config.api.baseUrl}`);

    // store it in memory, but if it's not initialized, you might need
    // to do a single SELECT on the database
    lastSyncDate = lastSyncDate
      || new Date((await db.getLastSyncDate(config.api.path)).getTime() - safetyWindow);
    return rounded
      ? new Date(Math.floor((lastSyncDate.getTime() / 1000) * 1000))
      : lastSyncDate;
  };

  /**
   * Sets the last known sync date.
   *
   * @param {Date} d
   */
  function setLastSyncDate(d) {
    if (d instanceof Date) lastSyncDate = d;
    else throw new Error('setLastSyncDate expects a Date parameter');
  }


  /**
   *
   * @param {*} modifiedSince if UNDEFINED it will use lastSyncDate and NULL will be equivalent to a full-sync?
   */
  let deltaSyncRunning = false;
  let beforePreviousSync = null;
  const deltaSync = async function deltaSync(modifiedSince) {
    const beforeSync = Date.now();

    if (deltaSyncRunning) { return 0; }
    deltaSyncRunning = true;
    try {
      const isFullSync = modifiedSince === null;
      console.log(`${isFullSync ? 'FULL' : 'DELTA'} sync ${config.api.baseUrl}${config.api.path}`);

      let modifiedSinceString = null;
      if (modifiedSince === undefined) {
        modifiedSinceString = moment(await getSafeLastSyncDate())/* .subtract(10, 'seconds') */
          .toISOString();
      } else if (modifiedSince === null) {
        //= > full sync
      } else {
        modifiedSinceString = moment(modifiedSince).toISOString();
      }

      // const path = `${config.api.baseUrl}${config.api.path}${config.api.path.includes('?') ? '&' : '?'}$$meta.deleted=any&modifiedSince=${modifiedSinceString}`;
      const limit = config.api.limit || 500;
      // old version with &&meta.deleted=any
      // const firstPath = `${config.api.path}${config.api.path.includes('?') ? '&' : '?'}limit=${limit}&$$meta.deleted=any${modifiedSinceString ? `&modifiedSince=${modifiedSinceString}` : ''}`;

      const pathHasFilters = config.api.path.includes('?');
      const pathWithoutFilters = config.api.path.split('?')[0];
      const extraQueryParams = `limit=${limit}${modifiedSinceString ? `&modifiedSince=${modifiedSinceString}` : ''}`;

      const updatedResourcesPath = `${config.api.path}${pathHasFilters ? '&' : '?'}${extraQueryParams}&expand=FULL`;
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
      // const filteredNonExpandedPath = `${path}${pathHasFilters ? '&' : '?'}expand=NONE&limit=*`;


      let totalCount = 0;
      let lastModified = '';
      const dbTransaction = await db.openTransaction();
      try {
        await db.createTempTables(dbTransaction);

        const beforeApiCalls = Date.now();

        // first delete (real deletions)
        await applyFunctionToList(async (hrefs, isLastPage, pageNum, count) => {
          console.log(`[page ${pageNum}] Trying to store ${hrefs.length} records on the DB for deletion (${count} done so far)`);
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

        // then insert or update
        // keep a list of 'seen' urls, to use later on
        const updatedResourcesInFilteredList = [];
        await applyFunctionToList(async (resources, isLastPage, pageNum, count) => {
          console.log(`[page ${pageNum}] Trying to store ${resources.length} records on the DB for update/insert (${count} done so far)`);
          await db.saveApiResultsToDb(resources, dbTransaction);
          lastModified = resources.reduce(
            (acc, cur) => (cur.$$meta.modified > acc ? cur.$$meta.modified : acc),
            lastModified,
          );
          // console.log('  Most recent lastmodified seen so far:', lastModified);
          console.log(`Synced ${count + resources.length} api resources so far in ${elapsedTimeString(beforeSync, 'm', count, 's')}.`);

          updatedResourcesInFilteredList.push(...resources.map(r => r.$$meta.permalink));

          if (isLastPage) totalCount += count + resources.length;
        },
        updatedResourcesPath, { nextLinksBroken: config.api.nextLinksBroken });


        await db.copyTempTablesDataToWriteTable(dbTransaction, isFullSync);
        if (config.dryRun) {
          console.log('Not committing transaction because dryRun');
          db.rollbackTransaction(dbTransaction);
        } else {
          await db.commitTransaction(dbTransaction);
        }

        const syncDuration = Date.now() - beforeApiCalls;
        // potentially we have seen results that were too recent (popping up while we were syncing),
        // so subtract the duration of the sync * 1.01 (0.99 to overcompensate for any clock
        // differences between our machine and the server) from the most recent 'modified' date
        // ALSO look at the delta between the start of the current and the previous sync,
        // because we can safely add this delta * 0.99 (0.99 to overcompensate for any clock
        // differences between our machine and the server)
        const timeBetweenThisAndPreviousSync = beforePreviousSync ? (beforeSync - beforePreviousSync) : 0;
        if (lastModified && lastModified.length > 0) {
          setLastSyncDate(new Date(Date.parse(lastModified)
            - Math.round(syncDuration * 1.01)
            + Math.round(timeBetweenThisAndPreviousSync * 0.99)));
        } else {
          const previousLastSyncDate = await getSafeLastSyncDate();
          const newLastSyncDate = new Date((modifiedSince || previousLastSyncDate.getTime())
            + Math.round(timeBetweenThisAndPreviousSync * 0.99));
          console.log(`Updating LAST SYNC DATE from ${previousLastSyncDate.toISOString()} to ${newLastSyncDate.toISOString()}`);
          setLastSyncDate(newLastSyncDate);
        }
        console.log('==== New LAST SYNC DATE =', (await getSafeLastSyncDate()).toISOString());

        console.log(`Synced ${totalCount} api resources in ${elapsedTimeString(beforeSync, 'm', totalCount, 's')}.`);
        return totalCount;
      } catch (e) {
        console.warn(`Problem while doing ${isFullSync ? 'FULL' : 'DELTA'} sync`, e, e.stack);
        if (e instanceof SriClientError) {
          console.warn(JSON.stringify(e.body, null, 2));
        }
        db.rollbackTransaction(dbTransaction);
      }
    } finally {
      deltaSyncRunning = false;
      // if (!modifiedSince)
      beforePreviousSync = beforeSync;
    }
    return 0;
  };

  const isDeltaSyncRunning = function isDeltaSyncRunning() {
    return deltaSyncRunning;
  };


  // do a full sync getting all resources from the API and storing them
  // this means emptying the existing DB first so we don't keep any deleted records lying around !
  const fullSync = async function fullSync() {
    // console.log(`fullSync ${config.api.baseUrl}${config.api.path}`);
    return deltaSync(null);
  };


  let socket = null;
  let deltaSyncOnBroadcastInterval;
  let deltaSyncRequested = false;
  // Utility function, should/can only set up a websocket if the config tells you what the broadcastUrl is
  const installBroadCastListeners = function installBroadCastListeners() {
    if (!config.broadcastUrl) {
      return null;
    }

    socket = io.connect(config.broadcastUrl);

    socket.on('connect', async () => {
      console.log('CONNECTED to audit/broadcast, listening for updates...');
      deltaSyncOnBroadcastInterval = setInterval(() => {
        if (deltaSyncRequested && !deltaSyncRunning) {
          deltaSyncRequested = false;
          deltaSync();
        } else {
          console.log(`No broadcast triggered delta sync needed because ${!deltaSyncRequested ? 'no delta sync requested' : ''}${deltaSyncRequested && deltaSyncRunning ? 'a delta sync is requested but another one is currently running' : ''}`);
        }
      }, 1000);
      socket.emit('join', config.api.path);
    });

    socket.on('disconnect', () => {
      console.log('DISCONNECTED from audit/broadcast, trying to reconnect');
      uninstallBroadCastListeners();
      installBroadCastListeners(); // no need to await
    });

    socket.on('update', async (data) => {
      console.log(`--- Audit/broadcast sent us a new message: ${data}, requesting new delta sync.`);

      deltaSyncRequested = true;
    });

    return socket;
  };

  /*
  // Try to have a more robust connection retry strategy?
  let socket = null;
  let retryConnectInterval;
  // Utility function, should/can only set up a websocket if the config tells you what the broadcastUrl is
  const installBroadCastListeners = async function installBroadCastListeners() {
    if (!config.broadcastUrl) {
      return null;
    }

    socket = io.connect(config.broadcastUrl);

    socket.on('connect', async () => {
      console.log('CONNECTED to audit/broadcast, listening for updates...');

      // stop trying to connect
      clearInterval(retryConnectInterval);

      socket.emit('join', config.api.path);
    });

    socket.on('disconnect', async () => {
      console.log('DISCONNECTED from audit/broadcast, trying to reconnect');
      socket = null;
      // retry strategy to set up the connection again !
      retryConnectInterval = setInterval(10000, installBroadCastListeners);
    });

    socket.on('update', async (data) => {
      console.log(`--- Audit/broadcast sent us a new message: ${data}`);

      const lastSync = await getSafeLastSyncDate();

      if (moment(data.timestamp) > moment(lastSync)) {
        console.log('Scheduling a new delta sync because lastSync.', data.timestamp, lastSync);
        deltaSync();
      } else {
        console.log('No sync needed.', data.timestamp, lastSync);
      }
    });

    return socket;
  };
  */


  const uninstallBroadCastListeners = function uninstallBroadCastListeners() {
    if (socket) {
      socket.close();
      socket = null;
      clearInterval(deltaSyncOnBroadcastInterval);
    }
  };

  const isSocketConnected = () => socket != null;

  const close = function close() {
    uninstallBroadCastListeners();
    // close db connections etc?
  };


  // Now create and return the object containing the necessary functions for the user to use
  return {
    getSafeLastSyncDate,
    fullSync,
    deltaSync,
    isDeltaSyncRunning,
    installBroadCastListeners,
    uninstallBroadCastListeners,
    isSocketConnected,
    close,
  };
};


// export module.exports;
