#!/usr/bin/env node
const fs = require('fs');
const dayjs = require('dayjs');
const {MongoClient} = require('mongodb');
const mysql = require('mysql2');
require('dotenv').config();

const NOSQL_URL = process.env.NOSQL_URL || 'mongodb://root:root@localhost:27017';
const NOSQL_DBNAME = process.env.NOSQL_DBNAME || 'jotunheim';
const NOSQL_TLS = process.env.NOSQL_TLS || 0;
const NOSQL_CERTIFICATE = process.env.NOSQL_CERTIFICATE || '/';

const SQL_HOST = process.env.SQL_HOST || 'localhost';
const SQL_PORT = process.env.SQL_PORT || 3306;
const SQL_USER = process.env.SQL_USER || 'root';
const SQL_PASSWORD = process.env.SQL_PASSWORD || 'root';
const SQL_DBNAME = process.env.SQL_DBNAME || 'jotunheim';

const timeOptions = {hour12: false};

let noSqlDb;
let noSqlClient;
let sqlConnection;

const _ = require('yargs')
    .scriptName("mysql2mongo")
    .usage('$0 <cmd> [args]')
    .command('incremental', 'copy incremental data based on the last run', function (yargs) {

        return yargs
            .option('tables', {
                description: 'Json file with information about the tables it should copy data from.',
                type: 'string',
                default: 'tables.json'
            });
    }, incremental)
    .command('fulltable <tableName>', 'Copy the entire table', function (yargs) {

        return yargs
            .positional('tableName', {
                type: 'string',
                describe: 'The name of the table will be copied'
            })
            .option('tables', {
                description: 'Json file with information about the tables it should copy data from.',
                type: 'string',
                default: 'tables.json'
            });
    }, fulltable)
    .command('period', 'Copy all tables data from provided period', function (yargs) {

        const defaultFrom = dayjs().add(-1, 'day').format('YYYY-MM-DD 00:00:00');
        const defaultTo = dayjs().add(-1, 'day').format('YYYY-MM-DD 23:59:59')

        return yargs
            .option('tables', {
                description: 'Json file with information about the tables it should copy data from.',
                type: 'string',
                default: 'tables.json'
            })
            .option('from', {
                alias: 'f',
                type: 'datetime',
                default: defaultFrom
            })
            .option('to', {
                alias: 't',
                type: 'datetime',
                default: defaultTo
            });
    }, period)
    .help()
    .alias('help', 'h')
    .argv;

async function incremental(argv) {

    await connectToDatabases();

    const noSqlCollection = noSqlDb.collection('_lastRun');

    let lastRun = await noSqlCollection.findOne({'_id': 0});

    if (!lastRun) lastRun = {};

    lastRun.date = lastRun.date || dayjs().add(-1, 'day').format('YYYY-MM-DD 00:00:00');

    const to = dayjs().format('YYYY-MM-DD hh:mm:ss')

    console.info(`Copying data from ${lastRun.date} to ${to}`);

    const tablesJson = fs.readFileSync(argv.tables);
    const tables = JSON.parse(tablesJson);

    for (const table of tables) {

        await processTable(table, lastRun.date, to);
    }

    lastRun.date = to;
    await noSqlCollection.replaceOne({'_id': 0}, lastRun, {upsert: true});

    sqlConnection.end();
    noSqlClient.close();
}

async function fulltable(argv) {

    await connectToDatabases();

    console.info(`Copying all data from ${argv.tableName} table`);

    const tablesJson = fs.readFileSync(argv.tables);
    const tables = JSON.parse(tablesJson);

    let tableConfig;

    for (const table of tables) {

        if (table.name === argv.tableName) {

            tableConfig = table;
        }

    }

    if (tableConfig) {

        await processTable(tableConfig);
    } else {

        throw `Table ${argv.tableName} not found in the ${argv.tables} file`;
    }

    sqlConnection.end();
    noSqlClient.close();

}

async function period(argv) {

    await connectToDatabases();

    console.info(`Copying data from ${argv.from} to ${argv.to}`);

    const tablesJson = fs.readFileSync(argv.tables);
    const tables = JSON.parse(tablesJson);

    for (const table of tables) {

        await processTable(table, argv.from, argv.to);
    }

    sqlConnection.end();
    noSqlClient.close();
}

async function connectToDatabases() {

    const noSqlUrlParts = [NOSQL_URL];
    const noSqlConnectionOptions = {};

    if (!!+NOSQL_TLS) {
        noSqlConnectionOptions.tlsCAFile = NOSQL_CERTIFICATE;
    }

    const noSqlUrl = noSqlUrlParts.join('');
    noSqlClient = new MongoClient(noSqlUrl, noSqlConnectionOptions);

    await noSqlClient.connect();
    noSqlDb = noSqlClient.db(NOSQL_DBNAME);

    sqlConnection = await mysql.createConnection(
        {
            host: SQL_HOST,
            port: SQL_PORT,
            user: SQL_USER,
            password: SQL_PASSWORD,
            database: SQL_DBNAME
        }
    );
}

async function processTable(table, from, to) {

    const now = new Date();
    console.debug(
        `${now.toLocaleTimeString('en-US', timeOptions)}:${now.getMilliseconds()} -`,
        table.name
    );

    const columns = table.columns.join(',');

    let sql = `
        SELECT t.${table.primaryKeyColumn}, t.${columns}
        FROM ${table.name} t
    `;

    if (from && to) {

        sql += `  
        WHERE (t.${table.insertDateColumn} >= '${from}' AND t.${table.insertDateColumn} <= '${to}')
           OR (t.${table.updateDateColumn} >= '${from}' AND t.${table.updateDateColumn} <= '${to}')
       `;
    }

    const query = sqlConnection.query(sql);
    const noSqlCollection = noSqlDb.collection(table.name);

    let totalRows = 0;
    let processedRows = 0;
    const maxParalellProcessing = 10000;
    const minParalellProcessing = 5000;
    await new Promise((resolve, reject) => {
        query
            .on('result', (row) => {

                totalRows++;
                printProcessesRows(processedRows, totalRows)

                if ((totalRows - processedRows) > maxParalellProcessing) {

                    sqlConnection.pause();
                }

                noSqlCollection
                    .replaceOne({'_id': row.id}, row, {upsert: true})
                    .then(() => {

                        processedRows++;
                        printProcessesRows(processedRows, totalRows);
                        if ((totalRows - processedRows) < minParalellProcessing) {

                            sqlConnection.resume();
                        }
                    });
            })
            .on('error', function (err) {

                console.error(err);
                reject();
            })
            .on('end', async () => {

                const interval = setInterval(() => {

                    if (totalRows === processedRows) {

                        clearInterval(interval);
                        console.debug(' Done.');
                        resolve();
                    }
                }, 100);
            });
    });
}

function printProcessesRows(processingRows, totalRows) {

    process.stdout.cursorTo(0);
    process.stdout.write(`    Processing ${processingRows}/${totalRows} rows    `);

}
