const fs = require('fs');
const yargs = require('yargs');
const dayjs = require('dayjs');
const {MongoClient} = require('mongodb');
const mysql = require('mysql2/promise');
require('dotenv').config();

const defaultFrom = dayjs().add(-1, 'day').format('YYYY-MM-DD 00:00:00');
const defaultTo = dayjs().add(-1, 'day').format('YYYY-MM-DD 23:59:59')

const argv = yargs
    .option('from', {
        alias: 'f',
        type: 'datetime',
        default: defaultFrom
    })
    .option('to', {
        alias: 't',
        type: 'datetime',
        default: defaultTo
    })
    .option('json', {
        alias: 'j',
        description: 'Json file with information about the tables it should copy data from.',
        type: 'string',
        default: 'tables.json'
    })
    .help()
    .alias('help', 'h').argv;

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

async function main() {

    const noSqlUrlParts = [NOSQL_URL];
    const noSqlConnectionOptions = {};

    if (!!+NOSQL_TLS) {
        noSqlConnectionOptions.tlsCAFile = NOSQL_CERTIFICATE;
    }

    const noSqlUrl = noSqlUrlParts.join('');
    const noSqlClient = new MongoClient(noSqlUrl, noSqlConnectionOptions);

    await noSqlClient.connect();
    const noSqlDb = noSqlClient.db(NOSQL_DBNAME);

    const sqlConnection = await mysql.createConnection(
        {
            host: SQL_HOST,
            port: SQL_PORT,
            user: SQL_USER,
            password: SQL_PASSWORD,
            database: SQL_DBNAME
        }
    );

    console.info(`Copying data from ${argv.from} to ${argv.to}`);

    const tablesJson = fs.readFileSync(argv.json);
    const tables = JSON.parse(tablesJson);

    for (const table of tables) {

        await processTable(sqlConnection, noSqlDb, table);
    }

    noSqlClient.close();
    sqlConnection.end();
}

async function processTable(sqlConnection, noSqlDb, table) {

    const now = new Date();
    console.debug(
        `${now.toLocaleTimeString('en-US', timeOptions)}:${now.getMilliseconds()} -`,
        table.name
    );

    const columns = table.columns.join(',');

    const sql = `
        SELECT t.${table.primaryKeyColumn}, t.${columns}
        FROM ${table.name} t
        WHERE t.${table.insertDateColumn} BETWEEN '${argv.from}' AND '${argv.to}'
            OR t.${table.updateDateColumn} BETWEEN '${argv.from}' AND '${argv.to}';
    `
    const [rows] = await sqlConnection.execute(sql);

    for (const row of rows) {

        const noSqlCollection = noSqlDb.collection(table.name);
        await noSqlCollection.replaceOne({'_id': row.id}, row, {upsert: true});
    }
}

main();
