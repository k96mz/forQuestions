// Libraries
const config = require('config');
const { Pool } = require('pg');
const Cursor = require('pg-cursor');
const modify = require('./modify2.js');
const { spawn } = require('child_process');
const fs = require('fs');
const Queue = require('better-queue');
const pretty = require('prettysize');
const TimeFormat = require('hh-mm-ss');
const Spinner = require('cli-spinner').Spinner;
const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');

// config constants
const relations = config.get('relations');
const fetchSize = config.get('fetchSize');
const tippecanoePath = config.get('tippecanoePath');
const pmtilesDir = config.get('pmtilesDir');
const logDir = config.get('logDir');
const spinnerString = config.get('spinnerString');

// global configurations
Spinner.setDefaultSpinnerString(spinnerString);
winston.configure({
  level: 'silly',
  format: winston.format.simple(),
  transports: [
    new DailyRotateFile({
      filename: `${logDir}/produce-clearmap-%DATE%.log`,
      datePattern: 'YYYY-MM-DD',
      maxSize: '20m',
      maxFiles: '14d',
    }),
  ],
});

// global variable
const pools = {};
const productionSpinner = new Spinner();
let moduleKeysInProgress = [];

const iso = () => {
  return new Date().toISOString();
};

const noPressureWrite = (downstream, f) => {
  return new Promise(res => {
    if (downstream.write(`\x1e${JSON.stringify(f)}\n`)) {
      res();
    } else {
      downstream.once('drain', () => {
        res();
      });
    }
  });
};

// viewをtableに変更、streamをdownstreamに変更
const fetch = async (database, schema, table, cursor, downstream) => {
  try {
    const rows = await cursor.read(fetchSize); // promise返す
    if (rows.length === 0) {
      // 終了条件
      return 0;
    }

    const features = rows.map(row => {
      let f = {
        type: 'Feature',
        properties: row,
        geometry: JSON.parse(row.st_asgeojson),
      };
      delete f.properties.st_asgeojson;
      f.properties._database = database;
      // schemaの記載を追記
      f.properties._schema = schema;
      f.properties._table = table;
      f = modify(f);
      return f;
    });

    for (const f of features) {
      try {
        // console.log(f);
        await noPressureWrite(downstream, f);
      } catch (err) {
        throw err;
      }
    }
    return rows.length;
  } catch (err) {
    // schemaの記載を追記
    console.error(
      `Error in fetch function for ${schema}.${table} in ${database}:`,
      err
    );
    throw err;
  }
};

const dumpAndModify = async (relation, downstream, moduleKey) => {
  // Promiseでのラップはなし、returnなどはいらないのか？
  const [database, schema, table] = relation.split('::');
  if (!pools[database]) {
    pools[database] = new Pool({
      host: config.get(`connection.${database}.host`),
      user: config.get(`connection.${database}.dbUser`),
      port: config.get(`connection.${database}.port`),
      password: config.get(`connection.${database}.dbPassword`),
      database: database,
    });
  }
  let client, cursor;
  try {
    // プールへの接続
    client = await pools[database].connect(); // promise返す
    // プレースホルダを使用してカラムの取得
    let sql = `
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = $1 
            AND table_name = $2 
            ORDER BY ordinal_position`;
    let cols = await client.query(sql, [schema, table]); // promise返す

    // geomカラムの削除
    cols = cols.rows.map(r => r.column_name).filter(r => r !== 'geom');
    //cols = cols.filter(v => !propertyBlacklist.includes(v))
    //test--------------------------
    if (table == 'unmap_wbya10_a') {
      cols.push(`ST_Area(${schema}.${table}.geom) AS areacalc`);
      cols.push(`ST_Length(${schema}.${table}.geom) AS lengthcalc`);
    }
    if (table == 'unmap_dral10_l') {
      cols.push(`ST_Length(${schema}.${table}.geom) AS lengthcalc`);
    }
    //until here--------------------
    // カラムの最後にGeoJSON化したgeomを追加
    cols.push(`ST_AsGeoJSON(${schema}.${table}.geom)`);
    await client.query('BEGIN');
    // カラムの文字列化
    sql = `SELECT ${cols.toString()} FROM ${schema}.${table}`;
    cursor = await client.query(new Cursor(sql)); // promise返す
    // 全てのデータが読み込まれるまで繰り返し
    while ((await fetch(database, schema, table, cursor, downstream)) !== 0) {}

    await client.query(`COMMIT`);
    console.log(` ${iso()}: finished ${relation} of Area ${moduleKey}`);
    winston.info(`${iso()}: finished ${relation} of ${moduleKey}`);
  } catch (err) {
    console.error(
      `Error executing query for ${schema}.${table} in ${database}:`,
      err
    );
    // エラーが発生した場合はロールバック
    if (client) {
      await client.query('ROLLBACK');
    }
    throw err;
  } finally {
    if (cursor) {
      await cursor.close(); // promise返す
    }
    if (client) {
      client.release();
    }
  }
};

const queue = new Queue(
  async (t, cb) => {
    const startTime = new Date();
    const moduleKey = t.moduleKey; //0-0-0
    const tmpPath = `${pmtilesDir}/part-${moduleKey}.pmtiles`;
    const dstPath = `${pmtilesDir}/${moduleKey}.pmtiles`;

    moduleKeysInProgress.push(moduleKey);
    productionSpinner.setSpinnerTitle(moduleKeysInProgress.join(', '));

    const tippecanoe = spawn(
      tippecanoePath,
      [
        '--quiet',
        '--no-feature-limit',
        '--no-tile-size-limit',
        '--force',
        '--simplification=2',
        '--drop-rate=1',
        '--minimum-zoom=0',
        '--maximum-zoom=5',
        '--base-zoom=5',
        '--hilbert',
        `--output=${tmpPath}`,
      ],
      { stdio: ['pipe', 'inherit', 'inherit'] }
    );
    tippecanoe.on('exit', () => {
      fs.renameSync(tmpPath, dstPath);
      moduleKeysInProgress = moduleKeysInProgress.filter(
        v => !(v === moduleKey)
      );
      productionSpinner.stop();
      process.stdout.write('\n');
      const logString = `${iso()}: process ${moduleKey} (${pretty(
        fs.statSync(dstPath).size
      )}) took ${TimeFormat.fromMs(new Date() - startTime)} .`;
      winston.info(logString);
      console.log(logString);
      if (moduleKeysInProgress.length !== 0) {
        productionSpinner.setSpinnerTitle('0-0-0');
        productionSpinner.start();
      }
      return cb();
    });

    productionSpinner.start();
    for (const relation of relations) {
      try {
        await dumpAndModify(relation, tippecanoe.stdin, moduleKey);
      } catch (err) {
        winston.error(err);
        cb(true);
      }
    }
    tippecanoe.stdin.end();
  },
  {
    concurrent: 1,
    maxRetries: 3,
    retryDelay: 1000,
  }
);

// push queue
const queueTasks = () => {
  for (const moduleKey of ['0-0-0']) {
    // For global, only one push!
    queue.push({
      moduleKey: moduleKey,
    });
  }
};

// disconnect pools
const closePools = async () => {
  for (const db in pools) {
    try {
      await pools[db].end();
      winston.info(`${iso()}: Closed pool for ${db}.`);
    } catch (err) {
      winston.error(`Error closing pool for ${db}: ${err}`);
    }
  }
};

// shutdown system
const shutdown = async () => {
  await closePools();
  winston.info(`${iso()}: production system shutdown.`);
  console.log('** production system for clearmap shutdown! **');
};

// main
const main = async () => {
  winston.info(`${iso()}: clearmap production started.`);
  queueTasks();
  queue.on('drain', async () => {
    await shutdown();
  });
};

main();
