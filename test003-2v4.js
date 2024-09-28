const config = require('config');
const { Pool } = require('pg');
const Cursor = require('pg-cursor');

// config constants
const host = config.get('host');
const port = config.get('port');
const dbUser = config.get('dbUser');
const dbPassword = config.get('dbPassword');
const relations = config.get('relations');
const fetchSize = config.get('fetchSize');

const pools = {};

const fetch = async (database, view, cursor) => {
  try {
    const rows = await cursor.read(fetchSize);
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
      f.properties._view = view;
      //f = modify(f)
      return f;
    });

    // 取得したデータを出力
    features.forEach(f => {
      console.log(f);
    });

    return rows.length;
  } catch (err) {
    console.error(`Error in fetch function for ${view} in ${database}:`, err);
    throw err;
  }
};

(async () => {
  for (const relation of relations) {
    const startTime = new Date();
    const [database, schema, view] = relation.split('::');
    if (!pools[database]) {
      pools[database] = new Pool({
        host: host,
        user: dbUser,
        port: port,
        password: dbPassword,
        database: database,
        idleTimeoutMillis: 1000,
      });
    }

    let client, cursor;
    try {
      // プールへの接続
      client = await pools[database].connect();
      // プレースホルダを使用してカラムの取得
      let sql = `
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = $1 
            AND table_name = $2 
            ORDER BY ordinal_position`;
      let cols = await client.query(sql, [schema, view]);

      // geomカラムの削除
      cols = cols.rows.map(r => r.column_name).filter(r => r !== 'geom');
      // カラムの最後にGeoJSON化したgeomを追加
      cols.push(`ST_AsGeoJSON(${schema}.${view}.geom)`);
      await client.query('BEGIN');
      // カラムの文字列化
      sql = `SELECT ${cols.toString()} FROM ${schema}.${view}`;
      cursor = await client.query(new Cursor(sql));
      // 全てのデータが読み込まれるまで繰り返し
      while ((await fetch(database, view, cursor)) !== 0) {}

      await client.query(`COMMIT`);
    } catch (err) {
      console.error(
        `Error executing query for ${schema}.${view} in ${database}:`,
        err
      );
      // エラーが発生した場合はロールバック
      if (client) {
        await client.query('ROLLBACK');
      }
    } finally {
      const endTime = new Date();
      const workTime = (endTime.getTime() - startTime.getTime()) / 1000;
      console.log(
        `workingTime for ${schema}.${view} in ${database} is ${workTime} (sec). End`
      );
      if (cursor) {
        await cursor.close();
      }
      if (client) {
        client.release();
      }
    }
  }
  for (const db in pools) {
    try {
      await pools[db].end();
      console.log(`Closed pool for ${db}.`);
    } catch (err) {
      console.error(`Error closing pool for ${db}: ${err}`);
    }
  }
})();
