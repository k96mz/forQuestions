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

let pools = {};

const fetch = (database, view, cursor) => {
  return new Promise((resolve, reject) => {
    let features = [];
    cursor.read(fetchSize, (err, rows) => {
      if (err) {
        return reject(err);
      }
      if (rows.length === 0) {
        // 終了条件
        return resolve(0);
      }

      // console.log('rows: ', rows);
      rows.forEach(row => {
        // console.log('row :', row);
        let f = {
          type: 'Feature',
          properties: row,
          geometry: JSON.parse(row.st_asgeojson),
        };
        delete f.properties.st_asgeojson;
        f.properties._database = database;
        f.properties._view = view;
        //f = modify(f)
        features.push(f);
      });

      // 取得したデータを出力
      features.forEach(f => {
        console.log(f);
      });

      // 読み込んだ行数を返す
      // console.log('rows.length', rows.length);
      resolve(rows.length);
    });
  });
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
      // カラムの取得
      let sql = `SELECT column_name FROM information_schema.columns WHERE table_schema = '${schema}' AND table_name = '${view}' ORDER BY ordinal_position`;
      let cols = await client.query(sql);
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
      console.error('Error executing query:', err);
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
})();
