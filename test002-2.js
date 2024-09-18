const config = require('config');
const { Pool } = require('pg');

// config constants
const host = config.get('host');
const port = config.get('port');
const dbUser = config.get('dbUser');
const dbPassword = config.get('dbPassword');
const relations = config.get('relations');

let pools = {};

(async () => {
  for (const relation of relations) {
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

    let client;
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
      // カラムの文字列化
      sql = `SELECT ${cols.toString()} FROM ${schema}.${view}`;
      const result = await client.query(sql);
      console.log(result.rows);
    } catch (err) {
      console.error('Error executing query:', err);
    } finally {
      if (client) {
        client.release();
      }
    }
  }
})();
