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
      client = await pools[database].connect();
      const sql = `SELECT * FROM ${schema}.${view} limit 1`;
      const res = await client.query(sql);
      console.log(res.rows);
    } catch (err) {
      console.error('Error executing query:', err);
    } finally {
      if (client) {
        client.release();
      }
    }
  }
})();
