require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');
const cliProgress = require('cli-progress');

const pool = new Pool({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,
});

const extractData = async () => {
  try {
    console.log('Connecting to PostgreSQL...');
    const client = await pool.connect();
    console.log('Connected to PostgreSQL successfully.');

    const query = `
      SELECT * FROM public.diferencia_rural_area;
      SELECT * FROM public.diferencia_urbana_area;
    `;
    console.log('Executing query to extract data...');
    const result = await client.query(query);
    console.log('Data extracted from PostgreSQL.');

    const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
    bar.start(result.rows.length, 0);

    console.log('Writing data to data.json...');
    fs.writeFileSync('data.json', JSON.stringify(result.rows));
    for (let i = 0; i < result.rows.length; i++) {
      bar.update(i + 1);
    }
    bar.stop();

    client.release();
    console.log('Data extraction completed successfully.');
  } catch (err) {
    console.error('Error extracting data:', err);
  } finally {
    pool.end();
  }
};

extractData();