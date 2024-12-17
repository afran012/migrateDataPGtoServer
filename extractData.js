
require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');

const pool = new Pool({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,
});

const extractData = async () => {
  try {
    const client = await pool.connect();
    const query = `
      SELECT * FROM public.diferencia_rural_area;
      SELECT * FROM public.diferencia_urbana_area;
    `;
    const result = await client.query(query);
    fs.writeFileSync('data.json', JSON.stringify(result.rows));
    client.release();
    console.log('Data extracted successfully');
  } catch (err) {
    console.error('Error extracting data:', err);
  } finally {
    pool.end();
  }
};

extractData();
