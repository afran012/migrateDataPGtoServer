const fs = require('fs');
const path = require('path');

const createFile = (filePath, content) => {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content);
};

const setupProject = () => {
  // .env file content
  const envContent = `
# PostgreSQL Database Configuration
PG_HOST=your_postgres_host
PG_PORT=your_postgres_port
PG_USER=your_postgres_user
PG_PASSWORD=your_postgres_password
PG_DATABASE=your_postgres_database

# SQL Server Database Configuration
SQLSERVER_HOST=your_sqlserver_host
SQLSERVER_PORT=your_sqlserver_port
SQLSERVER_USER=your_sqlserver_user
SQLSERVER_PASSWORD=your_sqlserver_password
SQLSERVER_DATABASE=your_sqlserver_database
`;

  // .gitignore file content
  const gitignoreContent = `
# Environment variables
.env

# Node modules
node_modules

# Logs
logs
*.log
npm-debug.log*
yarn-debug.log*
yarn-error.log*

# OS generated files
.DS_Store
Thumbs.db
`;

  // extractData.js content
  const extractDataContent = `
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
    const query = \`
      SELECT * FROM public.diferencia_rural_area;
      SELECT * FROM public.diferencia_urbana_area;
    \`;
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
`;

  // insertData.js content
  const insertDataContent = `
require('dotenv').config();
const sql = require('mssql');
const fs = require('fs');

const config = {
  user: process.env.SQLSERVER_USER,
  password: process.env.SQLSERVER_PASSWORD,
  server: process.env.SQLSERVER_HOST,
  database: process.env.SQLSERVER_DATABASE,
  port: parseInt(process.env.SQLSERVER_PORT, 10),
  options: {
    encrypt: true, // Use this if you're on Windows Azure
    enableArithAbort: true,
  },
};

const insertData = async () => {
  try {
    const pool = await sql.connect(config);
    const data = JSON.parse(fs.readFileSync('data.json', 'utf8'));

    for (const row of data) {
      const query = \`
        INSERT INTO public.diferencia_rural_area (id_0, geom, id, fid, fid_2, avaluo_ter, avaluo_com, terreno_co, dimension, etiqueta, relacion_s, espacio_de, local_id, created_us, created_da, last_edite, last_edi_1, globalid, shape_leng, shape_area, area_m2)
        VALUES (@id_0, @geom, @id, @fid, @fid_2, @avaluo_ter, @avaluo_com, @terreno_co, @dimension, @etiqueta, @relacion_s, @espacio_de, @local_id, @created_us, @created_da, @last_edite, @last_edi_1, @globalid, @shape_leng, @shape_area, @area_m2);
      \`;
      await pool.request()
        .input('id_0', sql.Int, row.id_0)
        .input('geom', sql.VarChar, row.geom)
        .input('id', sql.BigInt, row.id)
        .input('fid', sql.Numeric, row.fid)
        .input('fid_2', sql.Numeric, row.fid_2)
        .input('avaluo_ter', sql.Numeric, row.avaluo_ter)
        .input('avaluo_com', sql.Numeric, row.avaluo_com)
        .input('terreno_co', sql.VarChar, row.terreno_co)
        .input('dimension', sql.BigInt, row.dimension)
        .input('etiqueta', sql.VarChar, row.etiqueta)
        .input('relacion_s', sql.BigInt, row.relacion_s)
        .input('espacio_de', sql.VarChar, row.espacio_de)
        .input('local_id', sql.VarChar, row.local_id)
        .input('created_us', sql.VarChar, row.created_us)
        .input('created_da', sql.Date, row.created_da)
        .input('last_edite', sql.VarChar, row.last_edite)
        .input('last_edi_1', sql.Date, row.last_edi_1)
        .input('globalid', sql.VarChar, row.globalid)
        .input('shape_leng', sql.Numeric, row.shape_leng)
        .input('shape_area', sql.Numeric, row.shape_area)
        .input('area_m2', sql.Numeric, row.area_m2)
        .query(query);
    }

    console.log('Data inserted successfully');
  } catch (err) {
    console.error('Error inserting data:', err);
  } finally {
    sql.close();
  }
};

insertData();
`;

  // migrateData.js content
  const migrateDataContent = `
const { exec } = require('child_process');

const migrateData = () => {
  exec('node extractData.js', (err, stdout, stderr) => {
    if (err) {
      console.error('Error extracting data:', err);
      return;
    }
    console.log(stdout);
    exec('node insertData.js', (err, stdout, stderr) => {
      if (err) {
        console.error('Error inserting data:', err);
        return;
      }
      console.log(stdout);
    });
  });
};

migrateData();
`;

  // Create files
  createFile('.env', envContent);
  createFile('.gitignore', gitignoreContent);
  createFile('extractData.js', extractDataContent);
  createFile('insertData.js', insertDataContent);
  createFile('migrateData.js', migrateDataContent);

  console.log('Project setup completed successfully');
};

setupProject();