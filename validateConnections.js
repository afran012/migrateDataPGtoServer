require('dotenv').config();
const { Pool } = require('pg');
const sql = require('mssql');

const pgConfig = {
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,
};

const sqlConfig = {
  user: process.env.SQLSERVER_USER,
  password: process.env.SQLSERVER_PASSWORD,
  server: process.env.SQLSERVER_HOST,
  database: process.env.SQLSERVER_DATABASE,
  port: parseInt(process.env.SQLSERVER_PORT, 10),
  options: {
    encrypt: true, // Use this if you're on Windows Azure
    enableArithAbort: true,
    trustServerCertificate: true, // Add this line to trust self-signed certificates
  },
};

const validatePostgresConnection = async () => {
  const pool = new Pool(pgConfig);
  try {
    const client = await pool.connect();
    console.log('Connected to PostgreSQL successfully');
    client.release();
  } catch (err) {
    console.error('Error connecting to PostgreSQL:', err);
  } finally {
    pool.end();
  }
};

const validateSQLServerConnection = async () => {
  try {
    const pool = await sql.connect(sqlConfig);
    console.log('Connected to SQL Server successfully');
    pool.close();
  } catch (err) {
    console.error('Error connecting to SQL Server:', err);
  }
};

const validateConnections = async () => {
  await validatePostgresConnection();
  await validateSQLServerConnection();
};

validateConnections();