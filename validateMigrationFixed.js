require('dotenv').config();
const { Pool } = require('pg');
const sql = require('mssql');

const pgConfig = {
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: 'DB_TRIBUTAI_GDB',
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,
};

const sqlConfig = {
  user: process.env.SQLSERVER_USER,
  password: process.env.SQLSERVER_PASSWORD,
  server: process.env.SQLSERVER_HOST,
  database: 'DB_TRIBUTAI_TXN',
  port: parseInt(process.env.SQLSERVER_PORT, 10),
  options: {
    encrypt: true,
    enableArithAbort: true,
    trustServerCertificate: true,
  }
};

async function findExtraRecords(pgClient, sqlPool, tableName) {
  console.log('\nBuscando registros adicionales...');

  // Obtener todos los id_0 de PostgreSQL
  const pgQuery = 'SELECT id_0 FROM public.' + tableName + ' ORDER BY id_0';
  const pgResult = await pgClient.query(pgQuery);
  const pgIds = new Set(pgResult.rows.map(row => row.id_0));

  // Obtener todos los id_0 de SQL Server
  const sqlQuery = 'SELECT id_0 FROM TRIBUTAI.' + tableName + ' ORDER BY id_0';
  const sqlResult = await sqlPool.request().query(sqlQuery);
  const sqlIds = new Set(sqlResult.recordset.map(row => row.id_0));

  // Encontrar registros que están en SQL Server pero no en PostgreSQL
  const extraInSql = [...sqlIds].filter(id => !pgIds.has(id));
  if (extraInSql.length > 0) {
    console.log('\nRegistros encontrados en SQL Server pero no en PostgreSQL:');
    for (const id of extraInSql) {
      const detailQuery = `
        SELECT *
        FROM TRIBUTAI.${tableName}
        WHERE id_0 = ${id}
      `;
      const detailResult = await sqlPool.request().query(detailQuery);
      console.log('\nRegistro adicional encontrado:');
      console.table(detailResult.recordset);
    }
  }

  // Encontrar registros duplicados en SQL Server
  const duplicatesQuery = `
    WITH DuplicateCount AS (
      SELECT id_0, COUNT(*) as count
      FROM TRIBUTAI.${tableName}
      GROUP BY id_0
      HAVING COUNT(*) > 1
    )
    SELECT t.*, dc.count as numero_repeticiones
    FROM TRIBUTAI.${tableName} t
    INNER JOIN DuplicateCount dc ON t.id_0 = dc.id_0
    ORDER BY t.id_0;
  `;

  const duplicatesResult = await sqlPool.request().query(duplicatesQuery);
  if (duplicatesResult.recordset.length > 0) {
    console.log('\nRegistros duplicados encontrados en SQL Server:');
    console.table(duplicatesResult.recordset);
  }
}

async function validateTable(pgClient, sqlPool, tableName) {
  console.log(`\n=== Validando tabla: ${tableName} ===`);

  // 1. Validar conteo de registros
  const pgQuery = `SELECT COUNT(*) as count FROM public.${tableName}`;
  const sqlQuery = `SELECT COUNT(*) as count FROM TRIBUTAI.${tableName}`;

  const pgResult = await pgClient.query(pgQuery);
  const sqlResult = await sqlPool.request().query(sqlQuery);

  const pgCount = parseInt(pgResult.rows[0].count);
  const sqlCount = sqlResult.recordset[0].count;

  console.log('\nConteo de registros:');
  console.log(`PostgreSQL (origen): ${pgCount}`);
  console.log(`SQL Server (destino): ${sqlCount}`);
  
  if (pgCount !== sqlCount) {
    console.log(`\n⚠ Diferencia encontrada: ${sqlCount - pgCount} registros`);
    await findExtraRecords(pgClient, sqlPool, tableName);
  } else {
    console.log('✓ Los conteos coinciden correctamente');
  }
}

async function runValidation() {
  const pgPool = new Pool(pgConfig);
  let sqlPool;

  try {
    console.log('Iniciando validación detallada de migración...');
    
    const client = await pgPool.connect();
    console.log('Conectado a PostgreSQL (DB_TRIBUTAI_GDB)');
    
    sqlPool = await sql.connect(sqlConfig);
    console.log('Conectado a SQL Server (DB_TRIBUTAI_TXN)');

    const tables = ['diferencia_rural_area', 'diferencia_urbana_area'];
    
    for (const tableName of tables) {
      await validateTable(client, sqlPool, tableName);
    }

  } catch (error) {
    console.error('Error durante la validación:', error);
    if (error.stack) {
      console.error('Stack trace:', error.stack);
    }
  } finally {
    try {
      await pgPool.end();
      if (sql.connected) {
        await sql.close();
      }
      console.log('\nConexiones cerradas correctamente');
    } catch (error) {
      console.error('Error cerrando conexiones:', error);
    }
  }
}

console.log('Iniciando script de validación mejorado...');
runValidation().catch(error => {
  console.error('Error fatal en la validación:', error);
  process.exit(1);
});