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
  console.log(`Diferencia: ${sqlCount - pgCount}`);

  // 2. Verificar duplicados
  const duplicatesQuery = `
    SELECT id_0, COUNT(*) as repeticiones
    FROM TRIBUTAI.${tableName}
    GROUP BY id_0
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
  `;

  const duplicatesResult = await sqlPool.request().query(duplicatesQuery);
  
  if (duplicatesResult.recordset.length > 0) {
    console.log('\nRegistros duplicados encontrados:');
    console.table(duplicatesResult.recordset);
  } else {
    console.log('\n✓ No se encontraron duplicados');
  }

  // 3. Validar campos nulos o vacíos
  const nullCheckQuery = `
    SELECT 
      COUNT(CASE WHEN id_0 IS NULL THEN 1 END) as id_0_null,
      COUNT(CASE WHEN geom IS NULL THEN 1 END) as geom_null,
      COUNT(CASE WHEN id IS NULL THEN 1 END) as id_null,
      COUNT(CASE WHEN fid IS NULL THEN 1 END) as fid_null,
      COUNT(CASE WHEN terreno_co IS NULL THEN 1 END) as terreno_co_null
    FROM TRIBUTAI.${tableName}
  `;

  const nullCheckResult = await sqlPool.request().query(nullCheckQuery);
  
  console.log('\nCampos nulos encontrados:');
  console.table(nullCheckResult.recordset);

  // 4. Validar valores extremos
  const statsQuery = `
    SELECT 
      MIN(id_0) as min_id_0,
      MAX(id_0) as max_id_0,
      MIN(avaluo_ter) as min_avaluo_ter,
      MAX(avaluo_ter) as max_avaluo_ter,
      MIN(avaluo_com) as min_avaluo_com,
      MAX(avaluo_com) as max_avaluo_com
    FROM TRIBUTAI.${tableName}
  `;

  const statsResult = await sqlPool.request().query(statsQuery);
  
  console.log('\nEstadísticas de valores:');
  console.table(statsResult.recordset);
}

async function runValidation() {
  const pgPool = new Pool(pgConfig);
  let sqlPool;

  try {
    console.log('Iniciando validación de migración...');
    
    // Conectar a PostgreSQL
    const client = await pgPool.connect();
    console.log('Conectado a PostgreSQL (DB_TRIBUTAI_GDB)');
    
    // Conectar a SQL Server
    sqlPool = await sql.connect(sqlConfig);
    console.log('Conectado a SQL Server (DB_TRIBUTAI_TXN)');

    const tables = ['diferencia_rural_area', 'diferencia_urbana_area'];
    
    for (const tableName of tables) {
      await validateTable(client, sqlPool, tableName);
    }

  } catch (error) {
    console.error('Error durante la validación:', error);
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

console.log('Iniciando script de validación...');
runValidation().catch(error => {
  console.error('Error fatal en la validación:', error);
  process.exit(1);
});