require('dotenv').config();
const { Pool } = require('pg');
const sql = require('mssql');
const cliProgress = require('cli-progress');

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
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

async function insertRow(pool, tableName, row) {
  const request = pool.request();
  const insertQuery = `
    INSERT INTO TRIBUTAI.${tableName} (
      id_0, geom, id, fid, fid_2, avaluo_ter, avaluo_com,
      terreno_co, dimension, etiqueta, relacion_s, espacio_de,
      local_id, created_us, created_da, last_edite, last_edi_1,
      globalid, shape_leng, shape_area, area_m2
    )
    VALUES (
      @id_0, @geom, @id, @fid, @fid_2, @avaluo_ter, @avaluo_com,
      @terreno_co, @dimension, @etiqueta, @relacion_s, @espacio_de,
      @local_id, @created_us, @created_da, @last_edite, @last_edi_1,
      @globalid, @shape_leng, @shape_area, @area_m2
    );
  `;

  await request
    .input('id_0', sql.Int, row.id_0)
    .input('geom', sql.VarChar(sql.MAX), row.geom ? row.geom.toString() : null)
    .input('id', sql.BigInt, row.id)
    .input('fid', sql.Numeric(18,0), row.fid)
    .input('fid_2', sql.Numeric(18,0), row.fid_2)
    .input('avaluo_ter', sql.Numeric(18,2), row.avaluo_ter)
    .input('avaluo_com', sql.Numeric(18,2), row.avaluo_com)
    .input('terreno_co', sql.VarChar(255), row.terreno_co)
    .input('dimension', sql.BigInt, row.dimension)
    .input('etiqueta', sql.VarChar(255), row.etiqueta)
    .input('relacion_s', sql.BigInt, row.relacion_s)
    .input('espacio_de', sql.VarChar(255), row.espacio_de)
    .input('local_id', sql.VarChar(255), row.local_id)
    .input('created_us', sql.VarChar(255), row.created_us)
    .input('created_da', sql.Date, row.created_da)
    .input('last_edite', sql.VarChar(255), row.last_edite)
    .input('last_edi_1', sql.Date, row.last_edi_1)
    .input('globalid', sql.VarChar(255), row.globalid)
    .input('shape_leng', sql.Numeric(18,8), row.shape_leng)
    .input('shape_area', sql.Numeric(18,8), row.shape_area)
    .input('area_m2', sql.Numeric(18,8), row.area_m2)
    .query(insertQuery);
}

async function processBatch(client, sqlPool, tableName, offset, batchSize, progressBar) {
  const selectQuery = `
    SELECT * FROM public.${tableName}
    LIMIT ${batchSize} OFFSET ${offset}
  `;
  
  const { rows } = await client.query(selectQuery);
  
  if (rows.length > 0) {
    for (const row of rows) {
      try {
        await insertRow(sqlPool, tableName, row);
      } catch (error) {
        console.error(`Error insertando fila:`, error);
        throw error;
      }
    }
    progressBar.update(offset + rows.length);
  }
  
  return rows.length;
}

async function createTableIfNotExists(pool, tableName) {
  const schemaQuery = `
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'TRIBUTAI')
    BEGIN
      EXEC('CREATE SCHEMA TRIBUTAI')
    END
  `;
  await pool.request().query(schemaQuery);

  const query = `
    IF NOT EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'TRIBUTAI' AND t.name = '${tableName}')
    CREATE TABLE TRIBUTAI.${tableName} (
      id_0 INT,
      geom VARCHAR(MAX),
      id BIGINT,
      fid NUMERIC(18,0),
      fid_2 NUMERIC(18,0),
      avaluo_ter NUMERIC(18,2),
      avaluo_com NUMERIC(18,2),
      terreno_co VARCHAR(255),
      dimension BIGINT,
      etiqueta VARCHAR(255),
      relacion_s BIGINT,
      espacio_de VARCHAR(255),
      local_id VARCHAR(255),
      created_us VARCHAR(255),
      created_da DATE,
      last_edite VARCHAR(255),
      last_edi_1 DATE,
      globalid VARCHAR(255),
      shape_leng NUMERIC(18,8),
      shape_area NUMERIC(18,8),
      area_m2 NUMERIC(18,8)
    );
  `;
  
  await pool.request().query(query);
  console.log(`Tabla ${tableName} verificada/creada en esquema TRIBUTAI`);
}

async function migrateData() {
  const pgPool = new Pool(pgConfig);
  let sqlPool;
  const batchSize = 50; // Reducido el tamaño del lote
  
  try {
    console.log('Iniciando proceso de migración...');
    
    const client = await pgPool.connect();
    console.log('Conectado a PostgreSQL (DB_TRIBUTAI_GDB)');
    
    sqlPool = await sql.connect(sqlConfig);
    console.log('Conectado a SQL Server (DB_TRIBUTAI_TXN)');
    
    const tables = ['diferencia_rural_area', 'diferencia_urbana_area'];
    const multiBar = new cliProgress.MultiBar({
      clearOnComplete: false,
      hideCursor: true,
      format: '{table} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}'
    }, cliProgress.Presets.shades_classic);

    for (const tableName of tables) {
      console.log(`\nProcesando tabla: ${tableName}`);
      
      await createTableIfNotExists(sqlPool, tableName);
      
      const countResult = await client.query(`SELECT COUNT(*) FROM public.${tableName}`);
      const totalRows = parseInt(countResult.rows[0].count);
      
      const progressBar = multiBar.create(totalRows, 0, { table: tableName });
      
      for (let offset = 0; offset < totalRows; offset += batchSize) {
        try {
          const processedRows = await processBatch(
            client,
            sqlPool,
            tableName,
            offset,
            batchSize,
            progressBar
          );
          
          if (processedRows === 0) break;
          
          console.log(`Procesados ${offset + processedRows} de ${totalRows} registros en ${tableName}`);
        } catch (error) {
          console.error(`Error procesando lote en offset ${offset} para ${tableName}:`, error);
          throw error;
        }
      }
    }
    
    multiBar.stop();
    console.log('\nMigración completada exitosamente');
    
  } catch (error) {
    console.error('Error durante la migración:', error);
    throw error;
  } finally {
    try {
      await pgPool.end();
      if (sql.connected) {
        await sql.close();
      }
      console.log('Conexiones cerradas correctamente');
    } catch (error) {
      console.error('Error cerrando conexiones:', error);
    }
  }
}

console.log('Iniciando script de migración...');
migrateData().catch(error => {
  console.error('Error fatal en la migración:', error);
  process.exit(1);
});