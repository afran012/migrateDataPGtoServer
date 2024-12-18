require('dotenv').config();
const sql = require('mssql');
const fs = require('fs');
const cliProgress = require('cli-progress');

const config = {
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

const insertData = async () => {
  try {
    console.log('Connecting to SQL Server...');
    const pool = await sql.connect(config);
    console.log('Connected to SQL Server successfully.');

    console.log('Reading data from data.json...');
    const data = JSON.parse(fs.readFileSync('data.json', 'utf8'));

    const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
    bar.start(data.length, 0);

    console.log('Inserting data into SQL Server...');
    for (const [index, row] of data.entries()) {
      const query = `
        INSERT INTO public.diferencia_rural_area (id_0, geom, id, fid, fid_2, avaluo_ter, avaluo_com, terreno_co, dimension, etiqueta, relacion_s, espacio_de, local_id, created_us, created_da, last_edite, last_edi_1, globalid, shape_leng, shape_area, area_m2)
        VALUES (@id_0, @geom, @id, @fid, @fid_2, @avaluo_ter, @avaluo_com, @terreno_co, @dimension, @etiqueta, @relacion_s, @espacio_de, @local_id, @created_us, @created_da, @last_edite, @last_edi_1, @globalid, @shape_leng, @shape_area, @area_m2);
      `;
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

      bar.update(index + 1);
      console.log(`Inserted row ${index + 1} of ${data.length}`);
    }
    bar.stop();

    console.log('Data insertion completed successfully.');
  } catch (err) {
    console.error('Error inserting data:', err);
  } finally {
    sql.close();
  }
};

insertData();