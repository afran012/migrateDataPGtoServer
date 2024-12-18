const { exec } = require('child_process');

const migrateData = () => {
  console.log('Starting data migration process...');
  exec('node extractData.js', (err, stdout, stderr) => {
    if (err) {
      console.error('Error extracting data:', err);
      console.error(stderr);
      return;
    }
    console.log(stdout);
    console.log('Data extraction completed. Starting data insertion...');
    exec('node insertData.js', (err, stdout, stderr) => {
      if (err) {
        console.error('Error inserting data:', err);
        console.error(stderr);
        return;
      }
      console.log(stdout);
      console.log('Data migration process completed successfully.');
    });
  });
};

migrateData();