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