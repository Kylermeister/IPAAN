const {google} = require('google-auth-library');
const {BigQuery} = require('@google-cloud/bigquery');
const {Client} = require('pg');
const cron = require('node-cron');

// PostgreSQL connection details
const pgClient = new Client({
  user: 'admin',
  host: 'localhost',
  database: 'postgres',
  password: 'kyler',
  port: 5433,
});

pgClient.connect();

// Function to authenticate using OAuth 2.0
async function authenticate() {
    const keys = require('./path/to/your/oauth_client_id.json');
    const client = new google.auth.OAuth2(keys.client_id, keys.client_secret, keys.redirect_uris[0]);
    client.setCredentials({
        refresh_token: 'your_refresh_token'
    });
    return client;
}

// Function to fetch daily data from BigQuery
async function fetchDailyDataFromBigQuery(authClient) {
    const bigquery = new BigQuery({authClient});
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const formattedDate = yesterday.toISOString().split('T')[0];
    const query = `
        SELECT *
        FROM \`ipaan-421411.IPAAN_Test.StagingTable\`'
    `;
    const [rows] = await bigquery.query({query});
    return rows;
}

// Function to insert data into PostgreSQL
async function insertDataIntoPostgresql(data) {
    const keys = Object.keys(data[0]);
    const columns = keys.join(',');
    const placeholders = keys.map((_, i) => `$${i + 1}`).join(',');
    const query = `INSERT INTO your_postgresql_table (${columns}) VALUES (${placeholders})`;
    
    for (const row of data) {
        const values = keys.map(key => row[key]);
        await pgClient.query(query, values);
    }
}

// ETL Process
async function etlProcess() {
    try {
        const authClient = await authenticate();
        const data = await fetchDailyDataFromBigQuery(authClient);
        if (data.length > 0) {
            await insertDataIntoPostgresql(data);
            console.log('Data successfully inserted into PostgreSQL.');
        } else {
            console.log('No data to insert.');
        }
    } catch (error) {
        console.error('Error during ETL process:', error);
    }
}

// Schedule the ETL job to run daily at 2 AM
cron.schedule('0 2 * * *', () => {
    console.log('Running daily ETL job');
    etlProcess();
});

module.exports = etlProcess;
