const express = require('express');
const bodyParser = require('body-parser');
const { Pool } = require('pg');
const NodeCache = require('node-cache');

const app = express();
const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'ipaantst',
    password: 'kyler',
    port: 5432,
});
const myCache = new NodeCache();

app.use(bodyParser.json());

// Middleware for validating that only SELECT statements are allowed
const validateSelectQuery = (req, res, next) => {
  const { sql } = req.body;
  if (!sql) {
    return res.status(400).send('SQL query is required');
  }

  // Trim and convert the query to uppercase for consistent validation
  const trimmedSql = sql.trim().toUpperCase();

  // Check if the query starts with "SELECT"
  if (!trimmedSql.startsWith('SELECT')) {
    return res.status(400).send('Only SELECT statements are allowed');
  }

  // Additional validation to prevent dangerous operations in SELECT statements
  const forbiddenClauses = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'TRUNCATE', 'EXECUTE'];
  const hasForbiddenClauses = forbiddenClauses.some(clause => trimmedSql.includes(clause));
  if (hasForbiddenClauses) {
    return res.status(400).send('Forbidden SQL operation detected');
  }

  // If the query passes all checks, proceed to the next middleware
  next();
};

app.post('/execute-query', validateSelectQuery, async (req, res) => {
  const { sql, params } = req.body;

  const cacheKey = `query_${Buffer.from(sql).toString('base64')}`; //creates a key for the response so that it can be cached for quicker responses
  const cachedData = myCache.get(cacheKey);

  if (cachedData) {
    return res.status(200).json(cachedData);
  }

  try {
    const result = await pool.query(sql, params);
    myCache.set(cacheKey, result.rows, 3600); // Cache for 1 hour
    res.status(200).json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
