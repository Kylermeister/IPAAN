const express = require('express');
const { Pool } = require('pg');
const NodeCache = require('node-cache');

const app = express();
const port = 3000;

// Middleware to parse JSON bodies
app.use(express.json());

// PostgreSQL pool setup
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'ipaantst',
  password: 'kyler',
  port: 5432,
});

// Node-Cache setup
const myCache = new NodeCache();

// GET endpoint to fetch all users
app.get('/users', async (req, res) => {
  const cacheKey = 'allUsers';
  const cachedData = myCache.get(cacheKey);

  if (cachedData) {
    return res.status(200).json(cachedData);
  }

  try {
    const result = await pool.query('SELECT * FROM test1 LIMIT 1000');
    myCache.set(cacheKey, result.rows, 3600); // Cache for 1 hour
    res.status(200).json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

// POST endpoint to add a new user
app.post('/users', async (req, res) => {
  const { name, email } = req.body;
  try {
    const result = await pool.query(
      'INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *',
      [name, email]
    );
    myCache.del('allUsers'); // Invalidate the cache
    res.status(201).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

// PUT endpoint to update a user by ID
app.put('/users/:id', async (req, res) => {
  const { id } = req.params;
  const { name, email } = req.body;
  try {
    const result = await pool.query(
      'UPDATE users SET name = $1, email = $2 WHERE id = $3 RETURNING *',
      [name, email, id]
    );
    myCache.del('allUsers'); // Invalidate the cache
    res.status(200).json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

// DELETE endpoint to delete a user by ID
app.delete('/users/:id', async (req, res) => {
  const { id } = req.params;
  try {
    await pool.query('DELETE FROM users WHERE id = $1', [id]);
    myCache.del('allUsers'); // Invalidate the cache
    res.status(204).send();
  } catch (err) {
    console.error(err);
    res.status(500).send('Server error');
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}/`);
});
