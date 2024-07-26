const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Pool } = require('pg');
const NodeCache = require('node-cache');

const app = express();
const pool = new Pool({
  user: 'admin',
  host: 'localhost',
  database: 'postgres',
  password: 'kyler',
  port: 5433,
});

const myCache = new NodeCache();

app.use(bodyParser.json());
app.use(cors());

const validateSelectQuery = (req, res, next) => {
  const { sql } = req.body;
  console.log(`A request has been received`);

  if (!sql) {
    return res.status(400).send('SQL query is required');
  }

  const trimmedSql = sql.trim().toUpperCase();

  if (!trimmedSql.startsWith('SELECT')) {
    return res.status(400).send('Only SELECT statements are allowed');
  }

  const forbiddenClauses = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'TRUNCATE', 'EXECUTE'];
  const hasForbiddenClauses = forbiddenClauses.some(clause => trimmedSql.includes(clause));
  if (hasForbiddenClauses) {
    return res.status(400).send('Forbidden SQL operation detected');
  }

  next();
};

const buildLineQuery = (filters, startDate, endDate) => {
  const { cities, countries, isps } = filters;

  let selectFields = ["TO_CHAR(DATE_TRUNC('day', x.date), 'YYYY-MM-DD') AS date", "AVG(x.meanthroughputmbps) AS download","avg(x.minrtt) AS Latency","(avg(x.lossrate) * 100) as Lossrate" ];
  let groupByFields = ["date"];
  let orderByFields = ["date"];

  if (cities && cities.length > 0) {
    selectFields.push("y.city");
    groupByFields.push("y.city");
    orderByFields.push("y.city");
  }

  if (countries && countries.length > 0) {
    selectFields.push("y.countrycode");
    groupByFields.push("y.countrycode");
    orderByFields.push("y.countrycode");
  }

  if (isps && isps.length > 0) {
    selectFields.push("y.isp");
    groupByFields.push("y.isp");
    orderByFields.push("y.isp");
  }

  let query = `
    SELECT ${selectFields.join(", ")}
    FROM download x
    JOIN descriptors y ON x.descriptorid = y.id
    WHERE 1=1
  `;

  if (cities && cities.length > 0) {
    const cityList = cities.map(city => `'${city}'`).join(',');
    query += ` AND y.city IN (${cityList})`;
  }

  if (countries && countries.length > 0) {
    const countryList = countries.map(country => `'${country}'`).join(',');
    query += ` AND y.countrycode IN (${countryList})`;
  }

  if (isps && isps.length > 0) {
    const ispList = isps.map(isp => `'${isp}'`).join(',');
    query += ` AND y.isp IN (${ispList})`;
  }

  if (startDate && endDate) {
    query += ` AND x.date BETWEEN '${startDate}' AND '${endDate}'`;
  }

  query += `
    GROUP BY ${groupByFields.join(", ")}
    ORDER BY ${orderByFields.join(", ")}
  `;

  return query;
};

app.post('/query/line', async (req, res) => {
  const { filters, startDate, endDate } = req.body;
  if (!filters || (!filters.cities && !filters.countries && !filters.isps)) {
    return res.status(400).send('At least one filter is required');
  }

  const sql = buildLineQuery(filters, startDate, endDate);
  req.body.sql = sql;
  validateSelectQuery(req, res, () => executeQuery(sql, res));
});


const buildBarQuery = (filters, startDate, endDate) => {
  const { cities, countries, isps } = filters;
  let groupByFields = [];

  let query = `select AVG(x.meanthroughputmbps) as Download`

if (cities && cities.length > 0) {
      query += `,y.city`;
    }
  
    if (countries && countries.length > 0) {
      query += `,y.countrycode`;
    }
  
    if (isps && isps.length > 0) {
      query += `,y.isp`;
    } 
    
    query += ` FROM download x
    JOIN descriptors y ON x.descriptorid = y.id
    WHERE 1=1`;

  if (cities && cities.length > 0) {
    groupByFields.push("y.city");
    const cityList = cities.map(city => `'${city}'`).join(',');
    query += ` AND y.city IN (${cityList})`;
  }

  if (countries && countries.length > 0) {
    groupByFields.push("y.countrycode");
    const countryList = countries.map(country => `'${country}'`).join(',');
    query += ` AND y.countrycode IN (${countryList})`;
  }

  if (isps && isps.length > 0) {
    groupByFields.push("y.isp");
    const ispList = isps.map(isp => `'${isp}'`).join(',');
    query += ` AND y.isp IN (${ispList})`;
  }

  if (startDate && endDate) {
    query += ` AND x.date BETWEEN '${startDate}' AND '${endDate}'`;
  }

  
  query += `
  GROUP BY ${groupByFields.join(", ")}`
    
    query +=`;`

  return query;
};

const buildPieQuery = (filters, startDate, endDate) => {
  const { cities, countries, isps } = filters;
  let groupByFields = [];

  const cityList = cities.map(city => `'${city}'`).join(',');

  let query = `select y.city,
  COUNT(*) AS group_count,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS percentage_of_total
FROM
  download x 
join descriptors y on x.descriptorid = y.id 
where y.city IN (${cityList})
GROUP BY
  y.city 
ORDER BY
  percentage_of_total DESC;`

  return query
};

const buildOtherQuery3 = (params) => {
  // Build the SQL query for the third additional query type
  return 'SELECT * FROM ...';
};

const executeQuery = async (sql, res) => {
  const cacheKey = `query_${Buffer.from(sql).toString('base64')}`;
  const cachedData = myCache.get(cacheKey);

  if (cachedData) {
    return res.status(200).json(cachedData);
  }

  try {
    const result = await pool.query(sql);
    myCache.set(cacheKey, result.rows, 3600);
    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Error executing query:', err);
    res.status(500).send('Server error');
  }
};


// Placeholder endpoints for other query types
app.post('/query/bar', async (req, res) => {
  const { filters, startDate, endDate } = req.body;
  if (!filters || (!filters.cities && !filters.countries && !filters.isps)) {
    return res.status(400).send('At least one filter is required');
  }

  const sql = buildBarQuery(filters, startDate, endDate);
  req.body.sql = sql;
  validateSelectQuery(req, res, () => executeQuery(sql, res));
});

app.post('/query/pie', async (req, res) => {
  const { filters, startDate, endDate } = req.body;
  if (!filters || (!filters.cities && !filters.countries && !filters.isps)) {
    return res.status(400).send('At least one filter is required');
  }

  const sql = buildPieQuery(filters, startDate, endDate);
  req.body.sql = sql;
  validateSelectQuery(req, res, () => executeQuery(sql, res));
});

app.post('/query/type3', async (req, res) => {
  const params = req.body; // Adjust this to capture necessary parameters
  const sql = buildOtherQuery3(params);
  req.body.sql = sql;
  validateSelectQuery(req, res, () => executeQuery(sql, res));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server is running on port ${PORT}`);
});
