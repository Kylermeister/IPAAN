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

  
  const forbiddenClauses = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'TRUNCATE', 'EXECUTE'];
  const hasForbiddenClauses = forbiddenClauses.some(clause => trimmedSql.includes(clause));
  if (hasForbiddenClauses) {
    return res.status(400).send('Forbidden SQL operation detected');
  }

  next();
};

const buildLineQuery = (filters, startDate, endDate) => {
  const { cities, countries, isps } = filters;

  let selectFields = [
    "TO_CHAR(DATE_TRUNC('day', x.date), 'YYYY-MM-DD') AS date",
    "AVG(x.meanthroughputmbps) AS Speed",
    "avg(x.minrtt) AS Latency"
  ];
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

  // Construct GROUP BY clause dynamically
  const groupByClause = groupByFields.join(", ");

  // Main query for download data
  let query = `
    WITH download_data AS (
      SELECT
        ${selectFields.join(", ")},
        (avg(x.lossrate) * 100) as Lossrate
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
      GROUP BY ${groupByClause}
    ),

    upload_data AS (
      SELECT
      ${selectFields.join(", ")}
      FROM upload x
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
      GROUP BY ${groupByClause}
    )

    SELECT
      y.date,
      `

      if (cities && cities.length > 0) {
        query += ` y.city,
        `;
      }
    
      if (countries && countries.length > 0) {
        query += ` y.countrycode,
        `;
      }
    
      if (isps && isps.length > 0) {
        query += ` y.isp,
        `;
      }

      query +=`y.Speed as Download,
      u.speed AS Upload,
      y.Latency,
      y.Lossrate
    FROM download_data y
    LEFT JOIN upload_data u ON y.date = u.date `

    if (cities && cities.length > 0) {
      query += `AND y.city = u.city
      `;
    }
  
    if (countries && countries.length > 0) {
      query += ` And y.countrycode = u.countrycode
      `;
    }
  
    if (isps && isps.length > 0) {
      query += ` AND y.isp = u.isp 
      `;
    }

    query += `ORDER BY ${orderByFields.join(", ")};
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
  let selectFields = ["AVG(d.meanthroughputmbps) AS download"];
  let groupByFields = [];
  let joinConditions = [];
  let whereConditions = ["1=1"];
  
  if (cities && cities.length > 0) {
    selectFields.push("y.city");
    groupByFields.push("y.city");
    joinConditions.push("y.city");
    const cityList = cities.map(city => `'${city}'`).join(',');
    whereConditions.push(`y.city IN (${cityList})`);
  }
  
  if (countries && countries.length > 0) {
    selectFields.push("y.countrycode");
    groupByFields.push("y.countrycode");
    joinConditions.push("y.countrycode");
    const countryList = countries.map(country => `'${country}'`).join(',');
    whereConditions.push(`y.countrycode IN (${countryList})`);
  }
  
  if (isps && isps.length > 0) {
    selectFields.push("y.isp");
    groupByFields.push("y.isp");
    joinConditions.push("y.isp");
    const ispList = isps.map(isp => `'${isp}'`).join(',');
    whereConditions.push(`y.isp IN (${ispList})`);
  }
  
  if (startDate && endDate) {
    whereConditions.push(`d.date BETWEEN '${startDate}' AND '${endDate}'`);
  }
  
  const groupByClause = groupByFields.join(", ");
  const joinClause = joinConditions.length > 0 ? joinConditions.join(" AND ") : "1=1";

  let query = `
    WITH download_data AS (
      SELECT ${selectFields.join(", ")}
      FROM download d
      JOIN descriptors y ON d.descriptorid = y.id
      WHERE ${whereConditions.join(" AND ")}
      GROUP BY ${groupByClause}
    ),
    upload_data AS (
      SELECT AVG(u.meanthroughputmbps) AS upload, ${groupByFields.join(", ")}
      FROM upload u
      JOIN descriptors y ON u.descriptorid = y.id
      WHERE ${whereConditions.join(" AND ").replace(/d.date/g, 'u.date')}
      GROUP BY ${groupByClause}
    )
    SELECT
      ${groupByFields.join(",").replace(/y.city/g, 'd.city')},
      d.download,
      y.upload
    FROM download_data d
    LEFT JOIN upload_data y ON 1=1 `

    if (cities && cities.length > 0) {
      query += `AND y.city = d.city
      `;
    }
  
    if (countries && countries.length > 0) {
      query += ` And y.countrycode = d.countrycode
      `;
    }
  
    if (isps && isps.length > 0) {
      query += ` AND y.isp = d.isp 
      `;
    }

    query +=  ` ORDER BY ${groupByFields.join(", ")};
  ;`
  
  return query;
};


const buildPieQuery = (filters, startDate, endDate) => {
  const { cities, countries, isps } = filters;

  const cityList = cities.map(city => `'${city}'`).join(',');

  let query = `select y.city,
  COUNT(*) AS group_count,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) AS percentage_of_total
FROM
  download x 
join descriptors y on x.descriptorid = y.id 
where y.city IN (${cityList})
AND x.date BETWEEN '${startDate}' AND '${endDate}'
GROUP BY
  y.city 
ORDER BY
  percentage_of_total DESC;`

  return query
};

const buildMapQuery = (filters, startDate, endDate) => {
  const { cities, countries, isps } = filters;

  let query = `SELECT json_build_array(
  ST_Y(ST_Transform(x.geom, 4326)),
  ST_X(ST_Transform(x.geom, 4326)),
  x.meanthroughputmbps)::text AS formatted_result
FROM
  download x
  JOIN descriptors y ON x.descriptorid = y.id
where 1=1`
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

query += ` AND x.date BETWEEN '${startDate}' AND '${endDate}'
 limit (10000)`

  return query;
};

const executeQuery = async (sql, res) => {
  const cacheKey = `query_${Buffer.from(sql).toString('base64')}`;
  const cachedData = myCache.get(cacheKey);

  if (cachedData) {
    return res.status(200).json(cachedData);
  }

  try {
    //console.log(sql)
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

app.post('/query/map', async (req, res) => {
  const { filters, startDate, endDate } = req.body;
  if (!filters || (!filters.cities && !filters.countries && !filters.isps)) {
    return res.status(400).send('At least one filter is required');
  }

  const sql = buildMapQuery(filters, startDate, endDate);
  req.body.sql = sql;
  validateSelectQuery(req, res, () => executeMapQuery(sql, res));
});

const executeMapQuery = async (sql, res) => {
  const cacheKey = `query_${Buffer.from(sql).toString('base64')}`;
  const cachedData = myCache.get(cacheKey);

  if (cachedData) {
    return res.status(200).json(cachedData);
  }

  try {
    //console.log(sql)
    const result = await pool.query(sql);
    const transformedResults = result.rows.map(row => JSON.parse(row.formatted_result));
    myCache.set(cacheKey, transformedResults, 3600);
    res.status(200).json(transformedResults);

  } catch (err) {
    console.error('Error executing query:', err);
    res.status(500).send('Server error');
  }
};

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server is running on port ${PORT}`);
});
