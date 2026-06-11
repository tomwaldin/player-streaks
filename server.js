require('dotenv').config();
const express = require('express');
const cors = require('cors');
const https = require('https');
const { URL } = require('url');

const {
  DATABRICKS_HOST,
  WAREHOUSE_ID,
  DATABRICKS_TOKEN,
  CACHE_TTL_SECONDS,
  PORT = '3000'
} = process.env;

const QUERY = `
  SELECT *
  FROM ruby_sweeps.gold.user_streaks
  WHERE is_active = true
    AND date = (SELECT MAX(date) FROM ruby_sweeps.gold.user_streaks)
`;

const cacheTtlSeconds = Number(CACHE_TTL_SECONDS) || 60;
const MAX_POLL_TRIES = 60;
const POLL_INTERVAL_MS = 500;

const keepAliveAgent = new https.Agent({ keepAlive: true, maxSockets: 12 });
const inflightQueries = new Map();
const cache = new Map();

function validateConfig() {
  if (!DATABRICKS_HOST) throw new Error('Missing DATABRICKS_HOST');
  if (!WAREHOUSE_ID) throw new Error('Missing WAREHOUSE_ID');
  if (!DATABRICKS_TOKEN) throw new Error('Missing DATABRICKS_TOKEN');

  try {
    new URL(DATABRICKS_HOST);
  } catch (err) {
    throw new Error('DATABRICKS_HOST must be a valid URL');
  }
}

function databricksUrl(path) {
  const host = DATABRICKS_HOST.endsWith('/')
    ? DATABRICKS_HOST.slice(0, -1)
    : DATABRICKS_HOST;
  return `${host}${path}`;
}

async function parseBody(resp) {
  const text = await resp.text();
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function fetchWithAuth(url, options = {}) {
  const init = {
    ...options,
    headers: {
      ...(options.headers || {}),
      Authorization: `Bearer ${DATABRICKS_TOKEN}`,
    },
    agent: keepAliveAgent,
  };

  return fetch(url, init);
}

async function fetchJson(url, options = {}) {
  const resp = await fetchWithAuth(url, options);
  const body = await parseBody(resp);
  if (!resp.ok) {
    const message = typeof body === 'string' ? body : JSON.stringify(body);
    throw new Error(`Request failed ${resp.status} ${resp.statusText}: ${message}`);
  }
  return body;
}

async function submitStatement() {
  const url = databricksUrl('/api/2.0/sql/statements');
  const body = {
    statement: QUERY,
    warehouse_id: WAREHOUSE_ID,
    wait_timeout: '50s',
    on_wait_timeout: 'CANCEL',
  };

  const resp = await fetchWithAuth(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });

  const json = await parseBody(resp);
  if (!resp.ok) {
    const message = typeof json === 'string' ? json : JSON.stringify(json);
    throw new Error(`Submit failed ${resp.status} ${resp.statusText}: ${message}`);
  }

  if (!json?.statement_id) {
    throw new Error(`Missing statement_id in submit response: ${JSON.stringify(json)}`);
  }

  return json.statement_id;
}

async function fetchStatementStatus(statementId) {
  const url = databricksUrl(`/api/2.0/sql/statements/${encodeURIComponent(statementId)}`);
  return fetchJson(url, { method: 'GET' });
}

async function pollStatement(statementId) {
  for (let attempt = 1; attempt <= MAX_POLL_TRIES; attempt += 1) {
    const statusJson = await fetchStatementStatus(statementId);
    const state = statusJson?.status?.state;

    if (!state) {
      throw new Error(`Unexpected status response: ${JSON.stringify(statusJson)}`);
    }

    if (state === 'SUCCEEDED') {
      return statusJson;
    }

    if (state === 'FAILED') {
      const message = statusJson.status?.error?.message || JSON.stringify(statusJson.status?.error || 'Unknown error');
      throw new Error(`Databricks query failed: ${message}`);
    }

    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }

  throw new Error(`Databricks query timed out after ${MAX_POLL_TRIES * POLL_INTERVAL_MS}ms`);
}

function getResultData(json) {
  if (!json?.result?.data_array) {
    return [];
  }
  return json.result.data_array;
}

async function fetchChunkResult(statementId, chunkIndex) {
  const url = databricksUrl(`/api/2.0/sql/statements/${encodeURIComponent(statementId)}/result/${chunkIndex}`);
  return fetchJson(url, { method: 'GET' });
}

async function fetchChunkUrl(chunkUrl) {
  const resp = await fetchWithAuth(chunkUrl, { method: 'GET' });
  const json = await parseBody(resp);
  if (!resp.ok) {
    const message = typeof json === 'string' ? json : JSON.stringify(json);
    throw new Error(`Chunk fetch failed ${resp.status} ${resp.statusText}: ${message}`);
  }
  return json;
}

function makeSafeColumnName(column, index) {
  return column?.name || `col_${index}`;
}

function mapRowsToObjects(rows, columns) {
  return rows.map((row) => {
    const mapped = {};
    for (let i = 0; i < columns.length; i += 1) {
      mapped[makeSafeColumnName(columns[i], i)] = row[i];
    }
    return mapped;
  });
}

async function retrieveResult(statementId, statusJson) {
  const result = statusJson.result;
  if (!result) {
    throw new Error('Missing result object in status response');
  }

  const manifest = statusJson.manifest;
  const columns = manifest?.schema?.columns;
  if (!Array.isArray(columns) || columns.length === 0) {
    throw new Error('Result manifest does not contain schema columns');
  }

  const rows = Array.isArray(result.data_array) ? [...result.data_array] : [];
  const totalChunks = Number(manifest?.total_chunk_count || 1);

  if (totalChunks > 1) {
    if (Array.isArray(manifest?.chunks) && manifest.chunks.length > 1) {
      for (let idx = 1; idx < manifest.chunks.length; idx += 1) {
        const chunk = manifest.chunks[idx];
        if (!chunk?.url) {
          throw new Error(`Missing chunk URL in manifest chunk index ${idx}`);
        }
        const chunkJson = await fetchChunkUrl(chunk.url);
        rows.push(...getResultData(chunkJson));
      }
    } else {
      for (let chunkIndex = 1; chunkIndex < totalChunks; chunkIndex += 1) {
        const chunkJson = await fetchChunkResult(statementId, chunkIndex);
        rows.push(...getResultData(chunkJson));
      }
    }
  }

  return mapRowsToObjects(rows, columns);
}

async function executeQuery() {
  const now = Date.now();
  const cached = cache.get(QUERY);
  if (cached && cached.expiresAt > now) {
    return cached.rows;
  }

  if (inflightQueries.has(QUERY)) {
    return inflightQueries.get(QUERY);
  }

  const promise = (async () => {
    const statementId = await submitStatement();
    const statusJson = await pollStatement(statementId);
    const rows = await retrieveResult(statementId, statusJson);
    cache.set(QUERY, {
      expiresAt: Date.now() + cacheTtlSeconds * 1000,
      rows,
    });
    return rows;
  })();

  inflightQueries.set(QUERY, promise);

  try {
    return await promise;
  } finally {
    inflightQueries.delete(QUERY);
  }
}

validateConfig();

const app = express();
app.use(cors());
app.use(express.static('public'));

app.get('/leaderboard', async (req, res) => {
  try {
    const rows = await executeQuery();
    if (!Array.isArray(rows) || rows.length === 0) {
      return res.status(404).json({ error: 'No rows returned' });
    }
    return res.json({ rows });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return res.status(500).json({ error: message });
  }
});

app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

app.listen(Number(PORT), () => {
  console.log(`Server listening on port ${PORT}`);
});
