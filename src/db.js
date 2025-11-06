// src/db.js
const { MongoClient } = require('mongodb');
const fs = require('fs');

require('dotenv').config();

function buildMongoOptions() {
  const opts = {
    appName: process.env.MONGODB_APPNAME || 'etl-scheduler',
    serverSelectionTimeoutMS: parseInt(process.env.MONGODB_TIMEOUT_MS || '10000', 10),
  };
  // tlsCAFile opzionale (replica del tuo mongo_config.py)
  if (process.env.MONGODB_TLS_CA_FILE) {
    const caPath = process.env.MONGODB_TLS_CA_FILE;
    if (fs.existsSync(caPath)) {
      opts.tlsCAFile = caPath;
    }
  }
  return opts;
}

let _client = null;

async function getDb() {
  if (!_client) {
    if (!process.env.MONGODB_URI) {
      throw new Error('MONGODB_URI mancante (imposta in .env).');
    }
    if (!process.env.MONGODB_DB_NAME) {
      throw new Error('MONGODB_DB_NAME mancante (imposta in .env).');
    }

    const options = buildMongoOptions();
    _client = new MongoClient(process.env.MONGODB_URI, options);
    await _client.connect();
    // ping di validazione
    await _client.db().admin().command({ ping: 1 });
  }
  return _client.db(process.env.MONGODB_DB_NAME);
}

async function close() {
  if (_client) {
    await _client.close();
    _client = null;
  }
}

module.exports = { getDb, close };
