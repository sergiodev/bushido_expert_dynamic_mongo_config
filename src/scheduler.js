// src/scheduler.js
const cron = require('node-cron');
require('dotenv').config();
const { getDb, close } = require('./db');
const { runEtl } = require('./jobs/etl_hourly_configs');

// Qui definisci i JOB che vuoi far girare a mezzanotte.
// Puoi aggiungerne più di uno con parametri diversi.
const jobs = [
  {
    name: 'ETL-default',
    fn: runEtl,
    paramsEnv: true, // prendi i parametri da ENV .env
    params: null      // oppure passa un oggetto con override qui
  },
  // Esempio secondo job con parametri diversi:
  // {
  //   name: 'ETL-altro-periodo',
  //   fn: runEtl,
  //   paramsEnv: false,
  //   params: {
  //     RAW_COLLECTION: 'tracking',
  //     MAP_COLLECTION: 'exp_config_mapping',
  //     CONFIGS_DIR: './configs',
  //     START_ISO: '2025-11-01',
  //     END_ISO: '2025-11-05',
  //     MIN_TRADES: 5,
  //     LIFT_PP: 0.2,
  //     DRY_RUN: false,
  //     CONFIG_ID_MODE: 'account'
  //   }
  // }
];

async function runAllJobsOnce() {
  const db = await getDb();
  try {
    for (const job of jobs) {
      const params = job.paramsEnv
        ? {
            RAW_COLLECTION: process.env.RAW_COLLECTION,
            MAP_COLLECTION: process.env.MAP_COLLECTION,
            CONFIGS_DIR: process.env.CONFIGS_DIR,
            START_ISO: process.env.START_ISO,
            END_ISO: process.env.END_ISO,
            MIN_TRADES: parseInt(process.env.MIN_TRADES || '3', 10),
            LIFT_PP: parseFloat(process.env.LIFT_PP || '0.1'),
            DRY_RUN: process.env.DRY_RUN === '1',
            CONFIG_ID_MODE: process.env.CONFIG_ID_MODE || 'account',
            CONFIG_ID_ENV: process.env.CONFIG_ID_ENV
          }
        : job.params;

      console.log(`[Scheduler] Esecuzione job: ${job.name}`);
      await job.fn(db, params);
    }
  } catch (e) {
    console.error('[Scheduler] Errore in un job:', e);
  } finally {
    await close();
  }
}

// Pianifica: ogni giorno alle 00:00 Europe/Rome
cron.schedule('0 0 * * *', async () => {
  console.log(`[Scheduler] Tick: ${new Date().toISOString()} → Eseguo jobs (Europe/Rome)`);
  await runAllJobsOnce();
}, { timezone: 'Europe/Rome' });

// Avvio immediato una volta (utile al bootstrap)
runAllJobsOnce().then(() => {
  console.log('[Scheduler] Prima esecuzione completata, in attesa del prossimo cron (00:00 Europe/Rome).');
});
