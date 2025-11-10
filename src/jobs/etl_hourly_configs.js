// src/jobs/etl_hourly_configs.js
const fs = require('fs');
const path = require('path');
require('dotenv').config();

/* ------------------------- helpers: config validators ---------------------- */
function normalizeExpConfig(cfg) {
  if (!cfg || typeof cfg !== 'object' || Array.isArray(cfg)) {
    throw new Error('exp_config non valido: root deve essere un oggetto.');
  }

  const required = [
    'training_params',
    'trade_params',
    'open_position_params',
    'close_position_params',
    'lifecycle_params',
    'symbol_overrides',
    'tracking'
  ];
  for (const k of required) {
    if (!(k in cfg)) throw new Error(`exp_config non valido: manca la chiave "${k}"`);
    if (typeof cfg[k] !== 'object' || Array.isArray(cfg[k])) {
      throw new Error(`exp_config non valido: "${k}" deve essere un oggetto.`);
    }
  }

  const ni = v => (v == null ? v : parseInt(v, 10));
  const nf = v => (v == null ? v : parseFloat(v));

  if (cfg.training_params) {
    cfg.training_params.limit = ni(cfg.training_params.limit);
    cfg.training_params.tolerance = nf(cfg.training_params.tolerance);
    cfg.training_params.horizon = ni(cfg.training_params.horizon);
  }
  if (cfg.trade_params) {
    cfg.trade_params.commissions = nf(cfg.trade_params.commissions);
  }
  if (cfg.open_position_params) {
    cfg.open_position_params.weight_greater_than = nf(cfg.open_position_params.weight_greater_than);
    cfg.open_position_params.rockets_algs_with_weight_grater_than = nf(cfg.open_position_params.rockets_algs_with_weight_grater_than);
    cfg.open_position_params.volatility_threshold = nf(cfg.open_position_params.volatility_threshold);
    cfg.open_position_params.min_winning_label_weigh = nf(cfg.open_position_params.min_winning_label_weigh);
  }
  if (cfg.close_position_params) {
    const c = cfg.close_position_params;
    c.consecutive_decreases_threshold = ni(c.consecutive_decreases_threshold);
    c.sudden_changes_threshold = nf(c.sudden_changes_threshold);
    c.take_profit_highest_abs_value = nf(c.take_profit_highest_abs_value);
    c.stop_loss_highest_abs_value = nf(c.stop_loss_highest_abs_value);
    c.take_profit_abs_value = nf(c.take_profit_abs_value);
    c.stop_loss_abs_value = nf(c.stop_loss_abs_value);
  }
  if (cfg.lifecycle_params) {
    const l = cfg.lifecycle_params;
    l.positions_in_the_lifecycle = ni(l.positions_in_the_lifecycle);
    l.net_delta__and_current_rate_job_interval = ni(l.net_delta__and_current_rate_job_interval);
    l.leaderboard_and_open_pos_job_interval = ni(l.leaderboard_and_open_pos_job_interval);
    l.opened_positions_monitoring_job_interval = ni(l.opened_positions_monitoring_job_interval);
    l.opened_positions_high_frequency_monitoring_job_interval = ni(l.opened_positions_high_frequency_monitoring_job_interval);
    l.freeze_after_loss = ni(l.freeze_after_loss);
    l.post_freeze_grace_ops = ni(l.post_freeze_grace_ops);
  }
  if (cfg.tracking) {
    const t = cfg.tracking;
    t.min_abs_net_delta = nf(t.min_abs_net_delta);
    t.min_rate_rel_change = nf(t.min_rate_rel_change);
    t.dedupe_window_seconds = ni(t.dedupe_window_seconds);
  }

  delete cfg.use_mongo_config;
  delete cfg.mongo_config_id;

  return cfg;
}

/* ------------------------------ readers ----------------------------------- */
function readExpConfigForAccount(account, configsDir) {
  const safe = String(account).trim();
  const absDir = path.resolve(process.cwd(), configsDir || './configs');
  const f = path.join(absDir, `${safe}.json`);
  if (!fs.existsSync(f)) {
    throw new Error(`Config file non trovato per account: ${safe} (${f})`);
  }
  const data = JSON.parse(fs.readFileSync(f, 'utf-8'));
  return normalizeExpConfig(data);
}

function readExpConfigSingle(singleFilePath) {
  const abs = path.resolve(process.cwd(), singleFilePath);
  if (!fs.existsSync(abs)) {
    throw new Error(`Config file generale non trovato: ${abs}`);
  }
  const data = JSON.parse(fs.readFileSync(abs, 'utf-8'));
  return normalizeExpConfig(data);
}

function pickConfigId(account, mode, envId) {
  if (mode === 'env') {
    if (!envId) throw new Error('CONFIG_ID_MODE=env ma CONFIG_ID_ENV non è impostato');
    return envId;
  }
  return account;
}

/* ------------------------------ main ETL ---------------------------------- */
/**
 * Esegue l’ETL:
 *  - db: istanza Mongo (db = await getDb())
 *  - params: opzioni
 */
async function runEtl(db, params) {
  const {
    RAW_COLLECTION,
    MAP_COLLECTION = 'exp_config_mapping',
    CONFIGS_DIR = './configs',
    START_ISO,
    END_ISO,
    MIN_TRADES = 3,
    LIFT_PP = 0.1,
    MIN_EXP_VALUE = (process.env.MIN_EXP_VALUE != null ? parseFloat(process.env.MIN_EXP_VALUE) : undefined),
    DRY_RUN = false,
    CONFIG_ID_MODE = 'account',
    CONFIG_ID_ENV
  } = params;

  if (!RAW_COLLECTION) throw new Error('RAW_COLLECTION mancante');

  const raw = db.collection(RAW_COLLECTION);

  // filtro per data (START/END come YYYY-MM-DD)
  const matchStage = {};
  if (START_ISO) matchStage.ts = { ...(matchStage.ts || {}), $gte: new Date(START_ISO) };
  if (END_ISO)   matchStage.ts = { ...(matchStage.ts || {}), $lt:  new Date(END_ISO) };

  const pipeline = [
    ...(Object.keys(matchStage).length ? [{ $match: matchStage }] : []),

    // 1) normalizza campi dataset
    {
      $addFields: {
        _ts: "$ts",                   // Date
        _account: "$account_aleas",   // nome account
        _outcome_raw: { $ifNull: ["$outcome", null] },
        _net_delta_raw: { $ifNull: ["$net_delta", null] }
      }
    },

    // 2) ora locale + cast net_delta
    {
      $addFields: {
        hour: { $toInt: { $dateToString: { date: "$_ts", timezone: "Europe/Rome", format: "%H" } } },
        net_delta_num: { $toDouble: "$_net_delta_raw" }
      }
    },

    // 3) outcome: usa outcome se presente, altrimenti derivato dal segno di net_delta
    {
      $addFields: {
        outcome_norm: {
          $switch: {
            branches: [
              { case: { $in: [{ $toLower: "$_outcome_raw" }, ["win","won","success","vittoria"]] }, then: "win" },
              { case: { $in: [{ $toLower: "$_outcome_raw" }, ["loss","lost","fail","sconfitta"]] }, then: "loss" }
            ],
            default: {
              $cond: [
                { $and: [{ $ne: ["$net_delta_num", null] }, { $gt: ["$net_delta_num", 0] }] },
                "win",
                {
                  $cond: [
                    { $and: [{ $ne: ["$net_delta_num", null] }, { $lt: ["$net_delta_num", 0] }] },
                    "loss",
                    null
                  ]
                }
              ]
            }
          }
        }
      }
    },

    // 4) filtra righe utili
    {
      $match: {
        _ts: { $ne: null },
        _account: { $ne: null },
        hour: { $gte: 0, $lte: 23 },
        net_delta_num: { $ne: null },
        outcome_norm: { $in: ["win", "loss"] }
      }
    },

    // 5) aggrega per (account, hour)
    {
      $group: {
        _id: { account: "$_account", hour: "$hour" },
        total:  { $sum: 1 },
        wins:   { $sum: { $cond: [{ $eq: ["$outcome_norm", "win"] }, 1, 0] } },
        losses: { $sum: { $cond: [{ $eq: ["$outcome_norm", "loss"] }, 1, 0] } },
        avg_win:  { $avg: { $cond: [{ $eq: ["$outcome_norm", "win"] }, "$net_delta_num", null] } },
        avg_loss: { $avg: { $cond: [{ $eq: ["$outcome_norm", "loss"] }, "$net_delta_num", null] } }
      }
    },

    // 6) metriche finali per riga
    {
      $addFields: {
        account: "$_id.account",
        hour: "$_id.hour",
        win_rate: {
          $cond: [
            { $gt: ["$total", 0] },
            { $divide: ["$wins", "$total"] },
            null
          ]
        }
      }
    },

    // 7) exp_value = wr*avg_win + (1-wr)*avg_loss
    {
      $addFields: {
        exp_value: {
          $cond: [
            { $and: [{ $ne: ["$win_rate", null] }, { $ne: ["$avg_win", null] }, { $ne: ["$avg_loss", null] }] },
            { $add: [
              { $multiply: ["$win_rate", "$avg_win"] },
              { $multiply: [{ $subtract: [1, "$win_rate"] }, "$avg_loss"] }
            ]},
            null
          ]
        }
      }
    },

    { $project: { _id: 0, account: 1, hour: 1, total: 1, wins: 1, losses: 1, win_rate: 1, avg_win: 1, avg_loss: 1, exp_value: 1 } }
  ];

  const perAccHour = await raw.aggregate(pipeline).toArray();

  if (!perAccHour.length) {
    console.warn('[ETL] Nessun dato grezzo nel periodo.');
    const accs = [...new Set((perAccHour || []).map(r => r.account))];
    console.log(`[ETL] Accounts visti nei grezzi: ${accs.length ? accs.join(', ') : '(nessuno)'}`);
    const cfgDir = CONFIGS_DIR || './configs';
    const missing = [];
    const present = [];
    for (const a of accs) {
      const f = path.join(cfgDir, `${a}.json`);
      (fs.existsSync(f) ? present : missing).push(f);
    }
    if (present.length) console.log('[ETL] File config presenti:', present);
    if (missing.length) console.log('[ETL] File config MANCANTI:', missing);
    return;
  }

  // baseline ponderata per ora e selezione migliore
  const byHour = new Map();
  for (const r of perAccHour) {
    if (!byHour.has(r.hour)) byHour.set(r.hour, []);
    byHour.get(r.hour).push(r);
  }

  const picks = [];
  const LIFT_PP_NUM = parseFloat(LIFT_PP);
  const MIN_TRADES_NUM = parseInt(MIN_TRADES, 10);
  const MIN_EXP_VALUE_NUM = (MIN_EXP_VALUE != null && !Number.isNaN(MIN_EXP_VALUE)) ? MIN_EXP_VALUE : undefined;

  for (const [hour, rows] of byHour.entries()) {
    let sumWr = 0, sumN = 0;
    for (const r of rows) {
      if (r.win_rate != null && r.total > 0) {
        sumWr += r.win_rate * r.total;
        sumN  += r.total;
      }
    }
    const baseline = sumN > 0 ? (sumWr / sumN) : null;

    const eligible = rows.filter(r =>
      r.total >= MIN_TRADES_NUM &&
      r.exp_value != null &&
      (MIN_EXP_VALUE_NUM == null || r.exp_value >= MIN_EXP_VALUE_NUM) &&
      (baseline == null || ((r.win_rate - baseline) * 100) >= LIFT_PP_NUM)
    );
    if (!eligible.length) continue;

    eligible.sort((a, b) => (b.exp_value ?? -Infinity) - (a.exp_value ?? -Infinity));
    const best = eligible[0];
    picks.push({
      hour,
      account: best.account,
      exp_value: best.exp_value,
      win_rate: best.win_rate,
      lift_pp: baseline == null ? null : ((best.win_rate - baseline) * 100)
    });
  }

  if (!picks.length) {
    console.warn('[ETL] Nessuna ora soddisfa i criteri (MIN_TRADES/LIFT_PP/MIN_EXP_VALUE/exp_value).');
    const accs = [...new Set(perAccHour.map(r => r.account))];
    console.log(`[ETL] Accounts analizzati: ${accs.length ? accs.join(', ') : '(nessuno)'}`);

    const cfgDir = CONFIGS_DIR || './configs';
    const missing = [];
    const present = [];
    for (const a of accs) {
      const f = path.join(cfgDir, `${a}.json`);
      (fs.existsSync(f) ? present : missing).push(f);
    }
    if (present.length) console.log('[ETL] File config presenti:', present);
    if (missing.length) console.log('[ETL] File config MANCANTI:', missing);
    console.log('[ETL] Suggerimento: per test rapidi prova MIN_TRADES=1 e/o LIFT_PP=0');
    return;
  }

  /* -------- filtro picks: CONFIGS_MODE=dir → accetta solo account con file presente -------- */
  const mode = (process.env.CONFIGS_MODE || (CONFIGS_DIR ? 'dir' : 'single')).toLowerCase();
  const cfgDir = process.env.CONFIGS_DIR || CONFIGS_DIR || './configs';

  // opzionale: blacklist via .env
  const excluded = new Set(
    (process.env.EXCLUDE_ACCOUNTS || '')
      .split(',')
      .map(s => s.trim())
      .filter(Boolean)
  );

  const safePicks = picks.filter(p => {
    if (excluded.has(p.account)) {
      console.warn(`[ETL] h${String(p.hour).padStart(2,'0')}: skip ${p.account} → EXCLUDE_ACCOUNTS`);
      return false;
    }
    if (mode === 'dir') {
      const f = path.join(cfgDir, `${String(p.account).trim()}.json`);
      if (!fs.existsSync(f)) {
        console.warn(`[ETL] h${String(p.hour).padStart(2,'0')}: skip ${p.account} → file mancante (${f})`);
        return false;
      }
    }
    return true;
  });

  if (!safePicks.length) {
    console.warn('[ETL] Tutti i picks sono stati scartati (file mancanti/blacklist). Nessun upsert.');
    return;
  }

  /* ------------------------------ scrittura mapping ------------------------- */
  const now = new Date();
  const singleFile = process.env.CONFIGS_SINGLE_FILE;
  let singleCfg = null;
  if (mode === 'single') {
    singleCfg = readExpConfigSingle(singleFile || './configs/exp_config_generale.json');
  }

  const ops = [];
  for (const p of safePicks) {
    const expConfig = (mode === 'single')
      ? singleCfg
      : readExpConfigForAccount(p.account, cfgDir);

    const config_id = pickConfigId(p.account, CONFIG_ID_MODE, CONFIG_ID_ENV);

    ops.push({
      updateOne: {
        filter: { hour: p.hour, config_id },
        update: {
          $set: {
            hour: p.hour,
            config_id,
            active: true,
            priority: 0,
            updated_at: now,
            exp_config: expConfig
          }
        },
        upsert: true
      }
    });
  }

  console.log('[ETL] Selezioni (hour → account):');
  for (const p of safePicks.sort((a, b) => a.hour - b.hour)) {
    console.log(`  h${String(p.hour).padStart(2, '0')}: ${p.account} (exp=${p.exp_value?.toFixed(2) ?? 'n/d'}, lift=${p.lift_pp?.toFixed(1) ?? 'n/d'} pp)`);
  }

  if (DRY_RUN) {
    console.log('[ETL] DRY_RUN=1 → nessun write eseguito.');
  } else {
    const map = db.collection(MAP_COLLECTION);
    const res = await map.bulkWrite(ops, { ordered: false });
    console.log(`[ETL] Upsert completato. Matched=${res.matchedCount} Modified=${res.modifiedCount} Upserted=${res.upsertedCount}`);
  }
}

/* ---------------------------- CLI runner (ENV) ----------------------------- */
async function runEtlFromEnv() {
  const { getDb, close } = require('../db');
  const db = await getDb();

  try {
    // finestra automatica da DAYS_BACK (se END/START non forniti)
    let END_ISO = process.env.END_ISO;
    let START_ISO = process.env.START_ISO;
    const DAYS_BACK = parseInt(process.env.DAYS_BACK || '0', 10);

    if (!END_ISO && DAYS_BACK > 0) {
      const now = new Date();
      const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
      const start = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate() - DAYS_BACK + 1));
      END_ISO = end.toISOString().slice(0, 10);
      START_ISO = start.toISOString().slice(0, 10);
    }

    console.log(`[ETL] Periodo analizzato: ${START_ISO || '(default)'} → ${END_ISO || '(default)'}`);

    await runEtl(db, {
      RAW_COLLECTION: process.env.RAW_COLLECTION,
      MAP_COLLECTION: process.env.MAP_COLLECTION,
      CONFIGS_DIR: process.env.CONFIGS_DIR,
      START_ISO,
      END_ISO,
      MIN_TRADES: parseInt(process.env.MIN_TRADES || '3', 10),
      LIFT_PP: parseFloat(process.env.LIFT_PP || '0.1'),
      MIN_EXP_VALUE: (process.env.MIN_EXP_VALUE != null ? parseFloat(process.env.MIN_EXP_VALUE) : undefined),
      DRY_RUN: process.env.DRY_RUN === '1',
      CONFIG_ID_MODE: process.env.CONFIG_ID_MODE || 'account',
      CONFIG_ID_ENV: process.env.CONFIG_ID_ENV
    });
  } finally {
    await close();
  }
}

module.exports = { runEtl, runEtlFromEnv };
