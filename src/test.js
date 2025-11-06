require('dotenv').config();
const { MongoClient } = require('mongodb');

(async () => {
  const uri = process.env.MONGODB_URI;
  const dbName = process.env.MONGODB_DB_NAME;
  const collName = process.env.RAW_COLLECTION || 'tracking';

  if (!uri || !dbName) {
    console.error('❌ MONGODB_URI o MONGODB_DB_NAME mancanti in .env');
    process.exit(1);
  }

  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);
  const col = db.collection(collName);

  try {
    // 0) Totale
    const total = await col.estimatedDocumentCount();
    console.log(`✅ Connesso a ${dbName}.${collName}`);
    console.log(`📊 Documenti totali (stima): ${total}`);

    // 1) Tipo del campo ts (Date vs String) + min/max grezzi
    const tsTypeAgg = await col.aggregate([
      { $match: { ts: { $exists: true } } },
      { $project: { t: { $type: "$ts" }, ts: 1 } },
      { $group: {
          _id: "$t",
          min: { $min: "$ts" },
          max: { $max: "$ts" },
          n: { $sum: 1 }
      }},
      { $sort: { n: -1 } }
    ]).toArray();
    console.log("\n⏱️  ts: tipologia e range (senza filtri):");
    for (const r of tsTypeAgg) {
      console.log(`- type=${r._id}, count=${r.n}, min=${r.min}, max=${r.max}`);
    }

    // 2) Top 25 eventi (senza filtri tempo)
    const topEvents = await col.aggregate([
      { $group: { _id: "$event", n: { $sum: 1 } } },
      { $sort: { n: -1 } },
      { $limit: 25 }
    ]).toArray();
    console.log('\n🏷️  Top eventi (25):');
    for (const e of topEvents) console.log(`- ${e._id || '(null)'}: ${e.n}`);

    // 3) Conteggio campi candidati (senza filtri tempo)
    async function countField(field) {
      const r = await col.aggregate([
        { $match: { [field]: { $exists: true } } },
        { $group: { _id: null, n: { $sum: 1 } } }
      ]).toArray().catch(() => []);
      return r.length ? r[0].n : 0;
    }

    const accountFields = ['account_aleas','account','Account','acc','algo','config_snapshot.account_name','meta.account','details.account'];
    const outcomeFields = ['outcome','position_outcome','trade_outcome','result','status'];
    const pnlFields = ['net_delta','pnl','profit','pl'];

    console.log('\n👤 Campi account (conteggi esistenza):');
    for (const f of accountFields) {
      const n = await countField(f);
      if (n) console.log(`- ${f}: ${n}`);
    }

    console.log('\n🎯 Campi outcome (conteggi esistenza):');
    for (const f of outcomeFields) {
      const n = await countField(f);
      if (n) console.log(`- ${f}: ${n}`);
    }

    console.log('\n💰 Campi PnL (conteggi esistenza):');
    for (const f of pnlFields) {
      const n = await countField(f);
      if (n) console.log(`- ${f}: ${n}`);
    }

    // 4) 10 esempi che hanno outcome/pnl (senza filtri tempo)
    const examples = await col.aggregate([
      { $match: {
        $or: [
          ...outcomeFields.map(f => ({ [f]: { $exists: true } })),
          ...pnlFields.map(f => ({ [f]: { $exists: true } })),
        ]
      }},
      { $sort: { ts: -1 } }, // se ts è stringa ISO, l'ordinamento resta utile
      { $limit: 10 },
      { $project: {
          _id: 1, event: 1, ts: 1,
          account_aleas: 1, account: 1, acc: 1, algo: 1,
          "config_snapshot.account_name": 1,
          outcome: 1, position_outcome: 1, trade_outcome: 1, result: 1, status: 1,
          net_delta: 1, pnl: 1, profit: 1, pl: 1,
          state: 1, side: 1
      }}
    ]).toArray();

    console.log('\n🔎 10 esempi con outcome/pnl (senza filtri tempo):');
    console.log(JSON.stringify(examples, null, 2));

  } catch (err) {
    console.error('❌ Errore diagnosi:', err);
  } finally {
    await client.close();
  }
})();
