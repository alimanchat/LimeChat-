const express = require("express");
const http = require("http");
const { Pool } = require("pg");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("DATABASE_URL жок! Railway > LimeChat- > Variables ичине DATABASE_URL = ${{Postgres.DATABASE_URL}} кош.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("railway.internal") ? false : { rejectUnauthorized: false },
  max: 10
});
pool.on("error", e => console.error("PG pool error:", e.message));

// ---------------------------------------------------------------
// База: бир таблица, ар бир "жалбырак" (leaf) өз жолу менен сакталат
// chats/abc/messages/KEY/text -> "салам"
// ---------------------------------------------------------------
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS nodes (
      path TEXT COLLATE "C" PRIMARY KEY,
      val  JSONB NOT NULL
    )
  `);
}

function normPath(p) {
  return String(p || "").split("/").filter(Boolean).join("/");
}

function isSV(v) {
  return v && typeof v === "object" && v[".sv"] === "timestamp";
}

// JSON дарагын жалбырактарга жайуу
function flatten(val, base, out, now) {
  if (val === null || val === undefined) return;
  if (isSV(val)) { out.push([base, now]); return; }
  const t = typeof val;
  if (t === "string" || t === "boolean") { out.push([base, val]); return; }
  if (t === "number") { if (isFinite(val)) out.push([base, val]); return; }
  if (t === "object") {
    for (const k of Object.keys(val)) {
      if (!k || k.indexOf("/") !== -1) continue;
      flatten(val[k], base ? base + "/" + k : k, out, now);
    }
  }
}

// Жалбырактардан дарак куруу
function buildTree(rows, base) {
  if (!rows.length) return null;
  const cut = base ? base.length + 1 : 0;
  let root = null;
  for (const r of rows) {
    if (r.path === base) return r.val;
    const parts = r.path.slice(cut).split("/");
    if (root === null) root = {};
    let o = root;
    for (let i = 0; i < parts.length - 1; i++) {
      if (typeof o[parts[i]] !== "object" || o[parts[i]] === null) o[parts[i]] = {};
      o = o[parts[i]];
    }
    o[parts[parts.length - 1]] = r.val;
  }
  return root;
}

// p жолундагы бардык жазуулар: path = p ЖЕ path p/... менен башталат
const RANGE = `(path = $1 OR (path >= ($1 || '/') AND path < ($1 || '0')))`;

async function childKeys(p, limit) {
  const pre = p ? p + "/" : "";
  let sql, params;
  if (p) {
    sql = `SELECT k FROM (
             SELECT DISTINCT split_part(substr(path, $2), '/', 1) AS k
             FROM nodes WHERE path >= $1 AND path < $3
           ) t ORDER BY k COLLATE "C" DESC` + (limit ? " LIMIT $4" : "");
    params = [pre, pre.length + 1, p + "0"];
  } else {
    sql = `SELECT k FROM (
             SELECT DISTINCT split_part(path, '/', 1) AS k FROM nodes
           ) t ORDER BY k COLLATE "C" DESC` + (limit ? " LIMIT $1" : "");
    params = [];
  }
  if (limit) params.push(limit);
  const r = await pool.query(sql, params);
  return r.rows.map(x => x.k).filter(Boolean).reverse(); // өсүү тартибинде
}

async function getValue(p, limit) {
  if (!limit) {
    const r = p
      ? await pool.query(`SELECT path, val FROM nodes WHERE ${RANGE} ORDER BY path COLLATE "C"`, [p])
      : await pool.query(`SELECT path, val FROM nodes ORDER BY path COLLATE "C"`);
    return buildTree(r.rows, p);
  }
  const keys = await childKeys(p, limit);
  if (!keys.length) return null;
  const pre = p ? p + "/" : "";
  const r = await pool.query(
    `SELECT n.path, n.val FROM nodes n
     JOIN unnest($2::text[]) AS k ON (
       n.path = ($1 || k) OR (n.path >= ($1 || k || '/') AND n.path < ($1 || k || '0'))
     )
     ORDER BY n.path COLLATE "C"`,
    [pre, keys]
  );
  return buildTree(r.rows, p);
}

function ancestors(p) {
  const parts = p.split("/");
  const out = [];
  for (let i = 1; i < parts.length; i++) out.push(parts.slice(0, i).join("/"));
  return out;
}

async function setValue(p, val) {
  const leaves = [];
  flatten(val, p, leaves, Date.now());
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(`DELETE FROM nodes WHERE ${RANGE}`, [p]);
    const anc = ancestors(p);
    if (anc.length) await client.query("DELETE FROM nodes WHERE path = ANY($1::text[])", [anc]);
    if (leaves.length) {
      await client.query(
        `INSERT INTO nodes (path, val)
         SELECT t.p, t.v::jsonb FROM unnest($1::text[], $2::text[]) AS t(p, v)
         ON CONFLICT (path) DO UPDATE SET val = EXCLUDED.val`,
        [leaves.map(x => x[0]), leaves.map(x => JSON.stringify(x[1]))]
      );
    }
    await client.query("COMMIT");
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    throw e;
  } finally {
    client.release();
  }
}

async function removeValue(p) {
  await pool.query(`DELETE FROM nodes WHERE ${RANGE}`, [p]);
}

// Compare-and-set (runTransaction үчүн)
async function casValue(p, expect, val) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [p]);
    const r = await client.query(`SELECT path, val FROM nodes WHERE ${RANGE} ORDER BY path COLLATE "C"`, [p]);
    const cur = buildTree(r.rows, p);
    if (JSON.stringify(cur === undefined ? null : cur) !== JSON.stringify(expect === undefined ? null : expect)) {
      await client.query("ROLLBACK");
      return false;
    }
    await client.query(`DELETE FROM nodes WHERE ${RANGE}`, [p]);
    const leaves = [];
    flatten(val, p, leaves, Date.now());
    if (leaves.length) {
      await client.query(
        `INSERT INTO nodes (path, val)
         SELECT t.p, t.v::jsonb FROM unnest($1::text[], $2::text[]) AS t(p, v)
         ON CONFLICT (path) DO UPDATE SET val = EXCLUDED.val`,
        [leaves.map(x => x[0]), leaves.map(x => JSON.stringify(x[1]))]
      );
    }
    await client.query("COMMIT");
    return true;
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    throw e;
  } finally {
    client.release();
  }
}

// ---------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------
const app = express();
app.disable("x-powered-by");

app.get("/health", (req, res) => res.json({ status: "LimeChat running" }));

app.get("/api/visit", async (req, res) => {
  try {
    const r = await pool.query("SELECT val FROM nodes WHERE path = 'analytics/total_visits'");
    res.json({ count: r.rows[0] ? Number(r.rows[0].val) : 0 });
  } catch (e) { res.json({ count: 0 }); }
});

app.post("/api/visit", async (req, res) => {
  try {
    const r = await pool.query(
      `INSERT INTO nodes (path, val) VALUES ('analytics/total_visits', '1'::jsonb)
       ON CONFLICT (path) DO UPDATE SET val = to_jsonb((nodes.val #>> '{}')::bigint + 1)
       RETURNING val`
    );
    res.json({ count: Number(r.rows[0].val) });
  } catch (e) { res.json({ count: 0 }); }
});

// Сервердин файлдарын (server.js, package.json) сыртка бербейбиз
app.use((req, res, next) => {
  if (/^\/(server\.js|package(-lock)?\.json|node_modules|\.)/i.test(req.path)) return res.status(404).end();
  next();
});
app.use(express.static(__dirname, { index: "index.html", dotfiles: "ignore" }));

const server = http.createServer(app);

// ---------------------------------------------------------------
// WebSocket: realtime
// ---------------------------------------------------------------
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 128 * 1024 * 1024 });
const conns = new Set();

function related(a, b) {
  return a === b || a === "" || b === "" || b.startsWith(a + "/") || a.startsWith(b + "/");
}

function sendJSON(ws, obj) {
  if (ws.readyState === 1) {
    try { ws.send(JSON.stringify(obj)); } catch (_) {}
  }
}

async function evaluate(conn, sub) {
  if (sub.busy) { sub.dirty = true; return; }
  sub.busy = true;
  try {
    do {
      sub.dirty = false;
      if (sub.dead) break;
      if (sub.kind === "v") {
        const v = await getValue(sub.path, sub.limit);
        const str = JSON.stringify(v === undefined ? null : v);
        if (sub.dead) break;
        if (str !== sub.last) {
          sub.last = str;
          sendJSON(conn.ws, { t: "v", sid: sub.sid, val: v });
        }
      } else {
        const keys = await childKeys(sub.path, sub.limit);
        const fresh = keys.filter(k => !sub.known.has(k));
        sub.known = new Set(keys);
        for (const k of fresh) {
          if (sub.dead) break;
          const v = await getValue(sub.path ? sub.path + "/" + k : k);
          if (v !== null && v !== undefined) sendJSON(conn.ws, { t: "c", sid: sub.sid, key: k, val: v });
        }
      }
      if (!sub.ready) { sub.ready = true; sendJSON(conn.ws, { t: "r", sid: sub.sid }); }
    } while (sub.dirty);
  } catch (e) {
    console.error("evaluate error:", e.message);
  } finally {
    sub.busy = false;
  }
}

function notify(path) {
  for (const conn of conns) {
    for (const sub of conn.subs.values()) {
      if (related(sub.path, path)) evaluate(conn, sub);
    }
  }
}

function checkWritePath(p) {
  const depth = p ? p.split("/").length : 0;
  if (depth < 2) throw new Error("path too short");
  if (p.length > 600) throw new Error("path too long");
  if (p.split("/")[0] === "analytics") throw new Error("forbidden");
}

async function handle(conn, m) {
  const reply = (obj) => { if (m.id !== undefined) sendJSON(conn.ws, Object.assign({ id: m.id }, obj)); };
  try {
    const p = normPath(m.path);
    switch (m.op) {
      case "get": {
        const limit = Number(m.limit) > 0 ? Math.min(Number(m.limit), 1000) : 0;
        reply({ ok: true, val: await getValue(p, limit) });
        break;
      }
      case "set": {
        checkWritePath(p);
        await setValue(p, m.val === undefined ? null : m.val);
        reply({ ok: true });
        notify(p);
        break;
      }
      case "rm": {
        checkWritePath(p);
        await removeValue(p);
        reply({ ok: true });
        notify(p);
        break;
      }
      case "cas": {
        checkWritePath(p);
        const ok = await casValue(p, m.expect, m.val);
        reply({ ok: true, swapped: ok });
        if (ok) notify(p);
        break;
      }
      case "disc": {
        checkWritePath(p);
        conn.disc.set(p, m.val === undefined ? null : m.val);
        reply({ ok: true });
        break;
      }
      case "sub": {
        const limit = Number(m.limit) > 0 ? Math.min(Number(m.limit), 1000) : 0;
        const old = conn.subs.get(m.sid);
        if (old) old.dead = true;
        const sub = { sid: m.sid, path: p, kind: m.kind === "c" ? "c" : "v", limit, known: new Set(), last: undefined, busy: false, dirty: false, dead: false, ready: false };
        conn.subs.set(m.sid, sub);
        evaluate(conn, sub);
        break;
      }
      case "unsub": {
        const s = conn.subs.get(m.sid);
        if (s) { s.dead = true; conn.subs.delete(m.sid); }
        break;
      }
      default:
        reply({ ok: false, err: "unknown op" });
    }
  } catch (e) {
    console.error("op error:", m.op, e.message);
    reply({ ok: false, err: e.message });
  }
}

wss.on("connection", (ws, req) => {
  const conn = { ws, subs: new Map(), disc: new Map() };
  conns.add(conn);
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });
  // Чоң файл жай жүктөлүп жатканда да туташууну тирүү деп эсептейбиз
  if (req && req.socket) req.socket.on("data", () => { ws.isAlive = true; });
  ws.on("message", data => {
    let m;
    try { m = JSON.parse(data.toString()); } catch (_) { return; }
    if (m && typeof m === "object") handle(conn, m);
  });
  ws.on("close", async () => {
    conns.delete(conn);
    for (const s of conn.subs.values()) s.dead = true;
    for (const [p, val] of conn.disc) {
      try {
        if (val === null) await removeValue(p); else await setValue(p, val);
        notify(p);
      } catch (e) { console.error("disconnect op error:", e.message); }
    }
  });
  ws.on("error", () => {});
});

setInterval(() => {
  for (const conn of conns) {
    if (conn.ws.isAlive === false) { conn.ws.terminate(); continue; }
    conn.ws.isAlive = false;
    try { conn.ws.ping(); } catch (_) {}
  }
}, 25000);

// ---------------------------------------------------------------
// Старт
// ---------------------------------------------------------------
async function start() {
  for (let i = 1; i <= 10; i++) {
    try { await initDB(); break; }
    catch (e) {
      console.error("DB туташпай жатат (" + i + "/10):", e.message);
      if (i === 10) process.exit(1);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  server.listen(PORT, "0.0.0.0", () => console.log("LimeChat PostgreSQL server running"));
}

process.on("unhandledRejection", e => console.error("unhandledRejection:", e && e.message));
start();
