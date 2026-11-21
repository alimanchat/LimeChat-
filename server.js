const express = require("express");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: "20mb" }));
app.use(express.static(__dirname));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("railway.internal")
    ? false
    : { rejectUnauthorized: false }
});

// Базанын таблицасын түзүү
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS limechat_data (
      id BIGSERIAL PRIMARY KEY,
      data_path TEXT UNIQUE NOT NULL,
      data JSONB,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
}

// Сервер
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.get("/health", (req, res) => {
  res.json({ status: "LimeChat running" });
});

// PostgreSQL маалымат алуу
app.get("/api/data/:key", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT data FROM limechat_data WHERE data_path = $1",
      [req.params.key]
    );
    res.json(result.rows[0]?.data ?? null);
  } catch (error) {
    res.status(500).json({ error: "Database error" });
  }
});

// PostgreSQL маалымат сактоо
app.put("/api/data/:key", async (req, res) => {
  try {
    await pool.query(
      `INSERT INTO limechat_data (data_path, data, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (data_path)
       DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()`,
      [req.params.key, JSON.stringify(req.body)]
    );
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "Database error" });
  }
});

// Маалымат өчүрүү
app.delete("/api/data/:key", async (req, res) => {
  try {
    await pool.query(
      "DELETE FROM limechat_data WHERE data_path = $1",
      [req.params.key]
    );
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "Database error" });
  }
});

initDB()
  .then(() => {
    app.listen(PORT, "0.0.0.0", () => {
      console.log("LimeChat PostgreSQL server running");
    });
  })
  .catch(error => {
    console.error("Database startup error:", error);
    process.exit(1);
  });
