const express = require("express");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: "10mb" }));
app.use(express.static(__dirname));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.get("/health", (req, res) => {
  res.json({ status: "LimeChat server is running" });
});

app.get("/api/db-test", async (req, res) => {
  try {
    const result = await pool.query("SELECT NOW()");
    res.json({
      status: "PostgreSQL connected",
      time: result.rows[0].now
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ status: "Database connection failed" });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`LimeChat server running on port ${PORT}`);
});
