const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const { WebSocketServer } = require("ws");
const fs = require("fs");
const path = require("path");
const http = require("http");

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY;

if (!API_KEY) {
  console.error("Missing API_KEY env var -- refusing to start without it.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("localhost") ? false : { rejectUnauthorized: false }
});

function normalize(str) {
  return (str || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function rowToEmployee(row) {
  return {
    id: row.id,
    full_name: row.full_name,
    job_title: row.job_title,
    department: row.department,
    email: row.email,
    phone: row.phone,
    photo_url: row.photo_url,
    location: row.location,
    jerarquia: row.jerarquia,
    agrupamiento: row.agrupamiento,
    legajo: row.legajo,
    es_chofer: row.es_chofer,
    licencia_conducir: row.licencia_conducir,
    domicilio: row.domicilio,
    contacto_nombre: row.contacto_nombre,
    contacto_telefono: row.contacto_telefono,
    grupo_sanguineo: row.grupo_sanguineo,
    situacion_actual: row.situacion_actual,
    elementos_asignados: row.elementos_asignados || []
  };
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      id TEXT PRIMARY KEY,
      full_name TEXT NOT NULL,
      job_title TEXT NOT NULL,
      department TEXT NOT NULL,
      email TEXT NOT NULL,
      phone TEXT,
      photo_url TEXT,
      location TEXT,
      jerarquia TEXT,
      agrupamiento TEXT,
      legajo TEXT,
      es_chofer BOOLEAN NOT NULL DEFAULT FALSE,
      licencia_conducir TEXT,
      domicilio TEXT,
      contacto_nombre TEXT,
      contacto_telefono TEXT,
      grupo_sanguineo TEXT,
      situacion_actual TEXT NOT NULL DEFAULT 'EN_SERVICIO',
      elementos_asignados JSONB NOT NULL DEFAULT '[]'
    )
  `);

  const { rows } = await pool.query("SELECT COUNT(*)::int AS count FROM employees");
  if (rows[0].count === 0) {
    const seed = JSON.parse(fs.readFileSync(path.join(__dirname, "seed.json"), "utf-8"));
    for (const e of seed) {
      await upsertEmployee(e);
    }
    console.log(`Seeded ${seed.length} employees.`);
  }
}

async function upsertEmployee(e) {
  const { rows } = await pool.query(
    `INSERT INTO employees (
       id, full_name, job_title, department, email, phone, photo_url, location,
       jerarquia, agrupamiento, legajo, es_chofer, licencia_conducir, domicilio,
       contacto_nombre, contacto_telefono, grupo_sanguineo, situacion_actual, elementos_asignados
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
     ON CONFLICT (id) DO UPDATE SET
       full_name = EXCLUDED.full_name,
       job_title = EXCLUDED.job_title,
       department = EXCLUDED.department,
       email = EXCLUDED.email,
       phone = EXCLUDED.phone,
       photo_url = EXCLUDED.photo_url,
       location = EXCLUDED.location,
       jerarquia = EXCLUDED.jerarquia,
       agrupamiento = EXCLUDED.agrupamiento,
       legajo = EXCLUDED.legajo,
       es_chofer = EXCLUDED.es_chofer,
       licencia_conducir = EXCLUDED.licencia_conducir,
       domicilio = EXCLUDED.domicilio,
       contacto_nombre = EXCLUDED.contacto_nombre,
       contacto_telefono = EXCLUDED.contacto_telefono,
       grupo_sanguineo = EXCLUDED.grupo_sanguineo,
       situacion_actual = EXCLUDED.situacion_actual,
       elementos_asignados = EXCLUDED.elementos_asignados
     RETURNING *`,
    [
      e.id, e.full_name, e.job_title, e.department, e.email, e.phone ?? null,
      e.photo_url ?? null, e.location ?? null, e.jerarquia ?? null, e.agrupamiento ?? null,
      e.legajo ?? null, e.es_chofer ?? false, e.licencia_conducir ?? null, e.domicilio ?? null,
      e.contacto_nombre ?? null, e.contacto_telefono ?? null, e.grupo_sanguineo ?? null,
      e.situacion_actual ?? "EN_SERVICIO", JSON.stringify(e.elementos_asignados ?? [])
    ]
  );
  return rowToEmployee(rows[0]);
}

const app = express();
app.use(cors());
app.use(express.json());

app.use((req, res, next) => {
  if (req.header("x-api-key") !== API_KEY) {
    return res.status(401).json({ error: "Invalid or missing API key" });
  }
  next();
});

app.get("/", (_req, res) => {
  res.json({ status: "ok", message: "Employee Search realtime backend running" });
});

app.get("/employees", async (_req, res) => {
  const { rows } = await pool.query("SELECT * FROM employees ORDER BY full_name");
  res.json(rows.map(rowToEmployee));
});

app.get("/employees/search", async (req, res) => {
  const q = normalize(req.query.q || "");
  if (!q) return res.json([]);
  const { rows } = await pool.query("SELECT * FROM employees");
  const results = rows
    .map(rowToEmployee)
    .filter((e) =>
      normalize(e.full_name).includes(q) ||
      normalize(e.job_title).includes(q) ||
      normalize(e.department).includes(q) ||
      normalize(e.jerarquia).includes(q) ||
      normalize(e.legajo).includes(q)
    );
  res.json(results);
});

app.get("/employees/:id", async (req, res) => {
  const { rows } = await pool.query("SELECT * FROM employees WHERE id = $1", [req.params.id]);
  if (rows.length === 0) return res.status(404).json({ error: "Employee not found" });
  res.json(rowToEmployee(rows[0]));
});

app.put("/employees/:id", async (req, res) => {
  const employee = await upsertEmployee({ ...req.body, id: req.params.id });
  broadcast({ type: "upsert", employee });
  res.json(employee);
});

app.delete("/employees/:id", async (req, res) => {
  await pool.query("DELETE FROM employees WHERE id = $1", [req.params.id]);
  broadcast({ type: "delete", id: req.params.id });
  res.status(204).send();
});

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const clients = new Set();

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const key = req.headers["x-api-key"] || url.searchParams.get("api_key");
  if (url.pathname !== "/ws" || key !== API_KEY) {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    clients.add(ws);
    ws.on("close", () => clients.delete(ws));
  });
});

function broadcast(message) {
  const payload = JSON.stringify(message);
  for (const client of clients) {
    if (client.readyState === client.OPEN) client.send(payload);
  }
}

initDb()
  .then(() => {
    server.listen(PORT, () => console.log(`Employee Search backend listening on :${PORT}`));
  })
  .catch((err) => {
    console.error("Failed to initialize database", err);
    process.exit(1);
  });
