import fs from "fs";

// ---------------------------------------------
// CONFIG
// ---------------------------------------------
const INPUT_FILE = "./heatmap_cells.json";
const OUTPUT_FILE = "./heatmap_weather_daily_v1.json";

const DAYS = 3; // Anzahl Heatmap-Tage (ab morgen)

const BATCH_MIN = 30;
const BATCH_MAX = 120;
let batchSize = BATCH_MAX;

const SLEEP_MS = 3_000;
const RATE_LIMIT_SLEEP_MS = 60_000;

// best_match (alles außer Böen)
const DAILY_FIELDS_MAIN = [
  "temperature_2m_min",
  "temperature_2m_max",
  "precipitation_sum",
  "sunshine_duration",
  "windspeed_10m_max",
  "weathercode",
  "cloudcover_mean"
].join(",");

// gem_seamless (nur Böen)
const DAILY_FIELDS_GUSTS = "windgusts_10m_max";

// ---------------------------------------------
// HELPERS
// ---------------------------------------------
const sleep = ms => new Promise(r => setTimeout(r, ms));

function toISODate(d) {
  return d.toISOString().split("T")[0];
}

// 🌤 Sonnen-Dämpfung nach Weathercode (daily)
function weatherSunFactor(code) {
  if (code == null) return 1;
  if (code === 45 || code === 48) return 0.4;      // Nebel
  if (code >= 61 && code <= 82) return 0.3;        // Regen / Schauer / Schnee
  if (code >= 95) return 0.2;                      // Gewitter
  return 1;
}

// ---------------------------------------------
// DATE RANGE (ab morgen, exakt DAYS Tage)
// ---------------------------------------------
const today = new Date();

const startDate = new Date(today);
startDate.setDate(startDate.getDate() + 1);

const endDate = new Date(today);
endDate.setDate(endDate.getDate() + DAYS);

const START_DATE = toISODate(startDate);
const END_DATE = toISODate(endDate);

console.log(`📅 Heatmap range: ${START_DATE} → ${END_DATE}`);

// ---------------------------------------------
// LOAD CELLS
// ---------------------------------------------
const cellsAll = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
const cells = cellsAll;

console.log(`Loaded ${cells.length} cells`);

// ---------------------------------------------
// LOAD / INIT STORE (resume-fähig)
// ---------------------------------------------
let store = {
  v: 2,
  generated_at: new Date().toISOString(),
  days: DAYS,
  start_date: START_DATE,
  end_date: END_DATE,
  models: {
    default: "best_match",
    gusts: "gem_seamless"
  },
  cells: {}
};

if (fs.existsSync(OUTPUT_FILE)) {
  try {
    store = JSON.parse(fs.readFileSync(OUTPUT_FILE, "utf8"));
    console.log(
      `↩️ Resuming – already have ${Object.keys(store.cells).length} cells`
    );
  } catch {
    console.warn("⚠️ Could not read existing output file, starting fresh");
  }
}

// ---------------------------------------------
// MAIN LOOP (dynamic batches)
// ---------------------------------------------
let cursor = 0;
let consecutiveFailures = 0;

while (cursor < cells.length) {
  const batch = cells
    .slice(cursor, cursor + batchSize)
    .filter(c => !store.cells[c.id]);

  if (batch.length === 0) {
    cursor += batchSize;
    continue;
  }

  console.log(
    `Batch @${cursor} size=${batch.length} (dyn=${batchSize})`
  );

  const lat = batch.map(c => c.lat.toFixed(4)).join(",");
  const lon = batch.map(c => c.lon.toFixed(4)).join(",");

  const paramsMain = new URLSearchParams({
    latitude: lat,
    longitude: lon,
    daily: DAILY_FIELDS_MAIN,
    start_date: START_DATE,
    end_date: END_DATE,
    timezone: "auto"
  });

  const paramsGusts = new URLSearchParams({
    latitude: lat,
    longitude: lon,
    daily: DAILY_FIELDS_GUSTS,
    start_date: START_DATE,
    end_date: END_DATE,
    timezone: "auto",
    models: "gem_seamless"
  });

  const urlMain =
    "https://api.open-meteo.com/v1/forecast?" + paramsMain.toString();
  const urlGusts =
    "https://api.open-meteo.com/v1/forecast?" + paramsGusts.toString();

  let dataMain, dataGusts;

  try {
    const [resMain, resGusts] = await Promise.all([
      fetch(urlMain),
      fetch(urlGusts)
    ]);

    dataMain = await resMain.json();
    dataGusts = await resGusts.json();
  } catch {
    console.warn("⚠️ Network error");

    consecutiveFailures++;
    if (batchSize > BATCH_MIN) {
      batchSize = Math.max(BATCH_MIN, Math.floor(batchSize / 2));
      console.warn(`🧯 Reducing batch size → ${batchSize}`);
    }

    await sleep(RATE_LIMIT_SLEEP_MS);
    continue;
  }

  if (dataMain?.error || dataGusts?.error) {
    console.warn("⛔ Open-Meteo error");

    consecutiveFailures++;
    if (batchSize > BATCH_MIN) {
      batchSize = Math.max(BATCH_MIN, Math.floor(batchSize / 2));
      console.warn(`🧯 Reducing batch size → ${batchSize}`);
    }

    await sleep(RATE_LIMIT_SLEEP_MS);
    continue;
  }

  const responsesMain = Array.isArray(dataMain) ? dataMain : [dataMain];
  const responsesGusts = Array.isArray(dataGusts) ? dataGusts : [dataGusts];

  for (let i = 0; i < batch.length; i++) {
    const cell = batch[i];
    const rMain = responsesMain[i];
    const rGusts = responsesGusts[i];

    if (!rMain?.daily || !rGusts?.daily) continue;

    const d = rMain.daily;
    const g = rGusts.daily;
    const days = [];

    for (let day = 0; day < DAYS; day++) {
      const sunRawHours = (d.sunshine_duration?.[day] ?? 0) / 3600;

      const cloudFactor =
        d.cloudcover_mean?.[day] != null
          ? Math.max(0, 1 - d.cloudcover_mean[day] / 100)
          : 1;

      const weatherFactor =
        weatherSunFactor(d.weathercode?.[day]);

      const sunEffective =
        sunRawHours * cloudFactor * weatherFactor;

      days.push([
        Math.round(d.temperature_2m_min?.[day] ?? 0),
        Math.round(d.temperature_2m_max?.[day] ?? 0),
        Math.round(d.precipitation_sum?.[day] ?? 0),
        Math.round(sunEffective),
        Math.round(d.windspeed_10m_max?.[day] ?? 0),
        Math.round(g.windgusts_10m_max?.[day] ?? 0)
      ]);
    }

    store.cells[cell.id] = {
      c: cell.id,
      lat: cell.lat,
      lon: cell.lon,
      d: days
    };
  }

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(store));
  console.log(`💾 Saved ${Object.keys(store.cells).length} cells`);

  consecutiveFailures = 0;

  if (batchSize < BATCH_MAX) {
    batchSize += 10;
  }

  cursor += batch.length;
  await sleep(SLEEP_MS);
}

console.log("✅ Done – all batches processed");
