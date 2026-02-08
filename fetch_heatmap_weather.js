import fs from "fs";

// ---------------------------------------------
// CONFIG
// ---------------------------------------------
const INPUT_FILE = "./heatmap_cells.json";
const OUTPUT_FILE = "./heatmap_weather_daily_v1.json";

const BATCH_SIZE = 50;
const SLEEP_MS = 15_000;
const RATE_LIMIT_SLEEP_MS = 60_000;
const DAYS = 3; // Anzahl Heatmap-Tage (ab morgen)

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

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

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
const TEST_LIMIT = 0;

const cellsAll = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
const cells =
  TEST_LIMIT > 0 ? cellsAll.slice(0, TEST_LIMIT) : cellsAll;

console.log(
  `Loaded ${cells.length} cells` +
  (TEST_LIMIT ? " (TEST MODE)" : "")
);

const batches = chunk(cells, BATCH_SIZE);

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
// MAIN LOOP
// ---------------------------------------------
for (let b = 0; b < batches.length; b++) {
  const batch = batches[b].filter(c => !store.cells[c.id]);

  if (batch.length === 0) {
    console.log(`⏭️ Batch ${b + 1}/${batches.length} already done`);
    await sleep(SLEEP_MS);
    continue;
  }

  console.log(`Batch ${b + 1}/${batches.length} (${batch.length} cells)`);

  const lat = batch.map(c => c.lat.toFixed(4)).join(",");
  const lon = batch.map(c => c.lon.toFixed(4)).join(",");

  // -----------------------------
  // REQUEST A – best_match
  // -----------------------------
  const paramsMain = new URLSearchParams({
    latitude: lat,
    longitude: lon,
    daily: DAILY_FIELDS_MAIN,
    start_date: START_DATE,
    end_date: END_DATE,
    timezone: "auto"
  });

  const urlMain =
    "https://api.open-meteo.com/v1/forecast?" +
    paramsMain.toString();

  // -----------------------------
  // REQUEST B – gem_seamless (gusts)
  // -----------------------------
  const paramsGusts = new URLSearchParams({
    latitude: lat,
    longitude: lon,
    daily: DAILY_FIELDS_GUSTS,
    start_date: START_DATE,
    end_date: END_DATE,
    timezone: "auto",
    models: "gem_seamless"
  });

  const urlGusts =
    "https://api.open-meteo.com/v1/forecast?" +
    paramsGusts.toString();

  let dataMain, dataGusts;

  try {
    const [resMain, resGusts] = await Promise.all([
      fetch(urlMain),
      fetch(urlGusts)
    ]);

    dataMain = await resMain.json();
    dataGusts = await resGusts.json();
  } catch {
    console.warn("⚠️ Network error – retry after pause");
    await sleep(RATE_LIMIT_SLEEP_MS);
    b--;
    continue;
  }

  if (dataMain?.error || dataGusts?.error) {
    console.warn("⛔ Open-Meteo error");
    await sleep(RATE_LIMIT_SLEEP_MS);
    b--;
    continue;
  }

  const responsesMain = Array.isArray(dataMain) ? dataMain : [dataMain];
  const responsesGusts = Array.isArray(dataGusts) ? dataGusts : [dataGusts];

  // ---------------------------------------------
  // PROCESS CELLS
  // ---------------------------------------------
  for (let i = 0; i < batch.length; i++) {
    const cell = batch[i];
    const rMain = responsesMain[i];
    const rGusts = responsesGusts[i];

    if (!rMain?.daily || !rGusts?.daily) {
      console.warn("⚠️ Missing daily for cell", cell.id);
      continue;
    }

    const d = rMain.daily;
    const g = rGusts.daily;
    const days = [];

    for (let day = 0; day < DAYS; day++) {
      const sunRawHours =
        (d.sunshine_duration?.[day] ?? 0) / 3600;

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

        // ☀️ HEATMAP-Sonne (ungerundet!)
        Number(sunEffective.toFixed(2)),

        Math.round(d.windspeed_10m_max?.[day] ?? 0),
        Math.round(g.windgusts_10m_max?.[day] ?? 0) // 💨 gem_seamless
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

  await sleep(SLEEP_MS);
}

console.log("✅ Done – all batches processed");
