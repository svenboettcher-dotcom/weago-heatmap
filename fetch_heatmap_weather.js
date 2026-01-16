import fs from "fs";

// ---------------------------------------------
// CONFIG
// ---------------------------------------------
const INPUT_FILE = "./heatmap_cells.json";
const OUTPUT_FILE = "./heatmap_weather_daily_v1.json";

const BATCH_SIZE = 50;
const SLEEP_MS = 10_000;
const RATE_LIMIT_SLEEP_MS = 60_000;
const DAYS = 3;

const DAILY_FIELDS = [
  "temperature_2m_min",
  "temperature_2m_max",
  "precipitation_sum",
  "sunshine_duration",
  "windspeed_10m_max",
  "windgusts_10m_max"
].join(",");

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

// ---------------------------------------------
// LOAD CELLS
// ---------------------------------------------
const TEST_LIMIT = 10; // 0 = aus, z.B. 50 = Testmodus

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
  v: 1,
  generated_at: new Date().toISOString(),
  days: DAYS,
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

  const params = new URLSearchParams({
    latitude: lat,
    longitude: lon,
    daily: DAILY_FIELDS,
    forecast_days: String(DAYS),
    timezone: "auto"
  });

  const url = "https://api.open-meteo.com/v1/forecast?" + params.toString();

  let data;
  try {
    const res = await fetch(url);
    data = await res.json();
  } catch {
    console.warn("⚠️ Network error – retry after pause");
    await sleep(RATE_LIMIT_SLEEP_MS);
    b--;
    continue;
  }

  if (data?.error) {
    console.warn("⛔ Open-Meteo:", data.reason || data);
    console.log(`⏸️ Rate-limit pause ${RATE_LIMIT_SLEEP_MS / 1000}s`);
    await sleep(RATE_LIMIT_SLEEP_MS);
    b--;
    continue;
  }

  const responses = Array.isArray(data) ? data : [data];

  // ---------------------------------------------
  // PROCESS CELLS
  // ---------------------------------------------
  for (let i = 0; i < batch.length; i++) {
    const cell = batch[i];
    const r = responses[i];

    if (!r?.daily) {
      console.warn("⚠️ Missing daily for cell", cell.id);
      continue;
    }

    const d = r.daily;
    const days = [];

for (let day = 0; day < DAYS; day++) {
  days.push([
    Math.round(d.temperature_2m_min?.[day] ?? 0),
    Math.round(d.temperature_2m_max?.[day] ?? 0),
    Math.round(d.precipitation_sum?.[day] ?? 0),
    Math.round((d.sunshine_duration?.[day] ?? 0) / 3600),
    Math.round(d.windspeed_10m_max?.[day] ?? 0),
    Math.round(d.windgusts_10m_max?.[day] ?? 0)
  ]);
}


    store.cells[cell.id] = {
      c: cell.id,
      lat: cell.lat,
      lon: cell.lon,
      d: days
    };
  }

  // ---------------------------------------------
  // WRITE AFTER EACH BATCH
  // ---------------------------------------------
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(store));
  console.log(`💾 Saved ${Object.keys(store.cells).length} cells`);

  await sleep(SLEEP_MS);
}

console.log("✅ Done – all batches processed");
