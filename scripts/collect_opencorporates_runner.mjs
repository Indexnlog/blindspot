#!/usr/bin/env node

import fs from "node:fs";
import fsPromises from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const RATE_LIMIT_DELAY_MS = 550;
const CHECKPOINT_INTERVAL = 50;
const MATCH_THRESHOLD = 2;
const API_BASE = "https://api.opencorporates.com/v0.4";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, "..");
const envPath = path.join(rootDir, ".env");

if (fs.existsSync(envPath)) {
  const envContents = fs.readFileSync(envPath, "utf8");
  for (const line of envContents.split(/\r?\n/)) {
    if (!line || line.trim().startsWith("#")) {
      continue;
    }

    const eqIndex = line.indexOf("=");
    if (eqIndex === -1) {
      continue;
    }

    const key = line.slice(0, eqIndex).trim();
    const value = line.slice(eqIndex + 1).trim();
    if (key && !(key in process.env)) {
      process.env[key] = value;
    }
  }
}

const defaultDataDir = path.join(rootDir, "data");
const dataRoot = process.env.BLINDSPOT_DATA_ROOT || defaultDataDir;
const dartDataDir =
  process.env.BLINDSPOT_DART_DATA_DIR ||
  path.join(dataRoot, "dart_subsidiaries");
const outputDir =
  process.env.BLINDSPOT_OPENCORPORATES_DIR ||
  path.join(dataRoot, "opencorporates");
const latestDir = path.join(outputDir, "latest");
const archiveDir = path.join(outputDir, "archive");
const checkpointFile = path.join(latestDir, "checkpoint.json");
const resultsFile = path.join(latestDir, "matches.json");

const startedAt = new Date();
const runDay = startedAt.toISOString().slice(0, 10);
const runStamp = `${String(startedAt.getHours()).padStart(2, "0")}${String(startedAt.getMinutes()).padStart(2, "0")}${String(startedAt.getSeconds()).padStart(2, "0")}`;
const archiveDayDir = path.join(archiveDir, runDay);
const runLogFile = path.join(archiveDayDir, `run_${runStamp}.log`);

const regionMap = {
  us: "North America",
  ca: "North America",
  mx: "North America",
  gb: "Europe",
  de: "Europe",
  fr: "Europe",
  it: "Europe",
  es: "Europe",
  nl: "Europe",
  be: "Europe",
  ch: "Europe",
  at: "Europe",
  se: "Europe",
  no: "Europe",
  dk: "Europe",
  fi: "Europe",
  pl: "Europe",
  cz: "Europe",
  hu: "Europe",
  jp: "Asia",
  kr: "Asia",
  cn: "Asia",
  tw: "Asia",
  sg: "Asia",
  hk: "Asia",
  in: "Asia",
  th: "Asia",
  my: "Asia",
  id: "Asia",
  au: "Oceania",
  nz: "Oceania",
  br: "South America",
  ar: "South America",
  cl: "South America",
  za: "Africa",
  eg: "Africa",
};

function logLine(message) {
  console.log(message);
  fs.appendFileSync(runLogFile, `${message}\n`, "utf8");
}

function parseArgs(argv) {
  const options = {
    resume: false,
    dryRun: false,
    region: null,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--resume") {
      options.resume = true;
      continue;
    }

    if (arg === "--dry-run") {
      options.dryRun = true;
      continue;
    }

    if (arg === "--region") {
      options.region = argv[i + 1] ?? null;
      i += 1;
    }
  }

  return options;
}

async function loadEnvApiKey() {
  const envPath = path.join(rootDir, ".env");
  const envContents = await fsPromises.readFile(envPath, "utf8");
  const line = envContents
    .split(/\r?\n/)
    .find((entry) => entry.startsWith("OPENCORPORATES_API_KEY="));

  if (!line) {
    throw new Error("OPENCORPORATES_API_KEY not found in .env");
  }

  return line.split("=").slice(1).join("=").trim();
}

async function ensureOutputDir() {
  await fsPromises.mkdir(outputDir, { recursive: true });
  await fsPromises.mkdir(latestDir, { recursive: true });
  await fsPromises.mkdir(archiveDir, { recursive: true });
  await fsPromises.mkdir(archiveDayDir, { recursive: true });

  const legacyCheckpoint = path.join(outputDir, "checkpoint.json");
  const legacyMatches = path.join(outputDir, "matches.json");
  if (fs.existsSync(legacyCheckpoint) && !fs.existsSync(checkpointFile)) {
    fs.copyFileSync(legacyCheckpoint, checkpointFile);
  }
  if (fs.existsSync(legacyMatches) && !fs.existsSync(resultsFile)) {
    fs.copyFileSync(legacyMatches, resultsFile);
  }

  if (fs.existsSync(resultsFile)) {
    fs.copyFileSync(resultsFile, path.join(archiveDayDir, `matches_${runStamp}_pre.json`));
  }
  if (fs.existsSync(checkpointFile)) {
    fs.copyFileSync(checkpointFile, path.join(archiveDayDir, `checkpoint_${runStamp}_pre.json`));
  }
}

async function readJson(filePath, fallback) {
  try {
    const contents = await fsPromises.readFile(filePath, "utf8");
    return JSON.parse(contents);
  } catch {
    return fallback;
  }
}

async function loadDartSubsidiaries() {
  const entries = await fsPromises.readdir(dartDataDir);
  const subsidiaries = [];
  const seen = new Set();

  for (const entry of entries.filter((name) => name.endsWith(".json"))) {
    const fullPath = path.join(dartDataDir, entry);
    const parsed = await readJson(fullPath, []);
    for (const item of normalizeSubsidiaryPayload(parsed)) {
      const dedupeKey = `${String(item.corp_name || "").trim().toLowerCase()}::${String(item.sub_name || "").trim().toLowerCase()}`;
      if (seen.has(dedupeKey)) {
        continue;
      }
      seen.add(dedupeKey);
      subsidiaries.push(item);
    }
  }

  return subsidiaries;
}

function normalizeSubsidiaryPayload(data) {
  if (Array.isArray(data)) {
    return data
      .filter((item) => item && typeof item === "object")
      .map((item) => ({
        corp_code: item.corp_code || "",
        corp_name: item.corp_name || "",
        sub_name: item.sub_name || item.name || "",
        sub_code: item.sub_code || "",
        country: item.country || "",
        region: item.region || "",
        source: item.source || "",
      }));
  }

  if (data && typeof data === "object") {
    const normalized = [];
    for (const [corpName, items] of Object.entries(data)) {
      if (!Array.isArray(items)) {
        continue;
      }
      for (const item of items) {
        if (!item || typeof item !== "object") {
          continue;
        }
        normalized.push({
          corp_code: item.corp_code || "",
          corp_name: item.corp_name || corpName,
          sub_name: item.sub_name || item.name || "",
          sub_code: item.sub_code || "",
          country: item.country || "",
          region: item.region || "",
          source: item.source || "",
        });
      }
    }
    return normalized;
  }

  return [];
}

function normalizeWords(value) {
  return new Set(
    value
      .toLowerCase()
      .split(/\s+/)
      .map((word) => word.trim())
      .filter(Boolean),
  );
}

function cleanSearchName(name) {
  return name
    .replace(/（/g, "(")
    .replace(/）/g, ")")
    .replace(/[“”]/g, '"')
    .replace(/’/g, "'")
    .replace(/\(\*[0-9]+\)/g, "")
    .replace(/\(([A-Z0-9-]{2,10})\)\s*$/g, "")
    .replace(/에 피합병/g, " ")
    .replace(/의 종속기업/g, " ")
    .replace(/주식회사/g, " ")
    .replace(/\(주\)|㈜/g, " ")
    .replace(/[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]/g, " ")
    .replace(/[А-Яа-яЁё]+/g, " ")
    .replace(/[^\w\s&.,()'/+-]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/[ .,-]+$/g, "");
}

function buildSearchQueries(name) {
  const variants = [];
  const addVariant = (value) => {
    const normalized = value.replace(/\s+/g, " ").trim().replace(/[ .,-]+$/g, "");
    if (normalized && !variants.includes(normalized)) {
      variants.push(normalized);
    }
  };

  addVariant(name);
  const cleaned = cleanSearchName(name);
  addVariant(cleaned);
  addVariant(cleaned.replace(/\([^)]*\)/g, " "));
  addVariant(cleaned.replace(/&/g, "and"));
  addVariant(cleaned.replace(/\band\b/gi, "&"));
  addVariant(
    cleaned.replace(/\b(LLC|INC|LTD|LIMITED|CORPORATION|CORP|CO|GMBH|SAS|SARL|BV|PTY|PLC)\b\.?/gi, " "),
  );

  return variants;
}

function calculateWordOverlap(name1, name2) {
  const words1 = normalizeWords(name1);
  const words2 = normalizeWords(name2);
  let overlap = 0;

  for (const word of words1) {
    if (words2.has(word)) {
      overlap += 1;
    }
  }

  return overlap;
}

function isGoodMatch(dartName, ocResult) {
  const company = ocResult?.company ?? {};
  let score = calculateWordOverlap(dartName, company.name ?? "");

  for (const previousName of company.previous_names ?? []) {
    score = Math.max(
      score,
      calculateWordOverlap(dartName, previousName.company_name ?? ""),
    );
  }

  return { isMatch: score >= MATCH_THRESHOLD, score };
}

function getJurisdictionInfo(ocResult) {
  const jurisdiction = ocResult?.company?.jurisdiction_code ?? "";
  const countryCode = jurisdiction.includes("_")
    ? jurisdiction.split("_")[0]
    : jurisdiction;

  return {
    jurisdiction,
    country: countryCode.toUpperCase(),
    region: regionMap[countryCode.toLowerCase()] ?? "Other",
  };
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function searchCompany(query, apiKey) {
  const url = new URL(`${API_BASE}/companies/search`);
  url.searchParams.set("q", query);
  url.searchParams.set("api_token", apiKey);
  url.searchParams.set("per_page", "5");
  url.searchParams.set("order", "score");

  const response = await fetch(url, {
    headers: { "User-Agent": "blindspot-opencorporates-runner" },
  });

  if (response.status === 403) {
    logLine(`Rate limit exceeded for query: ${query}`);
    return null;
  }

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  const data = await response.json();
  return data?.results?.companies?.[0] ?? null;
}

function applyRegionFilter(subsidiaries, region) {
  if (!region) {
    return subsidiaries;
  }

  return subsidiaries.filter((sub) => {
    const subRegion = String(sub.region ?? "").trim().toLowerCase();
    return subRegion === region.trim().toLowerCase();
  });
}

async function saveJson(filePath, data) {
  await fsPromises.writeFile(filePath, JSON.stringify(data, null, 2) + "\n", "utf8");
}

async function run() {
  const options = parseArgs(process.argv.slice(2));
  const apiKey = await loadEnvApiKey();
  await ensureOutputDir();

  const checkpoint = options.resume
    ? await readJson(checkpointFile, { processed: 0, matches: [] })
    : { processed: 0, matches: [] };
  const existingMatches = options.resume
    ? await readJson(resultsFile, [])
    : [];
  const dartSubsidiaries = applyRegionFilter(
    await loadDartSubsidiaries(),
    options.region,
  );

  logLine(`Loaded ${dartSubsidiaries.length} DART subsidiaries`);
  logLine(`Starting from checkpoint: ${checkpoint.processed}`);
  logLine(`DART data dir: ${dartDataDir}`);
  logLine(`Output dir: ${outputDir}`);
  logLine(`Latest dir: ${latestDir}`);
  logLine(`Archive dir: ${archiveDayDir}`);
  if (options.region) {
    logLine(`Region filter: ${options.region}`);
  }

  if (options.dryRun) {
    const remaining = Math.max(dartSubsidiaries.length - checkpoint.processed, 0);
    const estimatedSeconds = (remaining * RATE_LIMIT_DELAY_MS) / 1000;
    logLine(
      `Dry run: Estimated time: ${estimatedSeconds.toFixed(1)} seconds (${(
        estimatedSeconds / 60
      ).toFixed(1)} minutes)`,
    );
    fs.copyFileSync(runLogFile, path.join(latestDir, "run.log"));
    return;
  }

  const matches = Array.isArray(existingMatches) ? [...existingMatches] : [];
  let processed = checkpoint.processed ?? 0;

  logLine(`Starting collection: ${processed + 1}/${dartSubsidiaries.length}`);

  for (let index = processed; index < dartSubsidiaries.length; index += 1) {
    const subsidiary = dartSubsidiaries[index];
    const name = String(subsidiary.sub_name ?? "").trim();
    if (!name) {
      processed = index + 1;
      continue;
    }

    logLine(`Searching: ${name}`);

    try {
      let searchQuery = name;
      let result = null;
      for (const [index, candidate] of buildSearchQueries(name).entries()) {
        if (index > 0) {
          logLine(`  Retrying with cleaned query: ${candidate}`);
        }
        const candidateResult = await searchCompany(candidate, apiKey);
        if (!candidateResult) {
          continue;
        }
        result = candidateResult;
        searchQuery = candidate;
        if (isGoodMatch(name, candidateResult).isMatch) {
          break;
        }
      }
      if (!result) {
        processed = index + 1;
        continue;
      }

      const { isMatch, score } = isGoodMatch(name, result);
      if (!isMatch) {
        logLine(`  No good match (score: ${score})`);
        processed = index + 1;
        continue;
      }

      const jurisdictionInfo = getJurisdictionInfo(result);
      const match = {
        dart_subsidiary: subsidiary,
        opencorporates_result: result,
        match_score: score,
        attribution: "OpenCorporates API search",
        collected_at: Math.floor(Date.now() / 1000),
        search_query: searchQuery,
        ...jurisdictionInfo,
      };

      matches.push(match);
      logLine(
        `  Match found: ${result.company.name} (score: ${score}, ${jurisdictionInfo.jurisdiction})`,
      );
    } catch (error) {
      logLine(`Error searching ${name}: ${error.message}`);
    }

    processed = index + 1;

    if (processed % CHECKPOINT_INTERVAL === 0) {
      await saveJson(checkpointFile, { processed, matches });
      logLine(
        `Checkpoint saved: ${processed}/${dartSubsidiaries.length} processed, ${matches.length} matches`,
      );
    }

    await sleep(RATE_LIMIT_DELAY_MS);
  }

  await saveJson(resultsFile, matches);
  await saveJson(checkpointFile, { processed, matches });
  fs.copyFileSync(resultsFile, path.join(archiveDayDir, `matches_${runStamp}.json`));
  fs.copyFileSync(checkpointFile, path.join(archiveDayDir, `checkpoint_${runStamp}.json`));
  fs.copyFileSync(runLogFile, path.join(latestDir, "run.log"));
  logLine(
    `Collection complete: ${processed}/${dartSubsidiaries.length} processed, ${matches.length} matches saved`,
  );
}

run().catch((error) => {
  console.error(`Error: ${error.message}`);
  process.exit(1);
});
