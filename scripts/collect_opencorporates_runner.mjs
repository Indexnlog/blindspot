#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const RATE_LIMIT_DELAY_MS = 550;
const CHECKPOINT_INTERVAL = 50;
const MATCH_THRESHOLD = 2;
const API_BASE = "https://api.opencorporates.com/v0.4";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(scriptDir, "..");
const dataDir = path.join(rootDir, "data");
const dartDataDir = path.join(dataDir, "dart_subsidiaries");
const outputDir = path.join(dataDir, "opencorporates");
const checkpointFile = path.join(outputDir, "checkpoint.json");
const resultsFile = path.join(outputDir, "matches.json");

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
  const envContents = await fs.readFile(envPath, "utf8");
  const line = envContents
    .split(/\r?\n/)
    .find((entry) => entry.startsWith("OPENCORPORATES_API_KEY="));

  if (!line) {
    throw new Error("OPENCORPORATES_API_KEY not found in .env");
  }

  return line.split("=").slice(1).join("=").trim();
}

async function ensureOutputDir() {
  await fs.mkdir(outputDir, { recursive: true });
}

async function readJson(filePath, fallback) {
  try {
    const contents = await fs.readFile(filePath, "utf8");
    return JSON.parse(contents);
  } catch {
    return fallback;
  }
}

async function loadDartSubsidiaries() {
  const entries = await fs.readdir(dartDataDir);
  const subsidiaries = [];

  for (const entry of entries.filter((name) => name.endsWith(".json"))) {
    const fullPath = path.join(dartDataDir, entry);
    const parsed = await readJson(fullPath, []);
    if (Array.isArray(parsed)) {
      subsidiaries.push(...parsed);
    } else if (parsed && typeof parsed === "object") {
      subsidiaries.push(parsed);
    }
  }

  return subsidiaries;
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
    console.log(`Rate limit exceeded for query: ${query}`);
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
  await fs.writeFile(filePath, JSON.stringify(data, null, 2) + "\n", "utf8");
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

  console.log(`Loaded ${dartSubsidiaries.length} DART subsidiaries`);
  console.log(`Starting from checkpoint: ${checkpoint.processed}`);
  if (options.region) {
    console.log(`Region filter: ${options.region}`);
  }

  if (options.dryRun) {
    const remaining = Math.max(dartSubsidiaries.length - checkpoint.processed, 0);
    const estimatedSeconds = (remaining * RATE_LIMIT_DELAY_MS) / 1000;
    console.log(
      `Dry run: Estimated time: ${estimatedSeconds.toFixed(1)} seconds (${(
        estimatedSeconds / 60
      ).toFixed(1)} minutes)`,
    );
    return;
  }

  const matches = Array.isArray(existingMatches) ? [...existingMatches] : [];
  let processed = checkpoint.processed ?? 0;

  console.log(`Starting collection: ${processed + 1}/${dartSubsidiaries.length}`);

  for (let index = processed; index < dartSubsidiaries.length; index += 1) {
    const subsidiary = dartSubsidiaries[index];
    const name = String(subsidiary.sub_name ?? "").trim();
    if (!name) {
      processed = index + 1;
      continue;
    }

    console.log(`Searching: ${name}`);

    try {
      const result = await searchCompany(name, apiKey);
      if (!result) {
        processed = index + 1;
        continue;
      }

      const { isMatch, score } = isGoodMatch(name, result);
      if (!isMatch) {
        console.log(`  No good match (score: ${score})`);
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
        ...jurisdictionInfo,
      };

      matches.push(match);
      console.log(
        `  Match found: ${result.company.name} (score: ${score}, ${jurisdictionInfo.jurisdiction})`,
      );
    } catch (error) {
      console.log(`Error searching ${name}: ${error.message}`);
    }

    processed = index + 1;

    if (processed % CHECKPOINT_INTERVAL === 0) {
      await saveJson(checkpointFile, { processed, matches });
      console.log(
        `Checkpoint saved: ${processed}/${dartSubsidiaries.length} processed, ${matches.length} matches`,
      );
    }

    await sleep(RATE_LIMIT_DELAY_MS);
  }

  await saveJson(resultsFile, matches);
  await saveJson(checkpointFile, { processed, matches });
  console.log(
    `Collection complete: ${processed}/${dartSubsidiaries.length} processed, ${matches.length} matches saved`,
  );
}

run().catch((error) => {
  console.error(`Error: ${error.message}`);
  process.exit(1);
});
