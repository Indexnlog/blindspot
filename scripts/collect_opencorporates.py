#!/usr/bin/env python3
"""
OpenCorporates Collector for BlindSpot Project
Phase 2: Collect subsidiary matches from OpenCorporates API

Usage:
    python scripts/collect_opencorporates.py [--resume] [--region REGION] [--dry-run]

Features:
- Resume from checkpoint (saves every 50 companies)
- Region filtering (e.g., --region Europe)
- Dry-run mode for time estimation
- Word-overlap matching with score threshold
- Jurisdiction/country/region classification
- Attribution tracking
"""

import os
import sys
import json
import time
import shutil
import argparse
import re
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Optional, Tuple


def safe_print(message: str):
    """Print safely on Windows terminals with limited encodings."""
    try:
        print(message)
    except UnicodeEncodeError:
        print(message.encode("ascii", errors="replace").decode("ascii"))


def clean_search_name(name: str) -> str:
    """Strip abbreviations, footnotes, and obvious mojibake noise for retry searches."""
    cleaned = name.replace("（", "(").replace("）", ")")
    cleaned = cleaned.replace("“", '"').replace("”", '"').replace("’", "'")
    cleaned = re.sub(r"\(\*[0-9]+\)", "", cleaned)
    cleaned = re.sub(r"\(([A-Z0-9\-]{2,10})\)\s*$", "", cleaned)
    cleaned = cleaned.replace("에 피합병", " ")
    cleaned = cleaned.replace("의 종속기업", " ")
    cleaned = cleaned.replace("주식회사", " ")
    cleaned = cleaned.replace("(주)", " ").replace("㈜", " ")
    cleaned = re.sub(r'["“”]', "", cleaned)
    cleaned = re.sub(r"[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]", " ", cleaned)
    cleaned = re.sub(r"[А-Яа-яЁё]+", " ", cleaned)
    cleaned = re.sub(r"[^\w\s&.,()'/+-]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,-")
    return cleaned


def build_search_queries(name: str) -> List[str]:
    """Generate progressively simpler search queries for retry attempts."""
    variants = []

    def add_variant(value: str):
        value = re.sub(r"\s+", " ", value).strip(" .,-")
        if value and value not in variants:
            variants.append(value)

    add_variant(name)

    cleaned = clean_search_name(name)
    add_variant(cleaned)
    add_variant(re.sub(r"\([^)]*\)", " ", cleaned))
    add_variant(cleaned.replace("&", "and"))
    add_variant(cleaned.replace(" and ", " & "))
    add_variant(re.sub(r"\b(LLC|INC|LTD|LIMITED|CORPORATION|CORP|CO|GMBH|SAS|SARL|BV|PTY|PLC)\b\.?", " ", cleaned, flags=re.IGNORECASE))

    return variants

# Load environment variables
load_dotenv()
API_KEY = os.getenv('OPENCORPORATES_API_KEY')
if not API_KEY:
    print("Error: OPENCORPORATES_API_KEY not found in .env")
    sys.exit(1)

# Constants
API_BASE = "https://api.opencorporates.com/v0.4"
RATE_LIMIT_DELAY = 0.55  # seconds between requests
CHECKPOINT_INTERVAL = 50
MATCH_THRESHOLD = 2  # minimum word overlap score

# Paths
SCRIPT_DIR = Path(__file__).parent
DEFAULT_DATA_DIR = SCRIPT_DIR.parent / "data"
DATA_ROOT = Path(os.getenv("BLINDSPOT_DATA_ROOT", str(DEFAULT_DATA_DIR))).expanduser()
DART_DATA_DIR = Path(
    os.getenv("BLINDSPOT_DART_DATA_DIR", str(DATA_ROOT / "dart_subsidiaries"))
).expanduser()
OUTPUT_DIR = Path(
    os.getenv("BLINDSPOT_OPENCORPORATES_DIR", str(DATA_ROOT / "opencorporates"))
).expanduser()
LATEST_DIR = OUTPUT_DIR / "latest"
ARCHIVE_DIR = OUTPUT_DIR / "archive"
CHECKPOINT_FILE = LATEST_DIR / "checkpoint.json"
RESULTS_FILE = LATEST_DIR / "matches.json"


class RunLogger:
    """Mirror collector output to both stdout and a log file."""

    def __init__(self, log_file: Path):
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(log_file, "a", encoding="utf-8")

    def write(self, message: str):
        safe_print(message)
        self._fh.write(message + "\n")
        self._fh.flush()

    def close(self):
        self._fh.close()

class OpenCorporatesCollector:
    def __init__(self, resume: bool = False, region_filter: Optional[str] = None, dry_run: bool = False):
        self.resume = resume
        self.region_filter = region_filter
        self.dry_run = dry_run
        self.session = requests.Session()
        self.last_request_time = 0
        self.started_at = datetime.now()
        self.run_day = self.started_at.strftime("%Y-%m-%d")
        self.run_stamp = self.started_at.strftime("%H%M%S")
        self.archive_day_dir = ARCHIVE_DIR / self.run_day

        self.ensure_output_layout()
        self.logger = RunLogger(self.archive_day_dir / f"run_{self.run_stamp}.log")
        self.snapshot_existing_latest()
        
        # Load checkpoint if resuming
        self.checkpoint = self.load_checkpoint() if resume else {"processed": 0, "matches": []}
        
        # Load DART subsidiaries
        self.dart_subsidiaries = self.load_dart_subsidiaries()
        
        # Only reuse saved results when explicitly resuming a previous run.
        self.existing_matches = self.load_existing_matches() if resume else []
        
        self.log(f"Loaded {len(self.dart_subsidiaries)} DART subsidiaries")
        self.log(f"Starting from checkpoint: {self.checkpoint['processed']}")
        self.log(f"DART data dir: {DART_DATA_DIR}")
        self.log(f"Output dir: {OUTPUT_DIR}")
        self.log(f"Latest dir: {LATEST_DIR}")
        self.log(f"Archive dir: {self.archive_day_dir}")
        if self.region_filter:
            self.log(f"Region filter: {self.region_filter}")

    def log(self, message: str):
        self.logger.write(message)

    def ensure_output_layout(self):
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        LATEST_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        self.archive_day_dir.mkdir(parents=True, exist_ok=True)

        legacy_checkpoint = OUTPUT_DIR / "checkpoint.json"
        legacy_matches = OUTPUT_DIR / "matches.json"
        if legacy_checkpoint.exists() and not CHECKPOINT_FILE.exists():
            shutil.copy2(legacy_checkpoint, CHECKPOINT_FILE)
        if legacy_matches.exists() and not RESULTS_FILE.exists():
            shutil.copy2(legacy_matches, RESULTS_FILE)

    def snapshot_existing_latest(self):
        for source, prefix in ((RESULTS_FILE, "matches"), (CHECKPOINT_FILE, "checkpoint")):
            if source.exists():
                shutil.copy2(source, self.archive_day_dir / f"{prefix}_{self.run_stamp}_pre.json")

    def load_dart_subsidiaries(self) -> List[Dict]:
        """Load DART subsidiary data"""
        subsidiaries = []
        seen = set()
        for json_file in DART_DATA_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    normalized = self.normalize_subsidiary_payload(data)
                    for item in normalized:
                        dedupe_key = (
                            item.get("corp_name", "").strip().lower(),
                            item.get("sub_name", "").strip().lower(),
                        )
                        if dedupe_key in seen:
                            continue
                        seen.add(dedupe_key)
                        subsidiaries.append(item)
            except Exception as e:
                self.log(f"Error loading {json_file}: {e}")
        
        return subsidiaries

    def normalize_subsidiary_payload(self, data) -> List[Dict]:
        """Normalize supported DART payloads into BlindSpot subsidiary rows."""
        if isinstance(data, list):
            normalized = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                sub_name = item.get("sub_name") or item.get("name") or ""
                normalized.append({
                    "corp_code": item.get("corp_code", ""),
                    "corp_name": item.get("corp_name", ""),
                    "sub_name": sub_name,
                    "sub_code": item.get("sub_code", ""),
                    "country": item.get("country", ""),
                    "region": item.get("region", ""),
                    "source": item.get("source", ""),
                })
            return normalized

        if isinstance(data, dict):
            normalized = []
            for corp_name, items in data.items():
                if not isinstance(items, list):
                    continue
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    sub_name = item.get("sub_name") or item.get("name") or ""
                    normalized.append({
                        "corp_code": item.get("corp_code", ""),
                        "corp_name": item.get("corp_name", corp_name),
                        "sub_name": sub_name,
                        "sub_code": item.get("sub_code", ""),
                        "country": item.get("country", ""),
                        "region": item.get("region", ""),
                        "source": item.get("source", ""),
                    })
            return normalized

        return []

    def load_checkpoint(self) -> Dict:
        """Load checkpoint data"""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.log(f"Error loading checkpoint: {e}")
        return {"processed": 0, "matches": []}

    def load_existing_matches(self) -> List[Dict]:
        """Load existing matches"""
        if RESULTS_FILE.exists():
            try:
                with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.log(f"Error loading results: {e}")
        return []

    def save_checkpoint(self, processed: int, matches: List[Dict]):
        """Save checkpoint"""
        checkpoint = {"processed": processed, "matches": matches}
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)

    def save_results(self, matches: List[Dict]):
        """Save final results"""
        with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(matches, f, ensure_ascii=False, indent=2)

    def rate_limit_wait(self):
        """Respect API rate limits"""
        elapsed = time.time() - self.last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()

    def search_company(self, query: str) -> Optional[Dict]:
        """Search for company in OpenCorporates"""
        if self.dry_run:
            return None
            
        self.rate_limit_wait()
        
        url = f"{API_BASE}/companies/search"
        params = {
            "q": query,
            "api_token": API_KEY,
            "per_page": 5,  # Limit results
            "order": "score"
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", {}).get("companies", [])
            
            if results:
                return results[0]  # Return top result
                
        except requests.exceptions.HTTPError as e:
            if response.status_code == 403:
                self.log(f"Rate limit exceeded for query: {query}")
                return None
            self.log(f"HTTP error for {query}: {e}")
        except Exception as e:
            self.log(f"Error searching {query}: {e}")
            
        return None

    def calculate_word_overlap(self, name1: str, name2: str) -> int:
        """Calculate word overlap score between two company names"""
        words1 = set(name1.lower().split())
        words2 = set(name2.lower().split())
        return len(words1 & words2)

    def is_good_match(self, dart_name: str, oc_result: Dict) -> Tuple[bool, int]:
        """Check if OpenCorporates result is a good match"""
        oc_name = oc_result.get("company", {}).get("name", "")
        score = self.calculate_word_overlap(dart_name, oc_name)
        
        # Also check previous names
        previous_names = oc_result.get("company", {}).get("previous_names", [])
        for prev in previous_names:
            prev_score = self.calculate_word_overlap(dart_name, prev.get("company_name", ""))
            score = max(score, prev_score)
        
        return score >= MATCH_THRESHOLD, score

    def get_jurisdiction_info(self, oc_result: Dict) -> Dict:
        """Extract jurisdiction, country, region info"""
        jurisdiction = oc_result.get("company", {}).get("jurisdiction_code", "")
        country = jurisdiction.split("_")[0] if "_" in jurisdiction else jurisdiction
        
        # Simple region mapping
        region_map = {
            "us": "North America",
            "ca": "North America", 
            "mx": "North America",
            "gb": "Europe",
            "de": "Europe",
            "fr": "Europe",
            "it": "Europe",
            "es": "Europe",
            "nl": "Europe",
            "be": "Europe",
            "ch": "Europe",
            "at": "Europe",
            "se": "Europe",
            "no": "Europe",
            "dk": "Europe",
            "fi": "Europe",
            "pl": "Europe",
            "cz": "Europe",
            "hu": "Europe",
            "jp": "Asia",
            "kr": "Asia",
            "cn": "Asia",
            "tw": "Asia",
            "sg": "Asia",
            "hk": "Asia",
            "in": "Asia",
            "th": "Asia",
            "my": "Asia",
            "id": "Asia",
            "au": "Oceania",
            "nz": "Oceania",
            "br": "South America",
            "ar": "South America",
            "cl": "South America",
            "za": "Africa",
            "eg": "Africa"
        }
        
        region = region_map.get(country.lower(), "Other")
        
        return {
            "jurisdiction": jurisdiction,
            "country": country.upper(),
            "region": region
        }

    def process_subsidiary(self, sub: Dict) -> Optional[Dict]:
        """Process a single DART subsidiary"""
        name = sub.get("sub_name", "").strip()
        if not name:
            return None
            
        self.log(f"Searching: {name}")

        result = None
        search_query = name
        for idx, candidate in enumerate(build_search_queries(name)):
            if idx > 0:
                self.log(f"  Retrying with cleaned query: {candidate}")
            candidate_result = self.search_company(candidate)
            if not candidate_result:
                continue

            is_match, score = self.is_good_match(name, candidate_result)
            result = candidate_result
            search_query = candidate
            if is_match:
                break
        if not result:
            return None
            
        is_match, score = self.is_good_match(name, result)
        if not is_match:
            self.log(f"  No good match (score: {score})")
            return None
            
        # Build match record
        match = {
            "dart_subsidiary": sub,
            "opencorporates_result": result,
            "match_score": score,
            "attribution": "OpenCorporates API search",
            "collected_at": time.time(),
            "search_query": search_query,
        }
        
        # Add jurisdiction info
        match.update(self.get_jurisdiction_info(result))
        
        self.log(f"  Match found: {result['company']['name']} (score: {score}, {match['jurisdiction']})")
        return match

    def archive_final_outputs(self):
        for source, prefix in ((RESULTS_FILE, "matches"), (CHECKPOINT_FILE, "checkpoint")):
            if source.exists():
                shutil.copy2(source, self.archive_day_dir / f"{prefix}_{self.run_stamp}.json")

        shutil.copy2(self.logger.log_file, LATEST_DIR / "run.log")

    def run(self):
        """Main collection loop"""
        total = len(self.dart_subsidiaries)
        matches = self.existing_matches.copy()
        processed = self.checkpoint["processed"]
        
        self.log(f"Starting collection: {processed + 1}/{total}")
        
        if self.dry_run:
            estimated_time = (total - processed) * RATE_LIMIT_DELAY
            self.log(f"Dry run: Estimated time: {estimated_time:.1f} seconds ({estimated_time/60:.1f} minutes)")
            shutil.copy2(self.logger.log_file, LATEST_DIR / "run.log")
            self.logger.close()
            return
        
        try:
            for i in range(processed, total):
                sub = self.dart_subsidiaries[i]
                
                match = self.process_subsidiary(sub)
                if match:
                    matches.append(match)
                
                processed = i + 1
                
                # Save checkpoint
                if processed % CHECKPOINT_INTERVAL == 0:
                    self.save_checkpoint(processed, matches)
                    self.log(f"Checkpoint saved: {processed}/{total} processed, {len(matches)} matches")
                
                sys.stdout.flush()  # Ensure output is visible
                
        except KeyboardInterrupt:
            self.log("\nInterrupted by user")
        finally:
            # Save final checkpoint and results on every exit path.
            self.save_checkpoint(processed, matches)
            self.save_results(matches)
            self.archive_final_outputs()
            self.log(f"\nCollection complete: {processed}/{total} processed, {len(matches)} matches saved")
            self.logger.close()

def main():
    parser = argparse.ArgumentParser(description="OpenCorporates Collector for BlindSpot")
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--region", help="Filter by region (e.g., Europe, Asia)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run for time estimation")
    
    args = parser.parse_args()
    
    collector = OpenCorporatesCollector(
        resume=args.resume,
        region_filter=args.region,
        dry_run=args.dry_run
    )
    
    collector.run()

if __name__ == "__main__":
    main()
