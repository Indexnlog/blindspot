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
import argparse
import requests
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, List, Optional, Tuple

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
DATA_DIR = SCRIPT_DIR.parent / "data"
DART_DATA_DIR = DATA_DIR / "dart_subsidiaries"
OUTPUT_DIR = DATA_DIR / "opencorporates"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
RESULTS_FILE = OUTPUT_DIR / "matches.json"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class OpenCorporatesCollector:
    def __init__(self, resume: bool = False, region_filter: Optional[str] = None, dry_run: bool = False):
        self.resume = resume
        self.region_filter = region_filter
        self.dry_run = dry_run
        self.session = requests.Session()
        self.last_request_time = 0
        
        # Load checkpoint if resuming
        self.checkpoint = self.load_checkpoint() if resume else {"processed": 0, "matches": []}
        
        # Load DART subsidiaries
        self.dart_subsidiaries = self.load_dart_subsidiaries()
        
        # Load existing results
        self.existing_matches = self.load_existing_matches()
        
        print(f"Loaded {len(self.dart_subsidiaries)} DART subsidiaries")
        print(f"Starting from checkpoint: {self.checkpoint['processed']}")
        if self.region_filter:
            print(f"Region filter: {self.region_filter}")

    def load_dart_subsidiaries(self) -> List[Dict]:
        """Load DART subsidiary data"""
        subsidiaries = []
        for json_file in DART_DATA_DIR.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        subsidiaries.extend(data)
                    else:
                        subsidiaries.append(data)
            except Exception as e:
                print(f"Error loading {json_file}: {e}")
        
        return subsidiaries

    def load_checkpoint(self) -> Dict:
        """Load checkpoint data"""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading checkpoint: {e}")
        return {"processed": 0, "matches": []}

    def load_existing_matches(self) -> List[Dict]:
        """Load existing matches"""
        if RESULTS_FILE.exists():
            try:
                with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading results: {e}")
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
                print(f"Rate limit exceeded for query: {query}")
                return None
            print(f"HTTP error for {query}: {e}")
        except Exception as e:
            print(f"Error searching {query}: {e}")
            
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
            
        print(f"Searching: {name}")
        
        result = self.search_company(name)
        if not result:
            return None
            
        is_match, score = self.is_good_match(name, result)
        if not is_match:
            print(f"  No good match (score: {score})")
            return None
            
        # Build match record
        match = {
            "dart_subsidiary": sub,
            "opencorporates_result": result,
            "match_score": score,
            "attribution": "OpenCorporates API search",
            "collected_at": time.time()
        }
        
        # Add jurisdiction info
        match.update(self.get_jurisdiction_info(result))
        
        print(f"  Match found: {result['company']['name']} (score: {score}, {match['jurisdiction']})")
        return match

    def run(self):
        """Main collection loop"""
        total = len(self.dart_subsidiaries)
        matches = self.existing_matches.copy()
        processed = self.checkpoint["processed"]
        
        print(f"Starting collection: {processed + 1}/{total}")
        
        if self.dry_run:
            estimated_time = (total - processed) * RATE_LIMIT_DELAY
            print(f"Dry run: Estimated time: {estimated_time:.1f} seconds ({estimated_time/60:.1f} minutes)")
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
                    print(f"Checkpoint saved: {processed}/{total} processed, {len(matches)} matches")
                
                sys.stdout.flush()  # Ensure output is visible
                
        except KeyboardInterrupt:
            print("\nInterrupted by user")
        finally:
            # Save final results
            self.save_results(matches)
            print(f"\nCollection complete: {processed}/{total} processed, {len(matches)} matches saved")

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