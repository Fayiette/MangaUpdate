import sys
import requests
import json
import csv
import os
import time
import boto3
import hashlib
from botocore.exceptions import ClientError
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import Dict, List, Optional, Tuple

# DIM schema (CSV + Parquet); keep in sync with flush_dim_cache / new rows.
DIM_SERIES_FIELDNAMES: List[str] = [
    "series_id",
    "title",
    "max_chapter",
    "type",
    "year",
    "authors",
    "artists",
    "genres",
    "cover_image_url",
    "cover_thumb_url",
]

# Load credentials from .env file
load_dotenv()


def env_str(key: str, default: str) -> str:
    """Return getenv(key) stripped, or default if unset/empty."""
    v = (os.getenv(key) or "").strip()
    return v if v else default


# Built-in defaults are generic filenames only — set real paths via .env or CI variables.
_DEFAULT_DIM_SERIES_CSV = "dim_series.csv"
_DEFAULT_FACT_PROGRESS_CSV = "fact_progress.csv"
_DEFAULT_DIM_SERIES_PARQUET = "dim_series.parquet"
_DEFAULT_FACT_PROGRESS_PARQUET = "fact_progress.parquet"
_DEFAULT_SERIES_API_SAMPLE_JSON = "api_sample.json"
_DEFAULT_MANGAUPDATES_API_BASE = "/api.example.com/"

# Load environment variables
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")


def send_discord_alert(message: str):
    """Send alert message to Discord webhook"""
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ Discord webhook URL is not set.")
        return
    payload = {"content": message}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            data=json.dumps(payload),
            headers=headers,
            timeout=5,
        )
        response.raise_for_status()
        print("📣 Discord alert sent.")
    except Exception as e:
        print(f"❌ Failed to send Discord alert: {e}")


def get_file_hash(file_path: str) -> str:
    """Calculate SHA256 hash of file"""
    if not os.path.exists(file_path):
        return None

    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


class MangaUpdatesDataWarehouse:
    """
    MangaUpdates Data Warehouse - DIM/Fact patterns
    - Batch processing: Load DIM to memory once, update all, write once
    - Persistent S3 client
    - Rate limited API
    - Conditional Parquet export
    - Progress validation (current > last)
    """

    def __init__(self, username, password, require_r2_pull: bool = False):
        # MangaUpdates API (see .env.example)
        self.base_url = env_str(
            "MANGAUPDATES_API_BASE_URL", _DEFAULT_MANGAUPDATES_API_BASE
        ).rstrip("/")
        self.username = username
        self.password = password
        self.token = None
        # If True, abort when R2 baseline CSVs are not both loaded (unless both keys missing = empty bucket bootstrap).
        self.require_r2_pull = require_r2_pull

        # Local file paths (R2 object keys use the same basename — see pull_from_r2 / upload_files_to_r2)
        self.dim_series_path = env_str(
            "DIM_SERIES_CSV_PATH", _DEFAULT_DIM_SERIES_CSV
        )
        self.fact_progress_path = env_str(
            "FACT_PROGRESS_CSV_PATH", _DEFAULT_FACT_PROGRESS_CSV
        )
        self.dim_series_parquet = env_str(
            "DIM_SERIES_PARQUET_PATH", _DEFAULT_DIM_SERIES_PARQUET
        )
        self.fact_progress_parquet = env_str(
            "FACT_PROGRESS_PARQUET_PATH", _DEFAULT_FACT_PROGRESS_PARQUET
        )
        self.series_api_sample_path = env_str(
            "SERIES_API_SAMPLE_PATH", _DEFAULT_SERIES_API_SAMPLE_JSON
        )

        # Track hashes before/after
        self.hashes_before = {}
        self.hashes_after = {}

        # Track if changes were made
        self.changes_made = False

        # Initialize S3 Client ONCE
        self.s3_client = self.create_s3_client()

        # BATCH PROCESSING: In-memory DIM cache
        # Dict[series_id, dict_row]
        self.dim_cache: Dict[str, Dict] = {}
        # Dict[series_id, bool] - track if we updated this row
        self.dim_updates_made: Dict[str, bool] = {}
        # If True, enrich step fetches every series to refresh cover URLs (slow).
        self.full_image_refresh: bool = False

    @staticmethod
    def r2_object_key_for_local_path(local_path: str) -> str:
        """R2 object name in bucket root: basename of the configured local path."""
        return os.path.basename(os.path.normpath(local_path))

    def create_s3_client(self):
        """Create and return S3 client for R2"""
        if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, R2_BUCKET]):
            print("⚠️ R2 credentials not fully configured.")
            return None

        return boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            region_name="auto",
        )

    def download_from_r2(self, object_name: str, local_path: str) -> str:
        """
        Download file from R2 to local using persistent client.

        Returns:
            "ok" — file downloaded
            "missing" — object does not exist (empty bucket / first bootstrap)
            "error" — network, permissions, or other failure
            "no_client" — R2 client not configured
        """
        if not self.s3_client:
            print("⚠️ R2 client not configured; cannot download.")
            return "no_client"

        try:
            self.s3_client.download_file(R2_BUCKET, object_name, local_path)
            print(f"✓ Downloaded {object_name} from R2")
            return "ok"
        except ClientError as e:
            code = (e.response.get("Error") or {}).get("Code", "") or ""
            if code in ("404", "NoSuchKey", "NotFound"):
                print(f"ℹ️ R2 object not found (treated as missing): {object_name}")
                return "missing"
            print(f"⚠️ Could not download {object_name} from R2: {e}")
            return "error"
        except Exception as e:
            print(f"⚠️ Could not download {object_name} from R2: {e}")
            return "error"

    def is_r2_pull_strict(self) -> bool:
        """Strict when explicitly requested or running in GitHub Actions / generic CI."""
        if self.require_r2_pull:
            return True
        if os.environ.get("GITHUB_ACTIONS", "").lower() == "true":
            return True
        if os.environ.get("CI", "").lower() == "true":
            return True
        return False

    def validate_r2_baseline_pull(self, dim_status: str, fact_status: str) -> bool:
        """
        Enforce baseline load in strict mode so ephemeral CI never overwrites R2
        with a truncated local CSV after a failed download.

        Allowed in strict mode:
        - both "ok"
        - both "missing" (brand-new bucket; script will create CSVs and upload)

        Always fails strict mode if any status is "error" or "no_client", or exactly one of dim/fact is "missing".
        """
        if not self.is_r2_pull_strict():
            return True

        if dim_status == "ok" and fact_status == "ok":
            return True
        if dim_status == "missing" and fact_status == "missing":
            print(
                "\nℹ️ Strict R2 pull: both baseline objects missing — continuing as empty-bucket bootstrap.\n"
            )
            return True

        print("\n✗ Strict R2 pull failed: baseline CSVs must both load from R2 in CI.")
        print(f"   DIM status:  {dim_status}")
        print(f"   FACT status: {fact_status}")
        print(
            "   Fix credentials, bucket, object keys, or network. "
            "Do not upload from a partial baseline."
        )
        return False

    def upload_to_r2(self, file_path: str, object_name: str = None) -> bool:
        """Upload file to Cloudflare R2 using persistent client"""
        if not self.s3_client:
            return False

        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            with open(file_path, "rb") as f:
                self.s3_client.upload_fileobj(f, R2_BUCKET, object_name)

            print(f"✓ Uploaded {object_name} to R2")
            return True

        except Exception as e:
            print(f"✗ Failed to upload to R2: {e}")
            return False

    def get_utc_date_str(self) -> str:
        """Get current UTC date as YYYY-MM-DD string (no time)"""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def files_changed(self) -> bool:
        """Check if any files have changed by comparing hashes"""
        check_files = [self.dim_series_path, self.fact_progress_path]

        for file_path in check_files:
            hash_before = self.hashes_before.get(file_path)
            hash_after = get_file_hash(file_path)

            if hash_before != hash_after:
                print(f"📝 Change detected in {file_path}")
                self.hashes_after[file_path] = hash_after
                return True

        return False

    def pull_from_r2(self) -> Tuple[str, str]:
        """Pull CSV files from R2 if they exist. Returns (dim_status, fact_status)."""
        print("\n=== Pulling from R2 ===\n")

        files_to_pull = [
            (
                self.r2_object_key_for_local_path(self.dim_series_path),
                self.dim_series_path,
            ),
            (
                self.r2_object_key_for_local_path(self.fact_progress_path),
                self.fact_progress_path,
            ),
        ]

        dim_status = "error"
        fact_status = "error"

        for r2_name, local_path in files_to_pull:
            status = self.download_from_r2(r2_name, local_path)
            if status == "ok":
                self.hashes_before[local_path] = get_file_hash(local_path)
            if local_path == self.dim_series_path:
                dim_status = status
            else:
                fact_status = status

        return dim_status, fact_status

    def calculate_hashes_after(self):
        """Calculate hashes of CSV files after processing"""
        files = [
            self.dim_series_path,
            self.fact_progress_path,
        ]

        for file_path in files:
            if os.path.exists(file_path):
                self.hashes_after[file_path] = get_file_hash(file_path)

    # ==========================================
    # BATCH DIM PROCESSING (THE IO THRASHING FIX)
    # ==========================================

    def normalize_dim_row(self, row: Dict) -> Dict:
        """Ensure all DIM columns exist (back-compat with older CSV without image cols)."""
        for key in DIM_SERIES_FIELDNAMES:
            if key not in row or row[key] is None:
                row[key] = ""
            else:
                row[key] = str(row[key])
        return row

    def load_dim_cache(self):
        """Load entire DIM CSV into memory once"""
        if not os.path.exists(self.dim_series_path):
            print("ℹ️ No existing DIM file found")
            return

        rows = []
        with open(self.dim_series_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        for row in rows:
            self.normalize_dim_row(row)
            series_id = row["series_id"]
            self.dim_cache[series_id] = row

        print(f"✓ Loaded {len(self.dim_cache)} series into DIM cache")

    def flush_dim_cache(self):
        """Write entire DIM cache back to disk once"""
        if not self.dim_cache:
            print("ℹ️ No DIM data to flush")
            return

        fieldnames = DIM_SERIES_FIELDNAMES
        for row in self.dim_cache.values():
            self.normalize_dim_row(row)

        with open(self.dim_series_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.dim_cache.values())

        print(f"✓ Flushed {len(self.dim_cache)} series to DIM CSV")

    def upsert_dim_series_batch(self, series_id: str, title: str, current_chapter: str):
        """Batch upsert - update memory cache only"""
        if series_id in self.dim_cache:
            self.normalize_dim_row(self.dim_cache[series_id])
            # Update existing row if chapter advanced
            if self.dim_cache[series_id]["max_chapter"] != current_chapter:
                self.dim_cache[series_id]["max_chapter"] = current_chapter
                self.dim_updates_made[series_id] = True
                self.changes_made = True
        else:
            # New series
            self.dim_cache[series_id] = {
                "series_id": series_id,
                "title": title,
                "max_chapter": current_chapter,
                "type": "",
                "year": "",
                "authors": "",
                "artists": "",
                "genres": "",
                "cover_image_url": "",
                "cover_thumb_url": "",
            }
            self.dim_updates_made[series_id] = True
            self.changes_made = True

    # ==========================================
    # API & CORE LOGIC
    # ==========================================

    def authenticate(self):
        """Authenticate and get session token"""
        auth_url = f"{self.base_url}/account/login"
        payload = {"username": self.username, "password": self.password}

        try:
            response = requests.put(auth_url, json=payload)
            response.raise_for_status()
            data = response.json()
            self.token = data.get("context", {}).get("session_token")
            return bool(self.token)
        except Exception as e:
            print(f"✗ Authentication error: {e}")
            return False

    def numeric_to_base36(self, numeric_id: int) -> str:
        """Convert numeric series ID to base36 format"""
        chars = "0123456789abcdefghijklmnopqrstuvwxyz"
        if numeric_id == 0:
            return "0"

        digits = []
        while numeric_id:
            digits.append(chars[numeric_id % 36])
            numeric_id //= 36

        return "".join(reversed(digits))

    def base36_to_numeric(self, base36_id: str) -> int:
        """Convert base36 series ID to numeric ID"""
        return int(base36_id, 36)

    def get_user_reading_lists(self):
        """Returns a list of unique series_ids across ALL user lists + their progress info"""
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            response = requests.get(f"{self.base_url}/lists", headers=headers)
            response.raise_for_status()
            user_lists = response.json()
        except Exception as e:
            print(f"✗ Failed to get lists: {e}")
            return [], {}

        print(f"Found {len(user_lists)} lists")
        all_series = {}

        for lst in user_lists:
            list_id = lst["list_id"]
            print(f"Processing list: {lst.get('title', 'Unnamed')} (ID: {list_id})")

            page = 1
            perpage = 50

            while True:
                # RATE LIMITING: 1 second between requests
                time.sleep(1)

                try:
                    search_url = f"{self.base_url}/lists/{list_id}/search"
                    payload = {
                        "orderby": "title",
                        "order": "asc",
                        "page": page,
                        "perpage": perpage,
                    }

                    resp = requests.post(search_url, headers=headers, json=payload)
                    resp.raise_for_status()
                    data = resp.json()

                    results = data.get("results", [])
                    if not results:
                        break

                    for item in results:
                        record = item.get("record", {})
                        series = record.get("series", {})
                        numeric_id = series.get("id")

                        if numeric_id:
                            base36_id = self.numeric_to_base36(numeric_id)
                            status = record.get("status", {})
                            current_chapter = status.get("chapter")

                            all_series[base36_id] = {
                                "title": series.get("title", "Unknown"),
                                "current_chapter": current_chapter,
                            }

                    if len(results) < perpage:
                        break
                    page += 1

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 400:
                        print(f"  ✗ List may be empty or pagination ended")
                        break
                    else:
                        print(f"  ✗ Error: {e}")
                        break

        print(f"✓ Total unique tracked series: {len(all_series)}")
        return list(all_series.keys()), all_series

    def get_series_info(self, series_id: int):
        """Get complete series information with rate limiting"""
        time.sleep(1)  # RATE LIMITING

        series_url = f"{self.base_url}/series/{series_id}"
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            response = requests.get(series_url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching series {series_id}: {e}")
            return None

    @staticmethod
    def extract_cover_urls(series_info: Optional[dict]) -> Tuple[str, str]:
        """Parse series.image.url.original / .thumb from API response."""
        if not series_info:
            return "", ""
        img = series_info.get("image") or {}
        urls = img.get("url") or {}
        original = (urls.get("original") or "").strip()
        thumb = (urls.get("thumb") or "").strip()
        return original, thumb

    def merge_cover_images_into_row(self, row: Dict, series_info: Optional[dict]) -> bool:
        """
        Non-empty API URLs always overwrite stored values (including backfill).
        Empty API values do not clear existing stored URLs.
        """
        original, thumb = self.extract_cover_urls(series_info)
        changed = False
        if original and row.get("cover_image_url") != original:
            row["cover_image_url"] = original
            changed = True
        if thumb and row.get("cover_thumb_url") != thumb:
            row["cover_thumb_url"] = thumb
            changed = True
        if changed:
            self.changes_made = True
        return changed

    def get_last_chapter_for_series_date(self, series_id: str, date_utc: str) -> int:
        """Get the MAX current_chapter from entries strictly before today"""
        if not os.path.exists(self.fact_progress_path):
            return 0

        last_chapter = None
        with open(self.fact_progress_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["series_id"] == series_id:
                    if row["date_utc"] < date_utc:
                        try:
                            val = int(float(row["current_chapter"]))
                            last_chapter = (
                                val if last_chapter is None else max(last_chapter, val)
                            )
                        except ValueError:
                            pass

        return last_chapter if last_chapter is not None else 0

    def fact_progress_exists_for_date(self, series_id: str, date_utc: str) -> bool:
        """Check if a FACT record already exists for this series on this date"""
        if not os.path.exists(self.fact_progress_path):
            return False

        with open(self.fact_progress_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["series_id"] == series_id and row["date_utc"] == date_utc:
                    return True
        return False

    def append_fact_progress(
        self, series_id: str, start_chapter: int, current_chapter: str, date_utc: str
    ):
        """Append new fact record to FACT table"""
        fact_data = {
            "series_id": series_id,
            "start_chapter": str(start_chapter),
            "current_chapter": current_chapter,
            "date_utc": date_utc,
        }

        fieldnames = ["series_id", "start_chapter", "current_chapter", "date_utc"]
        file_exists = os.path.exists(self.fact_progress_path)

        with open(self.fact_progress_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(fact_data)

        print(f"✓ Appended FACT: {series_id} ch.{start_chapter}→{current_chapter}")
        self.changes_made = True

    def process_reading_lists(self):
        """Main processing with batch DIM updates"""
        print("\n=== Processing Reading Lists ===\n")

        if not self.authenticate():
            return False

        # LOAD DIM CACHE ONCE
        self.load_dim_cache()

        series_ids, series_metadata = self.get_user_reading_lists()
        if not series_ids:
            print("No series found.")
            return False

        today_utc = self.get_utc_date_str()
        processed = 0

        for series_id in series_ids:
            meta = series_metadata.get(series_id)
            current_chapter = meta.get("current_chapter")

            if current_chapter is None:
                continue

            # BATCH DIM UPSERT (memory only)
            self.upsert_dim_series_batch(
                series_id, meta.get("title"), str(current_chapter)
            )

            # FACT LOGIC (with progress validation)
            if not self.fact_progress_exists_for_date(series_id, today_utc):
                last_chapter = self.get_last_chapter_for_series_date(
                    series_id, today_utc
                )

                try:
                    current_chap_int = int(float(current_chapter))
                except (ValueError, TypeError):
                    current_chap_int = 0

                # ONLY record if progress made
                if current_chap_int > last_chapter:
                    start_chapter = last_chapter + 1
                    self.append_fact_progress(
                        series_id, start_chapter, current_chapter, today_utc
                    )
                    processed += 1

        # FLUSH DIM CACHE TO DISK ONCE
        self.flush_dim_cache()

        print(f"✓ Processed {processed}/{len(series_ids)} series with progress")
        return True

    def enrich_dim_missing_fields(self):
        """Enrich DIM rows: missing metadata and/or cover URLs; optional full image refresh."""
        if not self.dim_cache:
            print("No DIM cache to enrich")
            return

        label = (
            "Full cover image refresh (all series)"
            if self.full_image_refresh
            else "Enriching missing metadata / cover images"
        )
        print(f"\n=== {label} ===\n")

        enriched_count = 0
        for series_id, row in self.dim_cache.items():
            self.normalize_dim_row(row)
            has_meta = bool(row["type"] or row["year"] or row["authors"])
            has_cover = bool((row.get("cover_image_url") or "").strip())

            if not self.full_image_refresh and has_meta and has_cover:
                continue

            print(f"Enriching {row['title']} ({series_id})...", end=" ")

            try:
                numeric_id = self.base36_to_numeric(series_id)
                series_info = self.get_series_info(numeric_id)

                if series_info:
                    if not has_meta:
                        row["type"] = series_info.get("type", "")
                        row["year"] = str(series_info.get("year", ""))
                        row["authors"] = " | ".join(
                            [a.get("name", "") for a in series_info.get("authors", [])]
                        )
                        row["artists"] = " | ".join(
                            [
                                a.get("name", "")
                                for a in series_info.get("authors", [])
                                if a.get("type") == "Artist"
                            ]
                        )
                        row["genres"] = " | ".join(
                            [g.get("genre", "") for g in series_info.get("genres", [])]
                        )
                        self.changes_made = True

                    self.merge_cover_images_into_row(row, series_info)

                    enriched_count += 1
                    print("✓")
                else:
                    print("✗")
            except Exception as e:
                print(f"✗ {e}")

        if enriched_count > 0:
            self.flush_dim_cache()
            print(f"✓ Enriched {enriched_count} series")

    def dump_series_api_sample(
        self, series_id_base36: Optional[str], outfile: Optional[str] = None
    ) -> bool:
        """Auth + GET /series/{id}; write full JSON for inspecting image shape."""
        if not self.authenticate():
            return False

        out_path = outfile or self.series_api_sample_path

        sid = series_id_base36
        if not sid or sid == "__first__":
            if not os.path.exists(self.dim_series_path):
                print("✗ No DIM CSV; pass a base36 series_id, e.g. --dump-sample <id>")
                return False
            with open(self.dim_series_path, "r", newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                first = next(reader, None)
            if not first or not first.get("series_id"):
                print("✗ Could not read first series_id from DIM CSV")
                return False
            sid = first["series_id"].strip()
            print(f"Using first DIM series_id: {sid}")

        try:
            numeric_id = self.base36_to_numeric(sid)
        except ValueError as e:
            print(f"✗ Bad series id {sid!r}: {e}")
            return False

        series_info = self.get_series_info(numeric_id)
        if not series_info:
            print("✗ No data returned")
            return False

        path = os.path.abspath(out_path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(series_info, f, indent=2, ensure_ascii=False)

        orig, thumb = self.extract_cover_urls(series_info)
        print(f"\nWrote full response to {path}")
        print(f"cover_image_url (original): {orig or '(empty)'}")
        print(f"cover_thumb_url (thumb):    {thumb or '(empty)'}")
        return True

    def export_to_parquet(self):
        """Export CSV files to Parquet format using PyArrow"""
        print("\n=== Exporting to Parquet ===\n")

        try:
            if os.path.exists(self.dim_series_path):
                df = pd.read_csv(self.dim_series_path, encoding="utf-8-sig")
                table = pa.Table.from_pandas(df)
                pq.write_table(table, self.dim_series_parquet)
                print(f"✓ Exported {self.dim_series_parquet}")

            if os.path.exists(self.fact_progress_path):
                df = pd.read_csv(self.fact_progress_path, encoding="utf-8-sig")
                table = pa.Table.from_pandas(df)
                pq.write_table(table, self.fact_progress_parquet)
                print(f"✓ Exported {self.fact_progress_parquet}")

        except Exception as e:
            print(f"✗ Error exporting to Parquet: {e}")

    def upload_files_to_r2(self):
        """Upload CSV and Parquet files to R2"""
        print("\n=== Uploading to R2 ===\n")

        files_to_upload = [
            (
                self.dim_series_path,
                self.r2_object_key_for_local_path(self.dim_series_path),
            ),
            (
                self.fact_progress_path,
                self.r2_object_key_for_local_path(self.fact_progress_path),
            ),
            (
                self.dim_series_parquet,
                self.r2_object_key_for_local_path(self.dim_series_parquet),
            ),
            (
                self.fact_progress_parquet,
                self.r2_object_key_for_local_path(self.fact_progress_parquet),
            ),
        ]

        uploaded_count = 0
        for local_path, r2_name in files_to_upload:
            if os.path.exists(local_path):
                if self.upload_to_r2(local_path, r2_name):
                    uploaded_count += 1

        print(f"\n✓ Uploaded {uploaded_count} files to R2")
        return uploaded_count > 0

    def main(self):
        """Main execution function"""
        # Step 1: Pull from R2
        dim_status, fact_status = self.pull_from_r2()
        if not self.validate_r2_baseline_pull(dim_status, fact_status):
            return "failed"

        # Step 2: Process reading lists (batch DIM processing)
        if not self.process_reading_lists():
            return "failed"

        # Step 3: Enrich metadata
        self.enrich_dim_missing_fields()

        # Step 4: Export Parquet (only if changes made)
        if self.changes_made:
            self.export_to_parquet()
        else:
            print("\nℹ️ No changes detected, skipping Parquet export")

        # Step 5: Calculate hashes and upload if changed
        self.calculate_hashes_after()
        if self.files_changed():
            self.upload_files_to_r2()
            return "uploaded"
        else:
            print("\n✓ No changes detected - skipping upload")
            return "no-change"


def main():
    """Main entry point (uses argparse when run as script)."""
    import argparse

    username = os.getenv("MANGAUPDATES_USERNAME")
    password = os.getenv("MANGAUPDATES_PASSWORD")

    parser = argparse.ArgumentParser(
        description="MangaUpdates DIM/FACT warehouse (CSV + Parquet, optional R2)."
    )
    parser.add_argument(
        "--dump-sample",
        nargs="?",
        const="__first__",
        default=None,
        metavar="SERIES_BASE36",
        help="Authenticate, GET /series/{id}, write JSON sample (SERIES_API_SAMPLE_PATH). "
        "Default: first series_id in local DIM CSV.",
    )
    parser.add_argument(
        "--full-image-refresh",
        action="store_true",
        help="Re-fetch every series in DIM to refresh cover_image_url / cover_thumb_url (slow).",
    )
    parser.add_argument(
        "--require-r2-pull",
        action="store_true",
        help="Abort unless both baseline CSVs load from R2 (or both keys missing for empty bucket). "
        "Also enabled when CI or GITHUB_ACTIONS is true.",
    )
    args = parser.parse_args()

    if not username or not password:
        print("✗ Set MANGAUPDATES_USERNAME and MANGAUPDATES_PASSWORD in .env file")
        return "failed"

    dw = MangaUpdatesDataWarehouse(
        username, password, require_r2_pull=args.require_r2_pull
    )
    dw.full_image_refresh = args.full_image_refresh

    if args.dump_sample is not None:
        sid = None if args.dump_sample == "__first__" else args.dump_sample
        ok = dw.dump_series_api_sample(sid)
        return "dumped" if ok else "failed"

    return dw.main()


if __name__ == "__main__":
    timestamp = int(time.time())
    exit_code = 0
    try:
        result = main()
        if result == "uploaded":
            send_discord_alert(
                f"✅ MangaUpdate Script — Uploaded to R2 at <t:{timestamp}:f>"
            )
        elif result == "no-change":
            send_discord_alert(
                f"✅ MangaUpdate Script — No changes to upload. Last checked at <t:{timestamp}:f>"
            )
        elif result == "dumped":
            pass
        elif result == "failed":
            send_discord_alert(
                f"❌ MangaUpdate Script failed at <t:{timestamp}:f> (auth, R2 baseline pull, or processing)."
            )
            exit_code = 1
        else:
            send_discord_alert(
                f"⚠️ MangaUpdate Script - Returned an unknown result at <t:{timestamp}:f>"
            )
            exit_code = 1
    except Exception as e:
        error_msg = f"❌ MangaUpdate Script failed at <t:{timestamp}:f>: {e}"
        print(error_msg)
        send_discord_alert(error_msg)
        exit_code = 1
    sys.exit(exit_code)
