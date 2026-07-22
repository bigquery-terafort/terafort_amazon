#!/usr/bin/env python3
"""
================================================================================
 TERAFORT <- AMAZON APPSTORE REPORTING API -> BIGQUERY        (v1.0)
================================================================================
 Pulls EVERYTHING the Amazon Appstore Reporting API offers:
   1. sales                  /api/appstore/download/report/sales/<Y>/<M>
   2. earnings               /api/appstore/download/report/earnings/<Y>/<M>
   3. subscription           /api/appstore/download/report/subscription/<Y>/<M>
   4. subscriptions_overview /api/appstore/download/report/subscriptions_overview/<Y>/<M>

 AUTH (permanent -- NO token chain, NO manual re-seed EVER):
   LWA client_credentials: POST https://api.amazon.com/auth/o2/token
   scope=adx_reporting::appstore:marketer  -> Bearer token (1h). Each run
   mints fresh tokens from CLIENT_ID/SECRET; auto re-mints mid-run when the
   token nears expiry (long backfills).

 API FACTS (from official docs, 2026-06-29 revision):
   * Monthly files, updated every 24h, data lag up to 48h; adjustments land
     in the month they are MADE -> files RESTATE -> we re-pull recent months
     daily and replace them (delete+insert per month = idempotent).
   * S3 pre-signed URL valid 5 minutes -> download immediately.
   * Max 3 requests/sec -> we sleep between calls.
   * Earliest report: Jan 2018.
   * 403/"not available"/404 for a month = no data or endpoint not enabled
     on the account -> logged and SKIPPED, never fatal (so one missing
     report type can't kill the whole pull).

 MODES (env MODE):
   daily     (default) -> current month + previous month, all 4 reports
   backfill            -> BACKFILL_START (YYYY-MM, default 2018-01) .. now

 BIGQUERY (env BQ_PROJECT, BQ_DATASET default 'amazon_appstore'):
   Tables: sales_raw / earnings_raw / subscription_raw / subs_overview_raw
   All CSV columns stored AS STRING (schema-drift-proof; typing happens in
   the staging layer), plus:
     report_month  (YYYY-MM)  -- replace-key
     _source_file, _loaded_at
   Idempotent load: DELETE report_month rows -> load fresh file.
================================================================================
"""
import csv
import io
import os
import re
import sys
import time
import zipfile
from datetime import date, datetime, timezone

import requests
from google.cloud import bigquery

TOKEN_URL = "https://api.amazon.com/auth/o2/token"
BASE_URL = "https://developer.amazon.com/api/appstore/download/report"
SCOPE = "adx_reporting::appstore:marketer"
TIMEOUT = 60
RATE_SLEEP = 0.5          # 3 req/s cap -> stay well under
TOKEN_SAFETY_S = 300      # re-mint token if <5 min of life left

REPORTS = {                # report_key -> (url_segment, bq_table)
    "sales":                  ("sales",                  "sales_raw"),
    "earnings":               ("earnings",               "earnings_raw"),
    "subscription":           ("subscription",           "subscription_raw"),
    "subscriptions_overview": ("subscriptions_overview", "subs_overview_raw"),
}


def log(msg: str) -> None:
    print(msg, flush=True)


def fail(msg: str) -> None:
    log(f"\n🚨 AMAZON PIPELINE FAILED: {msg}")
    sys.exit(1)


# ---------------------------------------------------------------- token ----
class Token:
    """LWA client_credentials token with auto re-mint (no chain, stateless)."""

    def __init__(self) -> None:
        self.client_id = os.environ.get("AMAZON_CLIENT_ID", "").strip()
        self.client_secret = os.environ.get("AMAZON_CLIENT_SECRET", "").strip()
        if not self.client_id or not self.client_secret:
            fail("AMAZON_CLIENT_ID / AMAZON_CLIENT_SECRET not set")
        self._value = None
        self._expires_at = 0.0

    def get(self) -> str:
        if self._value and time.time() < self._expires_at - TOKEN_SAFETY_S:
            return self._value
        for attempt in range(1, 4):
            try:
                r = requests.post(
                    TOKEN_URL,
                    data={
                        "grant_type": "client_credentials",
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                        "scope": SCOPE,
                    },
                    headers={"Content-Type":
                             "application/x-www-form-urlencoded"},
                    timeout=TIMEOUT,
                )
            except requests.RequestException as exc:
                if attempt == 3:
                    fail(f"LWA token network error: {exc}")
                time.sleep(5 * attempt)
                continue
            if r.status_code != 200:
                fail(f"LWA token HTTP {r.status_code}: {r.text[:300]} "
                     f"(check client id/secret + security profile mapping)")
            data = r.json()
            self._value = data["access_token"]
            self._expires_at = time.time() + int(data.get("expires_in", 3600))
            print(f"::add-mask::{self._value}")
            log("🔑 fresh LWA access token minted")
            return self._value
        fail("unreachable")


# ------------------------------------------------------------- download ----
def fetch_report(token: Token, segment: str, year: int, month: int):
    """Return CSV text for a monthly report, or None if not available."""
    url = f"{BASE_URL}/{segment}/{year}/{month:02d}"
    time.sleep(RATE_SLEEP)
    try:
        r = requests.get(url,
                         headers={"Authorization": f"Bearer {token.get()}"},
                         timeout=TIMEOUT)
    except requests.RequestException as exc:
        log(f"   ⚠️  {segment} {year}-{month:02d}: network error {exc} -> skip")
        return None

    if r.status_code in (403, 404):
        log(f"   ➖ {segment} {year}-{month:02d}: HTTP {r.status_code} "
            f"(no data / not enabled) -> skip")
        return None
    if r.status_code != 200:
        log(f"   ⚠️  {segment} {year}-{month:02d}: HTTP {r.status_code} "
            f"{r.text[:200]} -> skip")
        return None

    s3_url = r.text.strip().strip('"')
    if not s3_url.lower().startswith("http"):
        # some responses may be JSON-wrapped; try to find a URL inside
        m = re.search(r'https://[^\s"\']+', r.text)
        if not m:
            log(f"   ⚠️  {segment} {year}-{month:02d}: no S3 URL in response "
                f"({r.text[:120]!r}) -> skip")
            return None
        s3_url = m.group(0)

    time.sleep(RATE_SLEEP)
    try:
        f = requests.get(s3_url, timeout=TIMEOUT)   # 5-min validity: use NOW
    except requests.RequestException as exc:
        log(f"   ⚠️  {segment} {year}-{month:02d}: S3 download error {exc} "
            f"-> skip")
        return None
    if f.status_code != 200:
        log(f"   ⚠️  {segment} {year}-{month:02d}: S3 HTTP {f.status_code} "
            f"-> skip")
        return None

    blob = f.content
    if blob[:2] == b"PK":                      # zip -> first CSV inside
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not names:
                log(f"   ⚠️  {segment} {year}-{month:02d}: zip has no csv "
                    f"-> skip")
                return None
            blob = z.read(names[0])
    try:
        return blob.decode("utf-8-sig")
    except UnicodeDecodeError:
        return blob.decode("latin-1")


# ------------------------------------------------------------- bigquery ----
def bq_col(name: str) -> str:
    """CSV header -> BigQuery-safe column name."""
    c = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    if not c:
        c = "col"
    if c[0].isdigit():
        c = "_" + c
    return c[:128]


def load_month(bq: bigquery.Client, dataset: str, table: str,
               report_month: str, source_file: str, csv_text: str) -> int:
    rows_iter = csv.reader(io.StringIO(csv_text))
    try:
        header = next(rows_iter)
    except StopIteration:
        return 0
    cols = []
    seen = {}
    for h in header:                       # dedupe sanitized names
        c = bq_col(h)
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        cols.append(c)

    records = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for raw in rows_iter:
        if not any(x.strip() for x in raw):
            continue
        raw = (raw + [""] * len(cols))[:len(cols)]
        rec = {c: (v if v != "" else None) for c, v in zip(cols, raw)}
        rec["report_month"] = report_month
        rec["_source_file"] = source_file
        rec["_loaded_at"] = now_iso
        records.append(rec)
    if not records:
        return 0

    table_id = f"{bq.project}.{dataset}.{table}"
    schema = ([bigquery.SchemaField(c, "STRING") for c in cols]
              + [bigquery.SchemaField("report_month", "STRING"),
                 bigquery.SchemaField("_source_file", "STRING"),
                 bigquery.SchemaField("_loaded_at", "STRING")])

    # table exists? -> idempotent replace of this month
    try:
        bq.get_table(table_id)
        bq.query(f"DELETE FROM `{table_id}` "
                 f"WHERE report_month = '{report_month}'").result()
    except Exception:
        pass                                   # first load creates the table

    job = bq.load_table_from_json(
        records, table_id,
        job_config=bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        ))
    job.result()
    return len(records)


# ----------------------------------------------------------------- main ----
def month_range(start_ym: str, end_d: date):
    y, m = int(start_ym[:4]), int(start_ym[5:7])
    while (y, m) <= (end_d.year, end_d.month):
        yield y, m
        m += 1
        if m == 13:
            y, m = y + 1, 1


def main() -> None:
    project = os.environ.get("BQ_PROJECT", "").strip()
    if not project:
        fail("BQ_PROJECT not set")
    dataset = os.environ.get("BQ_DATASET", "amazon_appstore").strip()
    mode = os.environ.get("MODE", "daily").strip().lower()

    today = date.today()
    if mode == "backfill":
        start = os.environ.get("BACKFILL_START", "2018-01").strip()
        if not re.fullmatch(r"\d{4}-\d{2}", start) or start < "2018-01":
            fail(f"BACKFILL_START must be YYYY-MM and >= 2018-01 "
                 f"(earliest available); got {start!r}")
        months = list(month_range(start, today))
    else:
        prev_y, prev_m = ((today.year - 1, 12) if today.month == 1
                          else (today.year, today.month - 1))
        months = [(prev_y, prev_m), (today.year, today.month)]

    log(f"▶ mode={mode} months={len(months)} "
        f"({months[0][0]}-{months[0][1]:02d} .. "
        f"{months[-1][0]}-{months[-1][1]:02d}) dataset={dataset}")

    token = Token()
    bq = bigquery.Client(project=project)
    try:
        bq.create_dataset(f"{project}.{dataset}", exists_ok=True)
    except Exception as exc:
        fail(f"cannot create/access dataset {dataset}: {exc}")

    totals = {k: 0 for k in REPORTS}
    for (y, m) in months:
        ym = f"{y}-{m:02d}"
        log(f"── {ym} " + "─" * 40)
        for key, (segment, table) in REPORTS.items():
            csv_text = fetch_report(token, segment, y, m)
            if csv_text is None:
                continue
            n = load_month(bq, dataset, table, ym,
                           f"{segment}-{ym}.csv", csv_text)
            totals[key] += n
            log(f"   ✅ {key:<24s} {n:>7,} rows -> {table}")

    log("\n════ RUN SUMMARY ════")
    for key, n in totals.items():
        log(f"  {key:<24s} {n:>9,} rows loaded")
    if all(n == 0 for n in totals.values()):
        fail("ZERO rows loaded across all report types -- check API access "
             "(security profile attached?) or data availability")
    log("✅ done")


if __name__ == "__main__":
    main()
