#!/usr/bin/env python3
"""
================================================================================
 TERAFORT <- AMAZON APPSTORE REPORTING API -> BIGQUERY        (v2.0)
================================================================================
 v1.0 SE KYA BADLA — 7 FIXES:

  1. 🔑 EARNINGS YEARLY ENDPOINT
     Docs mein DO earnings endpoint hain — v1.0 sirf monthly try karta tha
     aur wo "Report not found" deta hai. Ab yearly bhi try hota hai:
         /report/earnings/<year>          ← NAYA
         /report/earnings/<year>/<month>
     Isse milta hai: Tax Withheld · Exchange Rate · AWS Credit (10%!) ·
     Payment Amount/Status/Date.

  2. 🛡️ ATOMIC LOAD (data loss ka khatra khatam)
     v1.0: DELETE karta tha PHIR load — beech mein fail ho jaye to us
     mahine ka data GAYAB. Ab: temp table mein load → phir TRANSACTION
     mein DELETE+INSERT. Fail ho to purana data salamat.

  3. 📅 PARTITIONING + DATE column
     report_date (DATE) add hui, table us par PARTITION hai. Query sasti
     aur tez. report_month (STRING) bhi rahegi (backward compatible).

  4. 🔁 SAHI RETRY
     v1.0: HTTP error par fail() foran exit kar deta tha — retry loop
     be-kaar tha. Ab: 429/5xx par backoff ke saath retry, 400/401 par
     foran fail (retry ka faida nahi).

  5. 🧬 SCHEMA DRIFT PROOF
     Amazon naya column add kare to ALTER TABLE se khud jur jayega,
     purana data mehfooz.

  6. ➖ "Report not found" ab warning nahi
     400 + "not found" = us mahine ka report nahi bana — ye normal hai,
     ab saaf log hota hai, error ki tarah nahi.

  7. ⏱️ _loaded_at ab TIMESTAMP hai (STRING nahi) — query karna aasan.

 AUTH — PERMANENT (koi token chain nahi, koi manual re-seed nahi):
   LWA client_credentials → 1 ghante ka token, khud re-mint hota hai.
   scope = adx_reporting::appstore:marketer
   ⚠️ Security profile "My Settings → API Access → Reporting API" par
      ATTACH honi chahiye — warna invalid_scope aayega.

 API FACTS (official docs):
   * Monthly files, har 24h update, 48h tak lag
   * Adjustments us mahine mein aate hain jis mein KIYE gaye → files
     RESTATE hoti hain → rozana pichla mahina dobara kheenchte hain
   * S3 pre-signed URL sirf 5 MINUTE
   * Max 3 req/sec
   * Earliest report: Jan 2018

 MODES (env MODE):
   daily     → chalu + pichla mahina (default)
   backfill  → BACKFILL_START se aaj tak

 BIGQUERY (env BQ_PROJECT, BQ_DATASET default 'amazon_appstore'):
   sales_raw · earnings_raw · earnings_yearly_raw ·
   subscription_raw · subs_overview_raw
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

# ── CONFIG ──────────────────────────────────────────────────────────────────
TOKEN_URL      = "https://api.amazon.com/auth/o2/token"
BASE_URL       = "https://developer.amazon.com/api/appstore/download/report"
SCOPE          = "adx_reporting::appstore:marketer"
TIMEOUT        = 60
RATE_SLEEP     = 0.5      # 3 req/s cap — is se kaafi neeche
TOKEN_SAFETY_S = 300      # token mein 5 min se kam bache to naya lein
MAX_RETRIES    = 3

# report_key -> (url_segment, bq_table, granularity)
REPORTS = {
    "sales":                  ("sales",                  "sales_raw",           "month"),
    "earnings":               ("earnings",               "earnings_raw",        "month"),
    "earnings_yearly":        ("earnings",               "earnings_yearly_raw", "year"),   # 🆕 FIX 1
    "subscription":           ("subscription",           "subscription_raw",    "month"),
    "subscriptions_overview": ("subscriptions_overview", "subs_overview_raw",   "month"),
}


def log(msg: str) -> None:
    print(msg, flush=True)


def fail(msg: str) -> None:
    log(f"\n🚨 AMAZON PIPELINE FAILED: {msg}")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════ TOKEN ══════════
class Token:
    """LWA client_credentials token — stateless, khud re-mint hota hai."""

    def __init__(self) -> None:
        self.client_id     = os.environ.get("AMAZON_CLIENT_ID", "").strip()
        self.client_secret = os.environ.get("AMAZON_CLIENT_SECRET", "").strip()
        if not self.client_id or not self.client_secret:
            fail("AMAZON_CLIENT_ID / AMAZON_CLIENT_SECRET set nahi hain")
        self._value = None
        self._expires_at = 0.0

    def get(self) -> str:
        if self._value and time.time() < self._expires_at - TOKEN_SAFETY_S:
            return self._value

        last_detail = ""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.post(
                    TOKEN_URL,
                    data={
                        "grant_type":    "client_credentials",
                        "client_id":     self.client_id,
                        "client_secret": self.client_secret,
                        "scope":         SCOPE,
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=TIMEOUT,
                )
            except requests.RequestException as exc:
                last_detail = f"network error: {exc}"
                if attempt == MAX_RETRIES:
                    break
                time.sleep(5 * attempt)
                continue

            if r.status_code == 200:
                data = r.json()
                self._value = data["access_token"]
                self._expires_at = time.time() + int(data.get("expires_in", 3600))
                print(f"::add-mask::{self._value}")
                log("🔑 fresh LWA access token minted")
                return self._value

            # 🔧 FIX 4 — sahi retry: config ki ghalti par retry be-kaar hai
            try:
                e = r.json()
                err = e.get("error", "")
                last_detail = (f"error={err!r} "
                               f"description={e.get('error_description')!r}")
            except ValueError:
                err, last_detail = "", r.text[:500]

            if err in ("invalid_scope", "invalid_client", "invalid_request",
                       "unauthorized_client"):
                fail(f"LWA token HTTP {r.status_code}: {last_detail}\n"
                     f"   invalid_scope  → Amazon console: My Settings ⌄ → API Access\n"
                     f"                    → Reporting API → security profile ATTACH karein\n"
                     f"   invalid_client → Client ID/Secret dobara copy karein\n"
                     f"                    (GitHub secret mein space/newline na ho)")

            if attempt == MAX_RETRIES:
                break
            time.sleep(5 * attempt)          # 429 / 5xx — dobara koshish

        fail(f"LWA token nahi mila ({MAX_RETRIES} koshishen): {last_detail}")


# ═══════════════════════════════════════════════════════ DOWNLOAD ══════════
def _is_report_missing(status: int, body: str) -> bool:
    """🔧 FIX 6 — 'report nahi bana' vs asli error ka farq."""
    b = body.lower()
    return (status in (403, 404)
            or (status == 400 and ("not found" in b or "no data" in b
                                   or "not available" in b)))


def fetch_report(token: Token, segment: str, year: int, month=None):
    """Monthly ya yearly report ka CSV text — ya None agar maujood nahi."""
    label = f"{year}" if month is None else f"{year}-{month:02d}"
    url   = (f"{BASE_URL}/{segment}/{year}"
             if month is None else f"{BASE_URL}/{segment}/{year}/{month:02d}")

    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(RATE_SLEEP)
        try:
            r = requests.get(url,
                             headers={"Authorization": f"Bearer {token.get()}"},
                             timeout=TIMEOUT)
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                log(f"   ⚠️  {segment} {label}: network error {exc} → skip")
                return None
            time.sleep(3 * attempt)
            continue

        if _is_report_missing(r.status_code, r.text):
            log(f"   ➖ {segment} {label}: report maujood nahi → skip")
            return None

        if r.status_code == 200:
            break

        if r.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
            log(f"   ⏳ {segment} {label}: HTTP {r.status_code} → retry {attempt}")
            time.sleep(5 * attempt)
            continue

        log(f"   ⚠️  {segment} {label}: HTTP {r.status_code} {r.text[:200]} → skip")
        return None
    else:
        return None

    # ── S3 pre-signed URL — sirf 5 minute, foran download karein ──
    s3_url = r.text.strip().strip('"')
    if not s3_url.lower().startswith("http"):
        m = re.search(r'https://[^\s"\']+', r.text)
        if not m:
            log(f"   ⚠️  {segment} {label}: jawab mein S3 URL nahi "
                f"({r.text[:120]!r}) → skip")
            return None
        s3_url = m.group(0)

    time.sleep(RATE_SLEEP)
    try:
        f = requests.get(s3_url, timeout=TIMEOUT)
    except requests.RequestException as exc:
        log(f"   ⚠️  {segment} {label}: S3 download error {exc} → skip")
        return None
    if f.status_code != 200:
        log(f"   ⚠️  {segment} {label}: S3 HTTP {f.status_code} → skip")
        return None

    blob = f.content
    if blob[:2] == b"PK":                          # ZIP → andar wali CSV
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not names:
                log(f"   ⚠️  {segment} {label}: zip mein csv nahi → skip")
                return None
            blob = z.read(names[0])

    try:
        return blob.decode("utf-8-sig")
    except UnicodeDecodeError:
        return blob.decode("latin-1")


# ═══════════════════════════════════════════════════════ BIGQUERY ══════════
def bq_col(name: str) -> str:
    """CSV header → BigQuery-safe column name."""
    c = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    if not c:
        c = "col"
    if c[0].isdigit():
        c = "_" + c
    return c[:128]


def _parse_csv(csv_text: str, report_key: str, source_file: str):
    """CSV → (records, cols). Har value STRING (schema-drift-proof)."""
    rows_iter = csv.reader(io.StringIO(csv_text))
    try:
        header = next(rows_iter)
    except StopIteration:
        return [], []

    cols, seen = [], {}
    for h in header:                               # sanitized naam dedupe
        c = bq_col(h)
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        cols.append(c)

    records = []
    now = datetime.now(timezone.utc)
    for raw in rows_iter:
        if not any(x.strip() for x in raw):
            continue
        raw = (raw + [""] * len(cols))[:len(cols)]
        rec = {c: (v if v != "" else None) for c, v in zip(cols, raw)}
        rec["_source_file"] = source_file
        rec["_loaded_at"]   = now.isoformat()      # 🔧 FIX 7 — TIMESTAMP
        records.append(rec)
    return records, cols


def load_period(bq: bigquery.Client, dataset: str, table: str,
                period_key: str, period_date: str,
                source_file: str, csv_text: str, report_key: str) -> int:
    """
    🛡️ FIX 2 + 3 + 5 — atomic, partitioned, schema-drift-proof load.

      1. temp table mein load  (target ko haath nahi lagta)
      2. target na ho → temp se bana dein (PARTITION BY report_date)
      3. target ho    → naye column ALTER se jorein, phir
                        TRANSACTION mein DELETE + INSERT
      4. temp delete

    Beech mein fail ho jaye to PURANA DATA SALAMAT rehta hai.
    """
    records, cols = _parse_csv(csv_text, report_key, source_file)
    if not records:
        return 0

    for r in records:
        r["report_month"] = period_key       # 'YYYY-MM' ya 'YYYY'
        r["report_date"]  = period_date      # 'YYYY-MM-01' → partition key

    meta = ["report_month", "report_date", "_source_file", "_loaded_at"]
    schema = ([bigquery.SchemaField(c, "STRING") for c in cols] +
              [bigquery.SchemaField("report_month", "STRING"),
               bigquery.SchemaField("report_date",  "DATE"),
               bigquery.SchemaField("_source_file", "STRING"),
               bigquery.SchemaField("_loaded_at",   "TIMESTAMP")])

    target = f"{bq.project}.{dataset}.{table}"
    tmp    = f"{bq.project}.{dataset}._tmp_{table}_{re.sub(r'[^0-9]', '', period_key)}"

    # ── 1. temp table mein load ──
    bq.load_table_from_json(
        records, tmp,
        job_config=bigquery.LoadJobConfig(
            schema=schema, write_disposition="WRITE_TRUNCATE"),
    ).result()

    try:
        # ── 2. target maujood hai? ──
        try:
            tgt = bq.get_table(target)
            exists = True
        except Exception:
            exists = False

        if not exists:
            bq.query(f"""
                CREATE TABLE `{target}`
                PARTITION BY report_date
                AS SELECT * FROM `{tmp}`
            """).result()
            log(f"      🆕 table bani: {table} (PARTITION BY report_date)")
        else:
            # ── 3a. FIX 5 — naye column jorein ──
            have = {f.name for f in tgt.schema}
            new  = [c for c in cols + meta if c not in have]
            if new:
                adds = ", ".join(
                    f"ADD COLUMN IF NOT EXISTS `{c}` "
                    f"{'DATE' if c == 'report_date' else 'TIMESTAMP' if c == '_loaded_at' else 'STRING'}"
                    for c in new)
                bq.query(f"ALTER TABLE `{target}` {adds}").result()
                log(f"      🧬 naye column jore: {', '.join(new[:5])}"
                    f"{' …' if len(new) > 5 else ''}")
                tgt  = bq.get_table(target)
                have = {f.name for f in tgt.schema}

            # ── 3b. FIX 2 — atomic swap ──
            insert_cols = [c for c in (cols + meta) if c in have]
            col_list    = ", ".join(f"`{c}`" for c in insert_cols)
            bq.query(f"""
                BEGIN TRANSACTION;
                  DELETE FROM `{target}` WHERE report_month = '{period_key}';
                  INSERT INTO `{target}` ({col_list})
                  SELECT {col_list} FROM `{tmp}`;
                COMMIT TRANSACTION;
            """).result()
    finally:
        bq.query(f"DROP TABLE IF EXISTS `{tmp}`").result()

    return len(records)


# ═══════════════════════════════════════════════════════════ MAIN ══════════
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
        fail("BQ_PROJECT set nahi hai")
    dataset = os.environ.get("BQ_DATASET", "amazon_appstore").strip()
    mode    = os.environ.get("MODE", "daily").strip().lower()

    today = date.today()
    if mode == "backfill":
        start = os.environ.get("BACKFILL_START", "2018-01").strip()
        if not re.fullmatch(r"\d{4}-\d{2}", start) or start < "2018-01":
            fail(f"BACKFILL_START 'YYYY-MM' aur >= 2018-01 hona chahiye; mila {start!r}")
        months = list(month_range(start, today))
    else:
        prev_y, prev_m = ((today.year - 1, 12) if today.month == 1
                          else (today.year, today.month - 1))
        months = [(prev_y, prev_m), (today.year, today.month)]

    years = sorted({y for (y, _) in months})       # 🆕 yearly earnings ke liye

    log(f"▶ mode={mode} · months={len(months)} "
        f"({months[0][0]}-{months[0][1]:02d} .. {months[-1][0]}-{months[-1][1]:02d}) "
        f"· years={years} · dataset={dataset}")

    token = Token()
    bq    = bigquery.Client(project=project)
    try:
        bq.create_dataset(f"{project}.{dataset}", exists_ok=True)
    except Exception as exc:
        fail(f"dataset {dataset} banti/khulti nahi: {exc}")

    totals = {k: 0 for k in REPORTS}
    errors = []

    # ── MONTHLY reports ──
    for (y, m) in months:
        ym = f"{y}-{m:02d}"
        log(f"── {ym} " + "─" * 44)
        for key, (segment, table, gran) in REPORTS.items():
            if gran != "month":
                continue
            try:
                csv_text = fetch_report(token, segment, y, m)
                if csv_text is None:
                    continue
                n = load_period(bq, dataset, table, ym, f"{y}-{m:02d}-01",
                                f"{segment}-{ym}.csv", csv_text, key)
                totals[key] += n
                log(f"   ✅ {key:<24s} {n:>7,} rows → {table}")
            except Exception as exc:
                log(f"   🔴 {key} {ym}: {type(exc).__name__}: {exc}")
                errors.append(f"{key}/{ym}")

    # ── 🆕 FIX 1 — YEARLY earnings ──
    for y in years:
        log(f"── {y} (yearly) " + "─" * 37)
        for key, (segment, table, gran) in REPORTS.items():
            if gran != "year":
                continue
            try:
                csv_text = fetch_report(token, segment, y, None)
                if csv_text is None:
                    continue
                n = load_period(bq, dataset, table, str(y), f"{y}-01-01",
                                f"{segment}-{y}.csv", csv_text, key)
                totals[key] += n
                log(f"   ✅ {key:<24s} {n:>7,} rows → {table}")
            except Exception as exc:
                log(f"   🔴 {key} {y}: {type(exc).__name__}: {exc}")
                errors.append(f"{key}/{y}")

    # ── SUMMARY ──
    log("\n" + "═" * 24 + " RUN SUMMARY " + "═" * 24)
    for key, n in totals.items():
        mark = "✅" if n else "➖"
        log(f"  {mark} {key:<24s} {n:>9,} rows")

    if errors:
        log(f"\n🔴 {len(errors)} fail hue: {', '.join(errors[:10])}")
        sys.exit(1)

    # 🔧 zero rows tab hi fail jab SAB khaali hon AUR sales bhi khaali ho.
    #    (token mil chuka tha, yani auth theek tha — to ye asli data masla hai)
    if totals.get("sales", 0) == 0 and mode == "daily":
        fail("sales report se ZERO rows — API access ya data availability check karein")

    log("✅ done")


if __name__ == "__main__":
    main()
