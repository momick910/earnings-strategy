#!/usr/bin/env python3
"""
Earnings Trading Strategy Scanner
Fetches upcoming earnings, analyzes historical post-earnings reactions,
scores each stock, and generates LONG/SHORT signals.

Dependencies: yfinance, pandas
  (bs4/beautifulsoup4 is used indirectly — it is already a required
  dependency of yfinance, so no separate install is needed)
"""

import html as _html
import json
import math
import os
import smtplib
import ssl
import threading
import warnings
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

warnings.filterwarnings("ignore")


# ─────────────────────────────────────────────
# STEP 1: Ticker universe — multi-index scraper
# ─────────────────────────────────────────────

def _clean_tickers(raw_list, suffix="", dot_dash=True):
    """
    Normalise raw ticker strings scraped from Wikipedia:
      - strip footnote markers like "[1]"
      - replace "." with "-" for US tickers (BRK.B → BRK-B) when dot_dash=True
      - append exchange suffix (e.g. ".DE" for German tickers)
    """
    out = []
    for t in raw_list:
        t = str(t).split("[")[0].strip()
        if not t or t.lower() == "nan" or t in ("—", "-", ""):
            continue
        if dot_dash:
            t = t.replace(".", "-")
        out.append(t + suffix)
    return out


def _scrape_wikipedia_tickers(url, col_hints, suffix="", dot_dash=True):
    """
    Attempt to scrape ticker symbols from a Wikipedia wikitable.

    Two-pass strategy:
      1. pd.read_html  — fast, but requires lxml (may not be installed)
      2. BeautifulSoup — uses Python's built-in html.parser, no lxml needed
         (bs4 is already a transitive dependency of yfinance)

    col_hints: ordered list of column-header substrings to search for
               (case-insensitive), e.g. ["Ticker symbol", "Symbol", "Ticker"]
    Returns a list of cleaned ticker strings, or [] on total failure.
    """
    # Pass 1 — pd.read_html
    try:
        dfs = pd.read_html(url)
        for df in dfs:
            for hint in col_hints:
                matches = [c for c in df.columns if hint.lower() in str(c).lower()]
                if matches:
                    result = _clean_tickers(
                        df[matches[0]].dropna().astype(str).tolist(),
                        suffix, dot_dash
                    )
                    if result:
                        return result
    except Exception:
        pass

    # Pass 2 — BeautifulSoup with html.parser
    try:
        import requests
        from bs4 import BeautifulSoup

        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; earnings-scanner/1.0)"},
            timeout=15,
        )
        if r.status_code != 200:
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        for table in soup.find_all("table", {"class": "wikitable"}):
            # Build a flat header list from all <th> cells in header rows
            headers = [th.get_text(strip=True) for th in table.find_all("th")]

            col_idx = None
            for hint in col_hints:
                for i, h in enumerate(headers):
                    if hint.lower() in h.lower():
                        col_idx = i
                        break
                if col_idx is not None:
                    break

            if col_idx is None:
                continue

            raw = []
            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) > col_idx:
                    raw.append(cells[col_idx].get_text(strip=True))

            result = _clean_tickers(raw, suffix, dot_dash)
            if result:
                return result
    except Exception:
        pass

    return []


# ── Per-index fetchers ─────────────────────────────────────────────────────────

def _fetch_sp500():
    tickers = _scrape_wikipedia_tickers(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        ["Symbol", "Ticker"],
        dot_dash=True,
    )
    if tickers:
        return tickers
    return [
        "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AMD",
        "NFLX", "INTC", "QCOM", "MU", "AVGO", "TXN", "AMAT", "KLAC",
        "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "AXP",
        "JNJ", "PFE", "MRK", "ABBV", "LLY", "BMY", "AMGN", "GILD",
        "XOM", "CVX", "COP", "OXY", "SLB", "EOG", "MPC", "VLO",
        "WMT", "TGT", "COST", "HD", "LOW", "NKE", "SBUX", "MCD",
        "DIS", "CMCSA", "VZ", "T", "TMUS", "CHTR", "PARA", "SNAP",
        "CRM", "ORCL", "NOW", "ADBE", "INTU", "WDAY", "SNOW", "PLTR",
        "PYPL", "V", "MA", "COF", "DFS", "ALLY", "AXP", "SOFI",
        "BA", "LMT", "RTX", "GE", "CAT", "DE", "MMM", "HON",
        "UNH", "CVS", "CI", "HUM", "ELV", "HCA", "THC", "MOH",
    ]


def _fetch_nasdaq100():
    tickers = _scrape_wikipedia_tickers(
        "https://en.wikipedia.org/wiki/Nasdaq-100",
        ["Ticker", "Symbol", "Ticker symbol"],
        dot_dash=True,
    )
    if tickers:
        return tickers
    # Fallback: canonical NASDAQ-100 names as of 2024-2025
    return [
        "AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA", "GOOGL", "GOOG",
        "AVGO", "COST", "NFLX", "AMD", "QCOM", "AMGN", "PEP", "ADBE",
        "CSCO", "INTU", "TXN", "CMCSA", "HON", "AMAT", "SBUX", "ISRG",
        "VRTX", "BKNG", "LRCX", "REGN", "KLAC", "MU", "PANW", "SNPS",
        "CDNS", "MDLZ", "MELI", "INTC", "ORLY", "ASML", "ADP", "FTNT",
        "CTAS", "CSX", "PCAR", "ROP", "MNST", "WDAY", "MRVL", "ROST",
        "ABNB", "CPRT", "AZN", "IDXX", "KDP", "PAYX", "FAST", "DDOG",
        "CRWD", "TEAM", "VRSK", "BIIB", "DLTR", "EXC", "FANG", "GFS",
        "GEHC", "ILMN", "KHC", "LULU", "MAR", "MRNA", "NXPI", "ODFL",
        "ON", "TTWO", "WBD", "XEL", "ZS", "PLTR", "MSTR", "SMCI",
    ]


def _fetch_russell2000():
    # The Russell 2000 Wikipedia article does not list all 2000 components.
    # We try the scrape, but expect it to fail and use the fallback list.
    tickers = _scrape_wikipedia_tickers(
        "https://en.wikipedia.org/wiki/Russell_2000_Index",
        ["Ticker", "Symbol", "Ticker symbol"],
        dot_dash=True,
    )
    if len(tickers) > 20:   # only trust it if we got a real component table
        return tickers
    # Fallback: ~200 liquid US small-cap stocks typically found in the index
    return [
        # Technology
        "ACMR", "AEHR", "ATEN", "BLKB", "CEVA", "CSGS", "FORM", "MGNI",
        "NABL", "PLAB", "PRGS", "POWI", "QTWO", "TTGT", "YEXT", "MTSI",
        "SPSC", "TRMK", "AEIS", "ALRM", "AMKR", "AMBA", "COHU", "DIOD",
        "ICHR", "IPGP", "KRNT", "LSCC", "MXCT", "NTNX", "SEMR", "SMTC",
        # Healthcare / Biotech
        "ACAD", "ADMA", "ARWR", "ENSG", "HCSG", "HIMS", "NVAX", "PRGO",
        "PRKS", "ATRC", "INSP", "OFIX", "ABCL", "RXRX", "CERT", "IBRX",
        "BBIO", "CDMO", "IMVT", "VKTX", "BPMC", "PTGX", "CRSP", "BEAM",
        "EDIT", "NTLA", "SGMO", "TNDM", "SEER", "RCUS", "ARVN", "BDTX",
        # Consumer / Retail
        "BOOT", "BKE", "BROS", "CAKE", "CELH", "DENN", "FIZZ", "PZZA",
        "SHAK", "TWNK", "WINA", "HIBB", "JACK", "NATH", "TLYS", "PLAY",
        "CHUY", "LOCO", "BJRI", "TXRH", "CBRL", "BURL", "FIVE", "OLLI",
        # Financial
        "AFG", "AX", "BANF", "BHLB", "BKU", "CHCO", "ERIE", "FBMS",
        "FFIN", "FNB", "GSHD", "HCI", "KMPR", "LKFN", "MKTX", "PIPR",
        "PLMR", "RLI", "SASR", "SBCF", "SEIC", "SNV", "TRMK", "UCBI",
        "UMBF", "VLY", "WSBC", "WSFS", "EFC", "MFA", "MITT", "ORC",
        # Energy
        "BSM", "CHRD", "CIVI", "GPOR", "NOG", "SWN", "VNOM", "ESTE",
        "MNRL", "FLMN", "CTRE", "GRNT", "REX", "PDCE", "TALO", "VTLE",
        # Industrial / Transportation
        "ARCB", "BLBD", "CMCO", "FCFS", "KTOS", "LSTR", "MRTN", "NSP",
        "WGO", "ODFL", "SAIA", "HTLD", "AVAV", "DAN", "DNOW", "GTES",
        "HLIO", "IIPR", "JBLU", "KBAL", "LAKE", "MGEE", "NVEE", "OTTR",
        # REIT / Real Estate
        "CUBE", "EXR", "NSA", "ROIC", "RHP", "STAG", "IIPR", "BTT",
        # Crypto / Speculative
        "MARA", "RIOT", "CLSK", "HIVE", "BTBT", "CIFR",
        # Misc well-known small caps
        "GME", "AMC", "BCPC", "CSWI", "CENT", "LANC", "SPTN", "CALM",
        "JOUT", "SMSI", "CCUR", "EBIX", "ECPG", "EGHT", "DAN", "DFIN",
    ]


def _normalize_de_tickers(raw_tickers):
    """
    Ensure every ticker ends with the Frankfurt (.DE) exchange suffix.

    Handles three cases the Wikipedia scrape can produce:
      "SAP"    → "SAP.DE"   (no suffix yet)
      "SAP.DE" → "SAP.DE"   (already correct, leave alone)
      "AIR.PA" → "AIR.DE"   (wrong exchange suffix — swap to Frankfurt)
    """
    result = []
    for t in raw_tickers:
        if not t or t in ("—", "-"):
            continue
        if t.endswith(".DE"):
            result.append(t)
        elif "." in t:
            # Replace whatever exchange suffix came back with .DE
            result.append(t.rsplit(".", 1)[0] + ".DE")
        else:
            result.append(t + ".DE")
    return result


def _fetch_dax40():
    # Scrape without adding a suffix — Wikipedia may already include ".DE"
    raw = _scrape_wikipedia_tickers(
        "https://en.wikipedia.org/wiki/DAX",
        ["Ticker symbol", "Symbol", "Ticker"],
        suffix="",        # normaliser below handles the .DE suffix
        dot_dash=False,
    )
    tickers = _normalize_de_tickers(raw)
    tickers = [t for t in tickers if len(t) > 3]
    if tickers:
        return tickers
    # Fallback: DAX 40 components as of 2024-2025
    return [
        "ADS.DE", "AIR.DE", "ALV.DE", "BAS.DE", "BAYN.DE", "BEI.DE",
        "BMW.DE", "BNR.DE", "CON.DE", "1COV.DE", "DB1.DE", "DBK.DE",
        "DHL.DE", "DHER.DE", "DTG.DE", "DTE.DE", "EOAN.DE", "ENR.DE",
        "FRE.DE", "FME.DE", "HNR1.DE", "HEN3.DE", "IFX.DE", "MBG.DE",
        "MRK.DE", "MTX.DE", "MUV2.DE", "P911.DE", "PAH3.DE", "QIA.DE",
        "RHM.DE", "RWE.DE", "SAP.DE", "SHL.DE", "SIE.DE", "SRT3.DE",
        "SY1.DE", "VOW3.DE", "VNA.DE", "ZAL.DE",
    ]


def _fetch_mdax():
    raw = _scrape_wikipedia_tickers(
        "https://en.wikipedia.org/wiki/MDAX",
        ["Ticker symbol", "Symbol", "Ticker"],
        suffix="",
        dot_dash=False,
    )
    tickers = _normalize_de_tickers(raw)
    tickers = [t for t in tickers if len(t) > 3]
    if tickers:
        return tickers
    # Fallback: representative MDAX mid-cap German stocks
    return [
        "AFX.DE", "AIXA.DE", "BC8.DE", "BOSS.DE", "DWS.DE", "EVK.DE",
        "GXI.DE", "HOT.DE", "KGX.DE", "LEG.DE", "LXS.DE", "MDG1.DE",
        "PSM.DE", "RAA.DE", "SDF.DE", "SMHN.DE", "SOW.DE", "SRT.DE",
        "TAG.DE", "TKA.DE", "UTDI.DE", "VBK.DE", "WAF.DE", "ZAL.DE",
    ]


# ── Main universe builder ──────────────────────────────────────────────────────

def get_tickers():
    """
    Build a deduplicated ticker universe from five indices:
      S&P 500  |  NASDAQ 100  |  Russell 2000 (small-cap)  |  DAX 40  |  MDAX

    Each index is scraped from Wikipedia (pd.read_html first, then
    BeautifulSoup as lxml-free fallback). If both scrapes fail, a hardcoded
    fallback list is used so the scanner can always run.

    Prints a per-index breakdown and the total unique count.
    """
    sources = [
        ("S&P 500",     _fetch_sp500),
        ("NASDAQ 100",  _fetch_nasdaq100),
        ("Russell 2000", _fetch_russell2000),
        ("DAX 40",      _fetch_dax40),
        ("MDAX",        _fetch_mdax),
    ]

    all_tickers = []
    parts = []

    for label, fetcher in sources:
        batch = fetcher()
        # Remove obvious duplicates within each batch and empty strings
        batch = list(dict.fromkeys(t for t in batch if t))
        all_tickers.extend(batch)
        parts.append(f"{label}: {len(batch)}")

    # Deduplicate across all indices while preserving insertion order
    combined = list(dict.fromkeys(all_tickers))

    print(f"Ticker universe — {', '.join(parts)}")
    print(f"Total unique tickers to scan: {len(combined)}")
    return combined


# ─────────────────────────────────────────────
# STEP 2: Identify tickers with earnings in the next 7 days
# ─────────────────────────────────────────────

def _parse_earnings_date_from_calendar(cal):
    """
    Extract the nearest upcoming earnings date from a yfinance calendar object.
    Handles both dict and DataFrame formats (varies by yfinance version).
    Returns a date object or None.
    """
    try:
        if isinstance(cal, dict):
            raw = cal.get("Earnings Date")
            if raw is None:
                return None
            # Newer yfinance wraps the value in a list
            if isinstance(raw, list):
                raw = raw[0] if raw else None
            if raw is None:
                return None

        elif isinstance(cal, pd.DataFrame):
            if "Earnings Date" in cal.columns:
                vals = cal["Earnings Date"].dropna()
                raw = vals.iloc[0] if len(vals) > 0 else None
            elif "Earnings Date" in cal.index:
                raw = cal.loc["Earnings Date"]
            else:
                return None
        else:
            return None

        # Normalise to a plain date.
        # pd.Timestamp() accepts datetime.date, datetime.datetime,
        # pd.Timestamp, and strings — covers all yfinance return types.
        try:
            return pd.Timestamp(raw).date()
        except Exception:
            return None
    except Exception:
        return None


# ── Cache ─────────────────────────────────────────────────────────────────────

_CACHE_FILE     = "cache.json"
_CACHE_TTL_DAYS = 7          # re-use a ticker's historical data for 7 days
_WORKERS        = 10         # parallel threads for both calendar scan and analysis


def _load_cache():
    """Load the on-disk reaction cache, returning {} if missing or corrupt."""
    try:
        with open(_CACHE_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {}


def _save_cache(cache):
    """Persist the in-memory cache dict to disk."""
    try:
        with open(_CACHE_FILE, "w") as fh:
            json.dump(cache, fh, indent=2)
    except Exception as e:
        print(f"Warning: could not save cache — {e}")


def _cache_get(cache, ticker):
    """
    Return cached reaction data for `ticker` if it is younger than
    _CACHE_TTL_DAYS, otherwise return None (cache miss).
    """
    entry = cache.get(ticker)
    if not entry:
        return None
    try:
        age = (datetime.now() - datetime.fromisoformat(entry["timestamp"])).days
        if age < _CACHE_TTL_DAYS:
            return entry["data"]
    except Exception:
        pass
    return None


# ── Bulk earnings calendar ─────────────────────────────────────────────────────

def _fetch_yf_earnings_calendar(days_ahead=7):
    """
    Fetch every company with confirmed earnings in the next `days_ahead` days
    using Yahoo Finance's market-wide earnings calendar — a single paginated
    HTTP call that replaces scanning every ticker individually.

    Uses yfinance's internal session (handles cookies / anti-bot headers).
    Falls back to [] so the caller can switch to the per-ticker scan.
    """
    from bs4 import BeautifulSoup

    today  = datetime.now().date()
    cutoff = today + timedelta(days=days_ahead)
    # Reuse yfinance's authenticated session via a throwaway Ticker object
    _sess  = yf.Ticker("SPY")
    found  = []

    for offset in range(0, 2000, 100):      # paginate; stop when a page is empty
        url = (
            f"https://finance.yahoo.com/calendar/earnings"
            f"?from={today}&to={cutoff}&offset={offset}&size=100"
        )
        try:
            resp = _sess._data.cache_get(url)
            if resp is None or resp.status_code != 200:
                break

            soup  = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table")
            if table is None:
                break

            # Map header labels → column index
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            sym_col  = next((i for i, h in enumerate(headers) if "symbol"        in h), None)
            date_col = next((i for i, h in enumerate(headers) if "earnings date" in h), None)

            if sym_col is None:
                break

            rows = table.find_all("tr")[1:]
            if not rows:
                break

            page_count = 0
            for row in rows:
                cells = row.find_all("td")
                if not cells:
                    continue

                sym = cells[sym_col].get_text(strip=True).split("[")[0].strip()
                if not sym:
                    continue

                # Parse the earnings date; fall back to today if unparseable
                earn_date = str(today)
                if date_col is not None and len(cells) > date_col:
                    raw = cells[date_col].get_text(strip=True).split(" at ")[0].strip()
                    try:
                        earn_date = str(datetime.strptime(raw, "%B %d, %Y").date())
                    except ValueError:
                        pass

                found.append({"ticker": sym, "earnings_date": earn_date})
                page_count += 1

            if page_count < 100:
                break          # last page — no need to request another

        except Exception:
            break

    return found


def get_upcoming_earnings(tickers, days_ahead=7):
    """
    Return tickers with confirmed earnings in the next `days_ahead` days.

    Fast path  — Yahoo Finance bulk calendar (1 paginated request).
    Slow path  — parallel per-ticker calendar scan with _WORKERS threads
                 (used only if the bulk calendar returns nothing).
    """
    today  = datetime.now().date()
    cutoff = today + timedelta(days=days_ahead)

    print(f"\nFetching earnings calendar for {today} → {cutoff}…")

    # ── Fast path ──────────────────────────────────────────────────────────────
    bulk = _fetch_yf_earnings_calendar(days_ahead)
    if bulk:
        print(f"  Bulk calendar: {len(bulk)} ticker(s) found — universe scan skipped.")
        return bulk

    # ── Slow path: parallel per-ticker scan ────────────────────────────────────
    print(f"  Bulk calendar unavailable. Scanning {len(tickers)} tickers "
          f"({_WORKERS} workers)…")

    upcoming = []
    scanned  = 0
    _lock    = threading.Lock()

    def _check(ticker):
        try:
            cal = yf.Ticker(ticker).calendar
            if cal is None:
                return None
            d = _parse_earnings_date_from_calendar(cal)
            if d and today <= d <= cutoff:
                return {"ticker": ticker, "earnings_date": str(d)}
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=_WORKERS) as exe:
        futures = {exe.submit(_check, t): t for t in tickers}
        for fut in as_completed(futures):
            res = fut.result()
            with _lock:
                scanned += 1
                if res:
                    upcoming.append(res)
                if scanned % 100 == 0:
                    print(f"  Scanned {scanned}/{len(tickers)}, "
                          f"found {len(upcoming)} so far…")

    print(f"Found {len(upcoming)} ticker(s) with earnings in the next {days_ahead} days.\n")
    return upcoming


# ─────────────────────────────────────────────
# STEP 3: Fetch historical earnings dates
# ─────────────────────────────────────────────

def _fetch_earnings_dates_with_bs4(ticker_obj, limit=24):
    """
    Parse historical earnings dates directly from Yahoo Finance's earnings
    calendar page using BeautifulSoup with Python's built-in html.parser.

    This avoids the lxml / html5lib dependency that pd.read_html requires.
    BeautifulSoup (bs4) is already installed as a transitive dependency of
    yfinance — no extra package install needed.

    Returns a pandas DatetimeIndex of past earnings announcement dates,
    or an empty list on failure.
    """
    try:
        from bs4 import BeautifulSoup  # already a yfinance dependency

        url = (
            f"https://finance.yahoo.com/calendar/earnings"
            f"?symbol={ticker_obj.ticker}&offset=0&size={min(limit, 100)}"
        )
        # Re-use yfinance's internal session / cache so we don't open a
        # raw requests.Session ourselves
        response = ticker_obj._data.cache_get(url)
        if response is None or response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")
        if table is None:
            return []

        rows = table.find_all("tr")
        dates = []
        for row in rows[1:]:   # skip header row
            cells = row.find_all("td")
            if not cells:
                continue
            raw_date = cells[0].get_text(strip=True)
            if not raw_date:
                continue
            try:
                # Format: "May 28, 2025 at 6 AM EDT"  or  "May 28, 2025"
                clean = raw_date.split(" at ")[0].strip()
                dt = datetime.strptime(clean, "%B %d, %Y")
                dates.append(pd.Timestamp(dt))
            except ValueError:
                continue

        return dates
    except Exception:
        return []


def _earnings_dates_from_quarterly_stmts(ticker_obj):
    """
    Fallback: use quarterly income-statement period-end dates as a proxy
    for earnings announcement dates.

    The actual announcement is typically 2-5 weeks after period end.
    We add 21 days (3 weeks) as a rough correction so that price-reaction
    windows centre on approximately the right date.
    """
    try:
        df = ticker_obj.quarterly_income_stmt
        if df is None or df.empty:
            return []
        dates = []
        for col in df.columns:
            ts = pd.Timestamp(col)
            # Shift forward ~3 weeks to approximate the announcement date
            approx = ts + pd.Timedelta(days=21)
            if approx < pd.Timestamp.now():
                dates.append(approx)
        # Most-recent first
        dates.sort(reverse=True)
        return dates
    except Exception:
        return []


def get_historical_earnings_dates(ticker_obj, num_quarters=4):
    """
    Return a list of the last `num_quarters` historical earnings dates
    for the given ticker, most recent first.

    Tries three paths in order:
      1. yfinance's get_earnings_dates()  (needs lxml or html5lib)
      2. Direct BeautifulSoup parse       (needs only bs4, already present)
      3. quarterly_income_stmt proxy      (always available, less precise)
    """
    today = pd.Timestamp.now().normalize()

    # Path 1: native yfinance method
    try:
        df = ticker_obj.get_earnings_dates(limit=24)
        if df is not None and not df.empty:
            idx = df.index
            if idx.tz is not None:
                idx = idx.tz_localize(None)
            past = sorted([d for d in idx if d < today], reverse=True)
            if len(past) >= 2:
                return past[:num_quarters]
    except Exception:
        pass

    # Path 2: BeautifulSoup parse (avoids lxml)
    try:
        dates = _fetch_earnings_dates_with_bs4(ticker_obj, limit=24)
        past = sorted([d for d in dates if d < today], reverse=True)
        if len(past) >= 2:
            return past[:num_quarters]
    except Exception:
        pass

    # Path 3: quarterly income-statement proxy
    dates = _earnings_dates_from_quarterly_stmts(ticker_obj)
    past = [d for d in dates if d < today]
    if len(past) >= 2:
        return past[:num_quarters]

    return []


# ─────────────────────────────────────────────
# STEP 4: Calculate per-earnings reaction and drift
# ─────────────────────────────────────────────

def _nth_trading_day_before(hist, anchor, n):
    """Return the Close price n trading sessions before `anchor` (a Timestamp)."""
    prior = hist.index[hist.index < anchor]
    if len(prior) < n:
        return None
    return float(hist.loc[prior[-n], "Close"])


def _nth_trading_day_after(hist, anchor, n):
    """Return the Close price n trading sessions after `anchor` (a Timestamp)."""
    after = hist.index[hist.index > anchor]
    if len(after) < n:
        return None
    return float(hist.loc[after[n - 1], "Close"])


def analyze_historical_earnings(ticker, num_quarters=4):
    """
    For a given ticker:
      1. Pull the last `num_quarters` historical earnings announcement dates.
      2. For each date compute:
           post-earnings reaction: (close t+1 − close t−1) / close t−1 × 100
           pre-earnings drift:     (close t−1 − close t−5) / close t−5 × 100
    Returns a list of result dicts, or None if data is insufficient.
    """
    try:
        stock = yf.Ticker(ticker)

        earn_dates = get_historical_earnings_dates(stock, num_quarters)
        if len(earn_dates) < 2:
            return None

        # Fetch 2 years of price history in a single call to cover all dates
        hist = stock.history(period="2y", auto_adjust=True)
        if hist.empty or len(hist) < 20:
            return None

        # Strip timezone from index so comparisons work uniformly
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)

        results = []
        for earn_ts in earn_dates:
            try:
                close_before = _nth_trading_day_before(hist, earn_ts, 1)   # t−1
                close_after  = _nth_trading_day_after(hist, earn_ts, 1)    # t+1
                close_5_ago  = _nth_trading_day_before(hist, earn_ts, 5)   # t−5

                if None in (close_before, close_after, close_5_ago):
                    continue
                if close_before == 0 or close_5_ago == 0:
                    continue

                reaction_pct = (close_after  - close_before) / close_before * 100
                drift_pct    = (close_before - close_5_ago)  / close_5_ago  * 100

                results.append({
                    "date":         earn_ts.strftime("%Y-%m-%d"),
                    "reaction_pct": round(reaction_pct, 2),
                    "drift_pct":    round(drift_pct, 2),
                })
            except Exception:
                continue

        # Need at least 2 usable data points to be meaningful
        return results if len(results) >= 2 else None

    except Exception:
        return None


# ─────────────────────────────────────────────
# STEP 5: Score and signal generation
# ─────────────────────────────────────────────

def _compute_score(reactions):
    """
    Four-component scoring system, each component worth 25 points (max 100).

    C1 — Reaction consistency (25 pts)
         4/4 same direction = 25  |  3/4 = 15  |  2/4 or less = 0

    C2 — Reaction magnitude — logarithmic scale (25 pts)
         avg |move| ≥ 10 % = 25  |  5 % ≈ 18  |  2 % ≈ 8  |  < 1 % = 0
         Formula: 25 × log₁₀(avg_abs),  clamped to [0, 25]

    C3 — Drift alignment — weighted by drift magnitude (25 pts)
         For each report, a drift 'aligns' when it and the reaction share a sign.
         The contribution is weighted by |drift|, so a large aligned drift
         counts more than a tiny one.
         Weighted ratio ≥ 0.90 = 25  |  ≥ 0.70 = 15  |  ≥ 0.40 = 5  |  else 0
         (With equal-weight drifts this maps exactly to 4/4 = 25, 3/4 = 15,
          2/4 = 5, 1/4 or 0/4 = 0.)

    C4 — Trend momentum — recency-weighted, variance-penalised (25 pts)
         Starts at 25.  Two penalties are subtracted:
           • Direction penalty: proportional to the recency-weighted fraction
             of reactions that went against the majority direction.
             (Weights are linear: Q1 most-recent = 2×, Q4 oldest = 1×.)
           • Variance penalty: 12.5 × CV, capped at 25, where CV = σ / μ of
             absolute reactions.  High variance (unpredictable size) reduces
             this component even when direction is consistent.

    Returns (total_score: int, breakdown: dict)
    """
    if not reactions:
        zero = {"consistency": 0, "magnitude": 0, "drift_alignment": 0, "momentum": 0}
        return 0, zero

    n     = len(reactions)
    pcts  = [r["reaction_pct"] for r in reactions]
    drifts = [r["drift_pct"]   for r in reactions]

    # ── C1: Reaction consistency ───────────────────────────────────────────
    positives      = sum(1 for p in pcts if p > 0)
    majority_count = max(positives, n - positives)
    majority_ratio = majority_count / n

    if majority_ratio == 1.0:     c1 = 25
    elif majority_ratio >= 0.75:  c1 = 15
    else:                         c1 = 0

    # ── C2: Reaction magnitude (log scale) ────────────────────────────────
    avg_abs = sum(abs(p) for p in pcts) / n
    if avg_abs < 1.0:
        c2 = 0
    elif avg_abs >= 10.0:
        c2 = 25
    else:
        c2 = round(25.0 * math.log10(avg_abs))   # log10(1)=0, log10(10)=25

    # ── C3: Drift alignment weighted by drift magnitude ───────────────────
    total_dw   = sum(abs(d) for d in drifts)
    aligned_dw = sum(
        abs(d) for p, d in zip(pcts, drifts)
        if (p > 0 and d > 0) or (p < 0 and d < 0)
    )
    w_ratio = (aligned_dw / total_dw) if total_dw > 0 else 0.0

    if w_ratio >= 0.90:   c3 = 25
    elif w_ratio >= 0.70: c3 = 15
    elif w_ratio >= 0.40: c3 = 5
    else:                 c3 = 0

    # ── C4: Trend momentum ────────────────────────────────────────────────
    # Recency weights: Q1 (index 0, most recent) = 2.0 … Q4 (oldest) = 1.0
    if n == 1:
        weights = [2.0]
    else:
        weights = [2.0 - (i / (n - 1)) for i in range(n)]   # linear 2 → 1
    w_total = sum(weights)

    # Direction penalty: weighted fraction going against majority
    w_positive = sum(w for w, p in zip(weights, pcts) if p > 0)
    w_minority  = w_total - max(w_positive, w_total - w_positive)
    dir_penalty = (w_minority / w_total) * 25.0

    # Variance penalty: CV of absolute reactions, scaled so CV=2 → full 25-pt penalty
    abs_pcts = [abs(p) for p in pcts]
    mean_abs = sum(abs_pcts) / n
    if n > 1 and mean_abs > 0.1:
        std_abs = math.sqrt(sum((a - mean_abs) ** 2 for a in abs_pcts) / (n - 1))
        cv = std_abs / mean_abs
        var_penalty = min(25.0, cv * 12.5)
    else:
        var_penalty = 0.0

    c4 = round(max(0.0, 25.0 - dir_penalty - var_penalty))

    total = c1 + c2 + c3 + c4
    breakdown = {
        "consistency":    c1,
        "magnitude":      c2,
        "drift_alignment": c3,
        "momentum":       c4,
    }
    return total, breakdown


def score_stock(reactions):
    """Return the 0–100 composite score (delegates to _compute_score)."""
    score, _ = _compute_score(reactions)
    return score


def generate_signal(reactions):
    """
    LONG     → majority of past reactions positive  AND majority of pre-drifts positive
    SHORT    → majority of past reactions negative  AND majority of pre-drifts negative
    NO TRADE → mixed or ambiguous evidence
    """
    if not reactions:
        return "NO TRADE"

    n             = len(reactions)
    pos_reactions = sum(1 for r in reactions if r["reaction_pct"] > 0)
    neg_reactions = n - pos_reactions
    pos_drifts    = sum(1 for r in reactions if r["drift_pct"]    > 0)
    neg_drifts    = n - pos_drifts

    # Require a strict majority in BOTH dimensions — ties count as NO TRADE
    if pos_reactions > neg_reactions and pos_drifts > neg_drifts:
        return "LONG"
    if neg_reactions > pos_reactions and neg_drifts > pos_drifts:
        return "SHORT"
    return "NO TRADE"


# ─────────────────────────────────────────────
# STEP 6: Output helpers
# ─────────────────────────────────────────────

def _score_breakdown(reactions):
    """Return the four score components as a dict (delegates to _compute_score)."""
    _, breakdown = _compute_score(reactions)
    return breakdown


def _build_chart_data(ticker_obj):
    """
    Fetch up to 1 year of daily close prices and return a compact JSON string
    ready to embed in a data-chart HTML attribute.

    Format: {"d": ["YYYY-MM-DD", ...], "p": [float, ...]}
    JS slices this single array for each time-horizon button (1W/1M/3M/6M/1Y).
    Returns "" on failure so the canvas is simply left empty.
    """
    try:
        hist = ticker_obj.history(period="1y", auto_adjust=True)
        if hist is None or hist.empty:
            return ""
        closes = hist["Close"].dropna()
        if len(closes) < 3:
            return ""
        return json.dumps({
            "d": [d.strftime("%Y-%m-%d") for d in closes.index],
            "p": [round(float(v), 2) for v in closes.values],
        }, separators=(",", ":"))   # compact — no spaces
    except Exception:
        return ""


def _fetch_ticker_meta(ticker):
    """
    Single ticker.info call that returns company name, a one-sentence business
    description, and all fundamental metrics needed for the HTML report.
    Every field falls back gracefully so callers never see an exception.
    """
    _NA = "N/A"
    _empty_funds = {
        "pe_trailing": _NA, "pe_forward": _NA, "eps_ttm": _NA,
        "revenue_growth": _NA, "earnings_growth": _NA,
        "market_cap": _NA, "week52_high": _NA, "week52_low": _NA,
    }

    try:
        stock = yf.Ticker(ticker)    # one object; reused for both info and history
        info  = stock.info

        # ── Company name ──────────────────────────────────────────────────
        name = info.get("longName") or info.get("shortName") or ticker

        # ── One-sentence business description ─────────────────────────────
        summary = info.get("longBusinessSummary", "") or ""
        if summary:
            dot = summary.find(". ")
            description = summary[:dot + 1] if dot >= 0 else summary[:300]
        else:
            description = ""

        # ── 1-year price history for the interactive canvas chart ────────
        chart_data = _build_chart_data(stock)

        # ── Metric formatters ─────────────────────────────────────────────
        def _pe(v):
            return f"{v:.1f}×"          if v is not None else _NA

        def _eps(v):
            return f"${v:.2f}"          if v is not None else _NA

        def _pct(v):
            return f"{v * 100:+.1f}%"   if v is not None else _NA

        def _mcap(v):
            if v is None:    return _NA
            if v >= 1e12:    return f"${v / 1e12:.2f}T"
            if v >= 1e9:     return f"${v / 1e9:.1f}B"
            if v >= 1e6:     return f"${v / 1e6:.0f}M"
            return f"${v:,.0f}"

        def _price(v):
            return f"${v:,.2f}"         if v is not None else _NA

        mcap_raw = info.get("marketCap", 0) or 0
        return {
            "company_name": name,
            "description":  description,
            "chart_data":   chart_data,
            "fundamentals": {
                "pe_trailing":      _pe   (info.get("trailingPE")),
                "pe_forward":       _pe   (info.get("forwardPE")),
                "eps_ttm":          _eps  (info.get("trailingEps")),
                "revenue_growth":   _pct  (info.get("revenueGrowth")),
                "earnings_growth":  _pct  (info.get("earningsGrowth")),
                "market_cap":       _mcap (info.get("marketCap")),
                "week52_high":      _price(info.get("fiftyTwoWeekHigh")),
                "week52_low":       _price(info.get("fiftyTwoWeekLow")),
                "_market_cap_raw":  mcap_raw,
            },
        }
    except Exception:
        return {"company_name": ticker, "description": "", "chart_data": "",
                "fundamentals": _empty_funds}

_TR_STOCK_MCAP = 1_000_000_000    # $1B — Trade Republic stock coverage
_TR_DERIV_MCAP = 3_000_000_000    # $3B — Trade Republic derivatives
_SC_STOCK_MCAP =   500_000_000    # $500M — Scalable Capital stock
_SC_DERIV_MCAP = 1_500_000_000    # $1.5B — Scalable Capital derivatives


def _broker_availability(ticker, market_cap_raw):
    """
    Estimate whether the stock and exchange-traded derivatives (knockouts /
    warrants) are available on Trade Republic (TR) and Scalable Capital (SC).

    Both platforms do not publish a complete public instrument universe, so
    this uses market-cap thresholds and exchange-suffix heuristics:
      • German-listed stocks (.DE) are supported by both brokers down to smaller
        caps because they trade on Xetra / Gettex which both platforms connect to.
      • US-listed stocks need a larger market cap before TR / SC list them and
        before structured-product issuers offer derivatives on them.

    Returns a dict with four booleans: tr_stock, tr_deriv, sc_stock, sc_deriv.
    """
    mcap  = market_cap_raw or 0
    is_de = ticker.endswith(".DE")

    if is_de:
        tr_stock = mcap >= 100_000_000
        tr_deriv = mcap >= 500_000_000
        sc_stock = mcap >=  50_000_000
        sc_deriv = mcap >= 200_000_000
    else:
        tr_stock = mcap >= _TR_STOCK_MCAP
        tr_deriv = mcap >= _TR_DERIV_MCAP
        sc_stock = mcap >= _SC_STOCK_MCAP
        sc_deriv = mcap >= _SC_DERIV_MCAP

    return {
        "tr_stock": tr_stock,
        "tr_deriv": tr_deriv,
        "sc_stock": sc_stock,
        "sc_deriv": sc_deriv,
    }


def _fmt(val):
    """Format a float as a signed percentage string, e.g. +3.45% or -1.20%."""
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}%"


def print_results_table(results):
    """
    Print a wide summary table sorted by score descending.

    Columns: Ticker | Signal | Q1 … Q4 (individual reactions, newest first)
             | Avg Rxn | Drift | Score | Earnings Date

    Q-labels refer to the 4 most recent historical reports, not fiscal quarters:
    Q1 = most recent report, Q4 = oldest of the four.
    Missing data (fewer than 4 valid reports) shows as N/A.
    """
    if not results:
        print("\nNo stocks passed the filter (score >= 60 with a clear LONG/SHORT signal).")
        return

    results.sort(key=lambda x: x["score"], reverse=True)

    # Fixed column widths (chars)
    TK, SG, QW, AV, DR, SC, DT = 8, 7, 11, 11, 10, 5, 12

    def col(label, w, align="right"):
        return f"{label:>{w}}" if align == "right" else f"{label:<{w}}"

    header = (
        f"{col('Ticker', TK, 'left')} "
        f"{col('Signal', SG, 'left')} "
        f"{col('Q1(Latest)', QW)} "
        f"{col('Q2', QW)} "
        f"{col('Q3', QW)} "
        f"{col('Q4(Oldest)', QW)} "
        f"{col('Avg Rxn', AV)} "
        f"{col('Drift', DR)} "
        f"{col('Score', SC)} "
        f"{col('Earns Date', DT)}"
    )
    bar = "─" * len(header)

    print(f"\n{'═' * len(header)}")
    print("  EARNINGS STRATEGY SCANNER — SIGNALS  (Q1 = most recent past report)")
    print(f"{'═' * len(header)}")
    print(header)
    print(bar)

    for r in results:
        qtrs = r.get("individual_reactions", [])
        bd   = r.get("score_breakdown", {})

        def qval(n):
            if n <= len(qtrs):
                return _fmt(qtrs[n - 1]["reaction_pct"])
            return "N/A"

        # Main data row
        print(
            f"{col(r['ticker'], TK, 'left')} "
            f"{col(r['signal'], SG, 'left')} "
            f"{col(qval(1), QW)} "
            f"{col(qval(2), QW)} "
            f"{col(qval(3), QW)} "
            f"{col(qval(4), QW)} "
            f"{col(_fmt(r['avg_reaction_pct']), AV)} "
            f"{col(_fmt(r['avg_drift_pct']), DR)} "
            f"{col(str(r['score']), SC)} "
            f"{col(r['earnings_date'], DT)}"
        )

        # Score breakdown sub-line
        c  = bd.get("consistency",    0)
        m  = bd.get("magnitude",      0)
        d  = bd.get("drift_alignment", 0)
        t  = bd.get("momentum",       0)
        print(
            f"{'':>{TK + SG + 2}}"          # indent to align under data columns
            f"  Consistency:{c:>3}/25"
            f"  Magnitude:{m:>3}/25"
            f"  Drift:{d:>3}/25"
            f"  Momentum:{t:>3}/25"
            f"  Total:{r['score']:>3}/100"
        )

    print(bar)
    print(
        f"  {len(results)} result(s)  |  Q1=most recent  |"
        f"  score >= 60  |  NO TRADE excluded\n"
    )


# ─────────────────────────────────────────────
# STEP 7: HTML report
# ─────────────────────────────────────────────

# CSS lives as a plain string so its { } don't need escaping inside f-strings.
_REPORT_CSS = """
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:#08090f;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;min-height:100vh}
.page{max-width:1440px;margin:0 auto;padding:0 0 56px}

/* ── Hero ── */
.hero{background:linear-gradient(160deg,#0d1525 0%,#111827 55%,#0d1525 100%);
  border-bottom:1px solid #1e2740;padding:22px 32px 18px}
.hero-top{margin-bottom:14px}
.hero-title{font-size:1.45rem;font-weight:800;letter-spacing:-.025em;margin-bottom:5px;
  background:linear-gradient(90deg,#60a5fa,#a78bfa);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}
.hero-sub{display:flex;align-items:center;gap:10px;font-size:.78rem;color:#64748b;flex-wrap:wrap}
.hero-sep{color:#1e2740}
.hero-lc{color:#10b981;font-weight:700}.hero-sc{color:#ef4444;font-weight:700}
.mc-row{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
@media(max-width:640px){.mc-row{grid-template-columns:1fr 1fr}}
.mc{background:rgba(255,255,255,.03);border:1px solid #1e2740;border-radius:10px;padding:14px 18px}
.mc-val{font-size:1.5rem;font-weight:800;line-height:1;margin-bottom:4px;color:#f1f5f9;font-variant-numeric:tabular-nums}
.mc-val.is-ticker{font-size:1.1rem;color:#60a5fa}
.mc-lbl{font-size:.65rem;text-transform:uppercase;letter-spacing:.1em;color:#475569;font-weight:600}

/* ── Filter bar ── */
.filter-bar{position:sticky;top:0;z-index:200;
  background:rgba(8,9,15,.95);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);
  border-bottom:1px solid #1e2740;padding:9px 32px}
.filter-inner{display:flex;align-items:center;gap:16px;flex-wrap:wrap;max-width:1440px;margin:0 auto}
.fg{display:flex;align-items:center;gap:6px}
.fl{font-size:.65rem;text-transform:uppercase;letter-spacing:.07em;color:#475569;font-weight:600;white-space:nowrap}
.btn-grp{display:flex;gap:3px}
.fb{padding:4px 10px;border-radius:6px;font-size:.7rem;font-weight:600;cursor:pointer;
  border:1px solid #1e2740;background:transparent;color:#64748b;transition:all .12s;line-height:1.6}
.fb:hover{border-color:#334155;color:#94a3b8}
.fb.active{background:#1e3a5f;border-color:#3b82f6;color:#60a5fa}
.fb.long-on{background:rgba(16,185,129,.12);border-color:#10b981;color:#10b981}
.fb.short-on{background:rgba(239,68,68,.12);border-color:#ef4444;color:#ef4444}
input[type=range]{-webkit-appearance:none;appearance:none;height:4px;
  background:#1e2740;border-radius:2px;outline:none;width:100px;cursor:pointer}
input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;width:14px;height:14px;
  background:#3b82f6;border-radius:50%;cursor:pointer}
input[type=range]::-moz-range-thumb{width:14px;height:14px;background:#3b82f6;
  border-radius:50%;border:none;cursor:pointer}
#scoreDisplay{color:#60a5fa;font-weight:700}
.sort-sel{-webkit-appearance:none;appearance:none;
  background:#0a0e1a url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%2364748b'/%3E%3C/svg%3E") no-repeat right 9px center;
  border:1px solid #1e2740;border-radius:8px;padding:5px 26px 5px 10px;
  color:#94a3b8;font-size:.73rem;outline:none;cursor:pointer}
.sort-sel:focus{border-color:#3b82f6}
.search-box{background:#0a0e1a;border:1px solid #1e2740;border-radius:8px;
  padding:5px 10px;color:#e2e8f0;font-size:.73rem;outline:none;width:140px}
.search-box:focus{border-color:#3b82f6}
.search-box::placeholder{color:#334155}
.res-count{margin-left:auto;font-size:.7rem;color:#475569;
  background:#0f1629;border:1px solid #1e2740;border-radius:20px;padding:3px 12px;white-space:nowrap}

/* ── Grid ── */
main{padding:16px 32px 0}
.grid{display:grid;grid-template-columns:1fr;gap:10px}
.empty-state{text-align:center;padding:72px 0;color:#334155;font-size:.9rem}

/* ── Card shell ── */
.card{
  background:#0f1629;border:1px solid #1e2740;border-left:4px solid #1e2740;
  border-radius:10px;overflow:hidden;transition:border-color .18s,box-shadow .18s
}
.card[data-signal="LONG"]  {border-left-color:#10b981}
.card[data-signal="SHORT"] {border-left-color:#ef4444}
.card:hover{border-color:#334155;box-shadow:0 4px 28px rgba(0,0,0,.45)}

/* ── Card header: identity + score + date ── */
.card-header{
  padding:12px 20px 10px;
  display:flex;align-items:center;gap:14px;flex-wrap:wrap;
  border-bottom:1px solid #1e2740
}
.ch-id{display:flex;align-items:center;gap:8px;flex-shrink:0}
.ticker-sym{font-size:1.3rem;font-weight:800;letter-spacing:-.01em;color:#f1f5f9}
.badge{padding:2px 8px;border-radius:5px;font-size:.65rem;font-weight:700;
  letter-spacing:.06em;text-transform:uppercase}
.badge-long{background:rgba(16,185,129,.12);color:#10b981;border:1px solid rgba(16,185,129,.25)}
.badge-short{background:rgba(239,68,68,.12);color:#ef4444;border:1px solid rgba(239,68,68,.25)}
.ch-name{flex:1;min-width:140px;overflow:hidden}
.company-nm{font-size:.84rem;color:#94a3b8;font-weight:500;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.description{font-size:.67rem;color:#64748b;line-height:1.3;margin-top:2px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ch-score{flex-shrink:0;display:flex;flex-direction:column;align-items:flex-end;gap:5px;
  padding-left:16px;border-left:1px solid #1e2740}
.chs-lbl{font-size:.55rem;text-transform:uppercase;letter-spacing:.09em;color:#475569;font-weight:600}
.chs-total{font-size:1.85rem;font-weight:800;font-variant-numeric:tabular-nums;line-height:1}
.chs-max{font-size:.7rem;color:#475569;font-weight:400;margin-left:2px}
.chs-dims{display:flex;gap:10px}
.chs-dim{font-size:.62rem;color:#64748b;white-space:nowrap}
.chs-dim strong{font-weight:700;margin-left:2px}
.ch-date{flex-shrink:0;display:flex;flex-direction:column;align-items:flex-end;gap:2px;
  padding-left:16px;border-left:1px solid #1e2740}
.rxn-sep{border-top:1px solid #1e2740;margin:2px 0}
.earns-lbl{font-size:.56rem;text-transform:uppercase;letter-spacing:.09em;color:#475569}
.earns-val{font-size:.82rem;font-weight:700;color:#e2e8f0;font-family:ui-monospace,monospace}

/* ── Card meta: fundamentals pills + broker badges ── */
.card-meta{
  padding:6px 20px;display:flex;align-items:center;gap:5px;flex-wrap:wrap;
  border-bottom:1px solid #1e2740;background:rgba(0,0,0,.18)
}
.meta-pill{display:flex;align-items:center;gap:4px;padding:3px 8px;border-radius:5px;
  background:rgba(30,39,64,.4);border:1px solid #1e2740}
.mp-key{font-size:.55rem;text-transform:uppercase;letter-spacing:.06em;color:#475569;white-space:nowrap}
.mp-val{font-size:.7rem;font-weight:600;color:#cbd5e1;font-family:ui-monospace,monospace}
.mp-na{color:#334155!important;font-style:italic;font-weight:400!important}
.meta-sep{width:1px;height:16px;background:#1e2740;flex-shrink:0;margin:0 3px}
.broker-pill{display:flex;align-items:center;gap:3px;padding:3px 8px;
  border-radius:5px;border:1px solid #1e2740;background:rgba(30,39,64,.3)}
.bp-name{font-size:.55rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;
  color:#64748b;margin-right:3px;white-space:nowrap}
.av{padding:1px 5px;border-radius:3px;font-size:.57rem;font-weight:700;white-space:nowrap}
.av-y{background:rgba(16,185,129,.12);color:#10b981;border:1px solid rgba(16,185,129,.3)}
.av-n{background:rgba(30,39,64,.4);color:#475569;border:1px solid #1e2740}

/* ── Card body: 3-column grid ── */
.card-body{display:grid;grid-template-columns:280px 280px 1fr}
@media(max-width:1100px){.card-body{grid-template-columns:280px 1fr}}
@media(max-width:680px){.card-body{grid-template-columns:1fr}}

.col-reactions{
  padding:14px 20px;border-right:1px solid #1e2740;
  display:flex;flex-direction:column;gap:10px
}
.section-label{font-size:.58rem;text-transform:uppercase;letter-spacing:.09em;color:#475569;font-weight:600}
.mid-stats{display:flex;gap:28px;align-items:flex-start;flex-wrap:wrap}
.stat-item{display:flex;flex-direction:column;gap:2px}
.stat-lbl{font-size:.6rem;text-transform:uppercase;letter-spacing:.07em;color:#475569;font-weight:600}
.stat-val{font-size:1.1rem;font-weight:700;font-family:ui-monospace,monospace}
.pos{color:#10b981}.neg{color:#ef4444}
.rxn-sep{border-top:1px solid #1e2740;margin:2px 0}
.breakdown{display:flex;flex-direction:column;gap:8px}
.bk-row{display:flex;align-items:center;gap:9px;font-size:.63rem}
.bk-lbl{width:76px;color:#64748b;flex-shrink:0}
.bk-track{flex:1;height:4px;background:#1e2740;border-radius:2px;overflow:hidden}
.bk-fill{height:100%;border-radius:2px}
.bk-val{width:34px;text-align:right;color:#94a3b8;font-family:ui-monospace,monospace;font-weight:600}

/* ── Chart column ── */
.col-chart{display:flex;flex-direction:column;background:#09101f}

/* ── Score column ── */
.col-score{padding:16px 18px;display:flex;flex-direction:column;gap:14px;border-right:1px solid #1e2740}
.score-top{display:flex;align-items:flex-start;gap:14px}
.sc-num-block{display:flex;flex-direction:column;align-items:flex-start;flex-shrink:0}
.sc-num{font-size:3.75rem;font-weight:900;line-height:1;font-variant-numeric:tabular-nums}
.sc-max-lbl{font-size:.65rem;color:#475569;margin-top:4px;font-weight:500;letter-spacing:.04em}
.sc-badges{display:flex;flex-direction:column;gap:5px;padding-top:5px}
.sc-badge{padding:3px 8px;border-radius:5px;font-size:.63rem;font-weight:700;white-space:nowrap;font-family:ui-monospace,monospace}
.chart-controls{
  display:flex;justify-content:space-between;align-items:center;
  padding:8px 14px 7px;flex-shrink:0;border-bottom:1px solid #1e2740
}
.horizon-btns{display:flex;gap:3px}
.hb{padding:3px 10px;border-radius:5px;font-size:.67rem;font-weight:600;cursor:pointer;
  border:1px solid #1e2740;background:transparent;color:#64748b;transition:all .12s}
.hb:hover{border-color:#334155;color:#94a3b8}
.hb.active{background:#1e3a5f;border-color:#3b82f6;color:#60a5fa}
.tip-display{font-size:.75rem;font-family:ui-monospace,monospace;min-height:18px;display:flex;gap:10px}
.tip-price{font-weight:700;color:#e2e8f0}
.tip-date{color:#475569}
.price-canvas{width:100%;flex:1;height:0;min-height:190px;display:block;cursor:crosshair}

/* footer */
footer{text-align:center;padding:28px 0 8px;color:#334155;font-size:.75rem}

/* ── Score info button + modal ── */
.info-btn{background:none;border:none;cursor:pointer;color:#334155;font-size:.85rem;
  line-height:1;padding:2px 6px;border-radius:4px;transition:color .12s;flex-shrink:0}
.info-btn:hover{color:#60a5fa}
.sc-col-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:2px}
.sc-col-lbl{font-size:.58rem;text-transform:uppercase;letter-spacing:.09em;color:#475569;font-weight:600}
.modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.72);
  backdrop-filter:blur(5px);-webkit-backdrop-filter:blur(5px);
  z-index:600;display:flex;align-items:center;justify-content:center;
  opacity:0;pointer-events:none;transition:opacity .18s}
.modal-backdrop.open{opacity:1;pointer-events:all}
.modal-box{background:#0f1629;border:1px solid #1e2740;border-radius:14px;
  padding:28px 32px 24px;max-width:500px;width:90%;position:relative;
  transform:translateY(10px);transition:transform .18s}
.modal-backdrop.open .modal-box{transform:translateY(0)}
.modal-title{font-size:1rem;font-weight:800;color:#f1f5f9;margin-bottom:20px}
.modal-close{position:absolute;top:14px;right:16px;background:none;border:none;
  color:#475569;font-size:1rem;cursor:pointer;padding:4px 8px;border-radius:5px}
.modal-close:hover{background:#1e2740;color:#94a3b8}
.modal-dim{margin-bottom:16px}
.modal-dim-title{font-size:.7rem;font-weight:700;text-transform:uppercase;
  letter-spacing:.08em;margin-bottom:5px}
.modal-dim-body{font-size:.8rem;color:#94a3b8;line-height:1.55}
.modal-footer{font-size:.72rem;color:#64748b;border-top:1px solid #1e2740;
  padding-top:14px;margin-top:4px;line-height:1.6}
"""


def _reaction_bars_svg(individual_reactions):
    """
    Build an inline SVG bar chart for up to 4 quarterly reactions.
    Positive bars are green, negative are red, both grow from a centre zero-line.
    """
    W, H      = 280, 270
    mid_y     = 126         # y-coordinate of the zero line
    max_bar_h = 108         # max bar height above/below the zero line
    bar_w     = 50
    slot_w    = W / 4

    vals    = [r["reaction_pct"] for r in individual_reactions]
    max_abs = max((abs(v) for v in vals), default=0) or 1

    parts = []

    # Zero line
    parts.append(
        f'<line x1="4" y1="{mid_y}" x2="{W - 4}" y2="{mid_y}" '
        f'stroke="#1e2740" stroke-width="1.5"/>'
    )

    for i in range(4):
        cx = slot_w * i + slot_w / 2
        bx = cx - bar_w / 2

        # Q label at the bottom
        parts.append(
            f'<text x="{cx:.1f}" y="{H - 2}" text-anchor="middle" '
            f'fill="#475569" font-size="13" font-family="system-ui,sans-serif">Q{i + 1}</text>'
        )

        if i < len(individual_reactions):
            v      = individual_reactions[i]["reaction_pct"]
            h      = max(4.0, abs(v) / max_abs * max_bar_h)
            color  = "#10b981" if v >= 0 else "#ef4444"
            rect_y = (mid_y - h) if v >= 0 else mid_y

            parts.append(
                f'<rect x="{bx:.1f}" y="{rect_y:.1f}" width="{bar_w}" '
                f'height="{h:.1f}" fill="{color}" opacity=".85" rx="4"/>'
            )

            sign    = "+" if v >= 0 else ""
            val_str = _html.escape(f"{sign}{v:.1f}%")
            txt_y   = max(16.0, rect_y - 7) if v >= 0 else min(H - 18.0, rect_y + h + 16)
            parts.append(
                f'<text x="{cx:.1f}" y="{txt_y:.1f}" text-anchor="middle" '
                f'fill="{color}" font-size="13" font-weight="700" '
                f'font-family="ui-monospace,monospace">{val_str}</text>'
            )
        else:
            # Greyed-out placeholder for a missing quarter
            parts.append(
                f'<rect x="{bx:.1f}" y="{mid_y - 2:.1f}" width="{bar_w}" '
                f'height="4" fill="#1e2740" rx="2"/>'
            )

    return (
        f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" '
        f'style="width:100%;height:{H}px;display:block;overflow:visible;">'
        + "".join(parts)
        + "</svg>"
    )


def _score_color(score):
    if score >= 80: return "#10b981"
    if score >= 60: return "#f59e0b"
    return "#ef4444"


def generate_html_report(results, meta):
    """
    Render a self-contained HTML report for the top 20 results,
    write it to report.html, open it in the default browser, and
    return the absolute file path.
    """
    top = results[:20]
    longs  = sum(1 for r in top if r["signal"] == "LONG")
    shorts = len(top) - longs

    run_date      = _html.escape(meta.get("run_date", ""))
    total_scanned = meta.get("total_scanned", 0)
    window_days   = meta.get("scan_window_days", 7)

    # ── Per-card HTML ───────────────────────────────────────────────────────
    def pct_class(v):
        return "pos" if v >= 0 else "neg"

    def fmt1(v):
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:.1f}%"

    def bk_bar(value, max_val, color, label):
        pct = f"{value / max_val * 100:.0f}%"
        return (
            f'<div class="bk-row">'
            f'<span class="bk-lbl">{label}</span>'
            f'<div class="bk-track"><div class="bk-fill" style="width:{pct};background:{color}"></div></div>'
            f'<span class="bk-val">{value}/{max_val}</span>'
            f'</div>'
        )

    def mpill(key, val):
        na_cls = " mp-na" if val == "N/A" else ""
        return (
            f'<div class="meta-pill">'
            f'<span class="mp-key">{key}</span>'
            f'<span class="mp-val{na_cls}">{_html.escape(str(val))}</span>'
            f'</div>'
        )

    cards = []
    for r in top:
        ticker  = _html.escape(r["ticker"])
        signal  = r["signal"]
        company = _html.escape(r.get("company_name", r["ticker"]))
        date    = _html.escape(r["earnings_date"])
        score   = r["score"]
        sc      = _score_color(score)
        avg_rxn = r["avg_reaction_pct"]
        drift   = r["avg_drift_pct"]
        indiv   = r.get("individual_reactions", [])
        bd      = r.get("score_breakdown", {})
        desc    = _html.escape(r.get("description", ""))
        fund    = r.get("fundamentals", {})
        broker  = r.get("broker_availability", {})
        chart_data_json = r.get("chart_data", "")
        mcap_raw = fund.get("_market_cap_raw", 0)

        badge     = f'<span class="badge badge-{"long" if signal == "LONG" else "short"}">{signal}</span>'
        desc_html = f'<div class="description">{desc}</div>' if desc else ""

        def _av(ok, label):
            cls  = "av av-y" if ok else "av av-n"
            mark = "✓" if ok else "—"
            return f'<span class="{cls}">{label}&nbsp;{mark}</span>'

        broker_html = (
            '<div class="broker-pill">'
            f'<span class="bp-name">Trade&nbsp;Rep.</span>'
            f'{_av(broker.get("tr_stock", False), "Stock")}'
            f'{_av(broker.get("tr_deriv", False), "Deriv")}'
            '</div>'
            '<div class="broker-pill">'
            f'<span class="bp-name">Scalable</span>'
            f'{_av(broker.get("sc_stock", False), "Stock")}'
            f'{_av(broker.get("sc_deriv", False), "Deriv")}'
            '</div>'
        )

        meta_pills = (
            mpill("MCap",    fund.get("market_cap",      "N/A")) +
            mpill("P/E",     fund.get("pe_trailing",     "N/A")) +
            mpill("Fwd P/E", fund.get("pe_forward",      "N/A")) +
            mpill("EPS TTM", fund.get("eps_ttm",         "N/A")) +
            mpill("Rev Grw", fund.get("revenue_growth",  "N/A")) +
            mpill("EPS Grw", fund.get("earnings_growth", "N/A")) +
            mpill("52W Hi",  fund.get("week52_high",     "N/A")) +
            mpill("52W Lo",  fund.get("week52_low",      "N/A"))
        )

        c_score  = bd.get("consistency",     0)
        m_score  = bd.get("magnitude",       0)
        da_score = bd.get("drift_alignment", 0)
        t_score  = bd.get("momentum",        0)

        bk_html = (
            bk_bar(c_score,  25, "#3b82f6", "Consistency") +
            bk_bar(m_score,  25, "#8b5cf6", "Magnitude")   +
            bk_bar(da_score, 25, "#06b6d4", "Drift align") +
            bk_bar(t_score,  25, "#f59e0b", "Momentum")
        )

        cards.append(f"""
  <div class="card" data-signal="{signal}" data-score="{score}" data-earnings-date="{r['earnings_date']}" data-ticker="{ticker}" data-avg-reaction="{avg_rxn}" data-market-cap="{mcap_raw}">

    <div class="card-header">
      <div class="ch-id">
        <span class="ticker-sym">{ticker}</span>{badge}
      </div>
      <div class="ch-name">
        <div class="company-nm">{company}</div>
        {desc_html}
      </div>
      <div class="ch-date">
        <span class="earns-lbl">Earnings</span>
        <span class="earns-val">{date}</span>
      </div>
    </div>

    <div class="card-meta">
      {meta_pills}
      <div class="meta-sep"></div>
      {broker_html}
    </div>

    <div class="card-body">

      <div class="col-score">
        <div class="sc-col-header">
          <span class="sc-col-lbl">Score</span>
          <button class="info-btn" id="scoreInfoBtn" title="How scoring works">ⓘ</button>
        </div>
        <div class="score-top">
          <div class="sc-num-block">
            <span class="sc-num" style="color:{sc}">{score}</span>
            <span class="sc-max-lbl">/ 100</span>
          </div>
          <div class="sc-badges">
            <span class="sc-badge" style="background:rgba(59,130,246,.12);color:#60a5fa;border:1px solid rgba(59,130,246,.25)">Con&nbsp;{c_score}</span>
            <span class="sc-badge" style="background:rgba(139,92,246,.12);color:#a78bfa;border:1px solid rgba(139,92,246,.25)">Mag&nbsp;{m_score}</span>
            <span class="sc-badge" style="background:rgba(6,182,212,.12);color:#22d3ee;border:1px solid rgba(6,182,212,.25)">Drft&nbsp;{da_score}</span>
            <span class="sc-badge" style="background:rgba(245,158,11,.12);color:#fbbf24;border:1px solid rgba(245,158,11,.25)">Mom&nbsp;{t_score}</span>
          </div>
        </div>
        <div class="rxn-sep"></div>
        <div class="breakdown">{bk_html}</div>
      </div>

      <div class="col-reactions">
        <div class="section-label">Post-earnings reactions (Q1 = most recent)</div>
        {_reaction_bars_svg(indiv)}
        <div class="mid-stats">
          <div class="stat-item">
            <span class="stat-lbl">Avg Reaction</span>
            <span class="stat-val {pct_class(avg_rxn)}">{fmt1(avg_rxn)}</span>
          </div>
          <div class="stat-item">
            <span class="stat-lbl">Pre-Drift</span>
            <span class="stat-val {pct_class(drift)}">{fmt1(drift)}</span>
          </div>
        </div>
      </div>

      <div class="col-chart chart-full">
        <div class="chart-controls">
          <div class="tip-display">
            <span class="tip-price"></span>
            <span class="tip-date"></span>
          </div>
          <div class="horizon-btns">
            <button class="hb" data-h="1W">1W</button>
            <button class="hb active" data-h="1M">1M</button>
            <button class="hb" data-h="3M">3M</button>
            <button class="hb" data-h="6M">6M</button>
            <button class="hb" data-h="1Y">1Y</button>
          </div>
        </div>
        <canvas class="price-canvas" data-chart='{chart_data_json}'></canvas>
      </div>

    </div>

  </div>""")

    cards_html = "\n".join(cards)

    # ── Summary stats for the 4 metric cards ───────────────────────────────
    n_sig    = len(top)
    avg_sc   = round(sum(r["score"] for r in top) / n_sig) if top else 0
    top_r    = max(top, key=lambda r: r["score"])              if top else None
    next_r   = min(top, key=lambda r: r["earnings_date"])      if top else None
    top_lbl  = f'{top_r["ticker"]} ({top_r["score"]})' if top_r else "—"
    # Format next earnings date in a friendly way
    next_lbl = "—"
    if next_r:
        try:
            ndate = datetime.strptime(next_r["earnings_date"], "%Y-%m-%d").date()
            today = datetime.now().date()
            if ndate == today:                  next_lbl = "Today"
            elif ndate == today + timedelta(1): next_lbl = "Tomorrow"
            else:                               next_lbl = ndate.strftime("%b %d")
        except Exception:
            next_lbl = next_r["earnings_date"]

    # ── JavaScript (plain string — no f-string, so {} are safe) ───────────
    _JS = """
(function () {
  var cards   = Array.from(document.querySelectorAll('#cardsGrid .card'));
  var grid    = document.getElementById('cardsGrid');
  var empty   = document.getElementById('emptyState');
  var counter = document.getElementById('resCount');

  var sigF = 'all', minScore = 60, dateF = 'all', sortBy = 'score', search = '';

  function run() {
    var now = new Date(); now.setHours(0,0,0,0);
    var w1  = new Date(now); w1.setDate(now.getDate() + 7);
    var w2  = new Date(now); w2.setDate(now.getDate() + 14);

    var vis = cards.filter(function(c) {
      var d = new Date(c.dataset.earningsDate);
      return (
        (sigF === 'all'  || c.dataset.signal === sigF) &&
        (+c.dataset.score >= minScore) &&
        (dateF === 'all'  ||
         (dateF === 'week' && d >= now && d < w1) ||
         (dateF === 'next' && d >= w1  && d < w2)) &&
        (search === '' ||
         c.dataset.ticker.toUpperCase().includes(search.toUpperCase()))
      );
    });

    cards.forEach(function(c) { c.style.display = 'none'; });

    vis.sort(function(a, b) {
      if (sortBy === 'score')    return +b.dataset.score        - +a.dataset.score;
      if (sortBy === 'date')     return a.dataset.earningsDate.localeCompare(b.dataset.earningsDate);
      if (sortBy === 'reaction') return +b.dataset.avgReaction  - +a.dataset.avgReaction;
      if (sortBy === 'mcap')     return +b.dataset.marketCap    - +a.dataset.marketCap;
      return 0;
    });

    vis.forEach(function(c) { c.style.display = ''; grid.appendChild(c); });

    counter.textContent = 'Showing ' + vis.length + ' of ' + cards.length
      + ' signal' + (cards.length !== 1 ? 's' : '');
    empty.style.display = vis.length === 0 ? 'block' : 'none';
  }

  // Signal buttons
  document.querySelectorAll('[data-filter="signal"]').forEach(function(b) {
    b.addEventListener('click', function() {
      document.querySelectorAll('[data-filter="signal"]').forEach(function(x) {
        x.classList.remove('active','long-on','short-on');
      });
      b.classList.add('active');
      if (b.dataset.value === 'LONG')  b.classList.add('long-on');
      if (b.dataset.value === 'SHORT') b.classList.add('short-on');
      sigF = b.dataset.value; run();
    });
  });

  // Date buttons
  document.querySelectorAll('[data-filter="date"]').forEach(function(b) {
    b.addEventListener('click', function() {
      document.querySelectorAll('[data-filter="date"]').forEach(function(x) {
        x.classList.remove('active');
      });
      b.classList.add('active');
      dateF = b.dataset.value; run();
    });
  });

  // Score slider
  var slider   = document.getElementById('scoreSlider');
  var scoreDisp = document.getElementById('scoreDisplay');
  slider.addEventListener('input', function() {
    minScore = +slider.value;
    scoreDisp.textContent = minScore;
    run();
  });

  // Sort
  document.getElementById('sortSel').addEventListener('change', function(e) {
    sortBy = e.target.value; run();
  });

  // Search
  document.getElementById('searchBox').addEventListener('input', function(e) {
    search = e.target.value.trim(); run();
  });

  run();
})();

/* ── Price chart engine ──────────────────────────────────────────────────── */
(function () {
  var HORIZONS = {'1W':5,'1M':22,'3M':66,'6M':130,'1Y':9999};

  function drawChart(canvas, dates, prices, hoverIdx) {
    if (!canvas || !prices || prices.length < 2) return;
    var dpr = window.devicePixelRatio || 1;
    var W   = canvas.offsetWidth;
    var H   = canvas.offsetHeight;
    if (!W || !H) return;
    canvas.width  = W * dpr;
    canvas.height = H * dpr;
    var ctx = canvas.getContext('2d');
    ctx.save(); ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, W, H);

    var pL=58, pR=16, pT=16, pB=28, cW=W-pL-pR, cH=H-pT-pB, n=prices.length;
    var lo=Infinity, hi=-Infinity;
    for (var k=0;k<n;k++){if(prices[k]<lo)lo=prices[k];if(prices[k]>hi)hi=prices[k];}
    var rng=hi-lo||hi*0.002, yLo=lo-rng*.06, yHi=hi+rng*.06, yR=yHi-yLo;
    var isUp=prices[n-1]>=prices[0], col=isUp?'#10b981':'#ef4444';
    function sx(i){return pL+(i/(n-1))*cW;}
    function sy(p){return pT+(1-(p-yLo)/yR)*cH;}

    /* grid */
    ctx.strokeStyle='#1a2638'; ctx.lineWidth=1; ctx.setLineDash([2,5]);
    for(var gi=0;gi<=5;gi++){var gy=pT+(gi/5)*cH;ctx.beginPath();ctx.moveTo(pL,gy);ctx.lineTo(pL+cW,gy);ctx.stroke();}
    ctx.setLineDash([]);

    /* Y labels */
    ctx.fillStyle='#4b5a6e'; ctx.font='10px ui-monospace,monospace'; ctx.textAlign='right';
    for(var yi=0;yi<=5;yi++){
      var lp=yHi-(yi/5)*yR, ly=pT+(yi/5)*cH;
      ctx.fillText('$'+lp.toFixed(2),pL-5,ly+3.5);
    }

    /* X labels */
    var nX=Math.min(6,n); ctx.textAlign='center'; ctx.fillStyle='#4b5a6e';
    ctx.font='10px system-ui,sans-serif';
    for(var xi=0;xi<nX;xi++){
      var di=Math.round(xi/(nX-1)*(n-1)), dx=sx(di);
      var d=new Date(dates[di]+'T12:00:00');
      var lbl=d.toLocaleDateString('en-US',{month:'short',day:'numeric'});
      ctx.fillText(lbl,dx,pT+cH+18);
    }

    /* area gradient */
    var grad=ctx.createLinearGradient(0,pT,0,pT+cH);
    grad.addColorStop(0,isUp?'rgba(16,185,129,.18)':'rgba(239,68,68,.18)');
    grad.addColorStop(1,'rgba(0,0,0,0)');
    ctx.beginPath(); ctx.moveTo(sx(0),sy(prices[0]));
    for(var ai=1;ai<n;ai++) ctx.lineTo(sx(ai),sy(prices[ai]));
    ctx.lineTo(sx(n-1),pT+cH); ctx.lineTo(sx(0),pT+cH); ctx.closePath();
    ctx.fillStyle=grad; ctx.fill();

    /* line */
    ctx.beginPath(); ctx.moveTo(sx(0),sy(prices[0]));
    for(var li=1;li<n;li++) ctx.lineTo(sx(li),sy(prices[li]));
    ctx.strokeStyle=col; ctx.lineWidth=2; ctx.lineJoin='round'; ctx.stroke();

    /* last-price dot */
    ctx.beginPath(); ctx.arc(sx(n-1),sy(prices[n-1]),3.5,0,Math.PI*2);
    ctx.fillStyle=col; ctx.fill();

    /* crosshair + hover dot */
    if (hoverIdx !== null && hoverIdx >= 0 && hoverIdx < n) {
      var hx=sx(hoverIdx), hy=sy(prices[hoverIdx]);
      ctx.strokeStyle='#3d5068'; ctx.lineWidth=1; ctx.setLineDash([3,4]);
      ctx.beginPath(); ctx.moveTo(hx,pT); ctx.lineTo(hx,pT+cH); ctx.stroke();
      ctx.setLineDash([]);
      ctx.beginPath(); ctx.arc(hx,hy,5,0,Math.PI*2);
      ctx.fillStyle=col; ctx.fill();
      ctx.strokeStyle='#0a0e1a'; ctx.lineWidth=1.5; ctx.stroke();
    }
    ctx.restore();
  }

  function initChart(container) {
    var canvas  = container.querySelector('.price-canvas');
    var tipP    = container.querySelector('.tip-price');
    var tipD    = container.querySelector('.tip-date');
    var buttons = container.querySelectorAll('.hb');
    if (!canvas) return;

    var raw;
    try { raw = JSON.parse(canvas.dataset.chart); } catch(e) { return; }
    var allDates=raw.d, allPrices=raw.p, hoverIdx=null, curD, curP;

    function load(days) {
      var s=Math.max(0,allDates.length-days);
      curD=allDates.slice(s); curP=allPrices.slice(s); hoverIdx=null;
      drawChart(canvas,curD,curP,null);
      if(tipP) tipP.textContent=''; if(tipD) tipD.textContent='';
    }

    buttons.forEach(function(b){
      b.addEventListener('click',function(){
        buttons.forEach(function(x){x.classList.remove('active');});
        b.classList.add('active');
        load(HORIZONS[b.dataset.h]||22);
      });
    });

    canvas.addEventListener('mousemove',function(e){
      if(!curP||curP.length<2) return;
      var rect=canvas.getBoundingClientRect();
      var pL=58,pR=16,cW=canvas.offsetWidth-pL-pR;
      var idx=Math.max(0,Math.min(curP.length-1,
              Math.round((e.clientX-rect.left-pL)/cW*(curP.length-1))));
      hoverIdx=idx;
      drawChart(canvas,curD,curP,idx);
      if(tipP) tipP.textContent='$'+curP[idx].toFixed(2);
      if(tipD) tipD.textContent=curD[idx];
    });

    canvas.addEventListener('mouseleave',function(){
      hoverIdx=null; drawChart(canvas,curD,curP,null);
      if(tipP) tipP.textContent=''; if(tipD) tipD.textContent='';
    });

    if(window.ResizeObserver)
      new ResizeObserver(function(){if(curP)drawChart(canvas,curD,curP,hoverIdx);}).observe(canvas);

    load(22); /* default: 1M */
  }

  document.querySelectorAll('.chart-full').forEach(initChart);

  // Score info modal
  var modal = document.getElementById('scoreModal');
  document.querySelectorAll('.info-btn').forEach(function(btn) {
    btn.addEventListener('click', function(e) {
      e.stopPropagation();
      modal.classList.add('open');
    });
  });
  document.getElementById('scoreModalClose').addEventListener('click', function() {
    modal.classList.remove('open');
  });
  modal.addEventListener('click', function(e) {
    if (e.target === modal) modal.classList.remove('open');
  });
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') modal.classList.remove('open');
  });
})();
"""

    # ── Assemble full page ──────────────────────────────────────────────────
    page = (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">\n'
        f'<title>Earnings Scanner — {run_date}</title>\n'
        f'<style>*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}'
        f'body{{background:#08090f;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,'
        f"'Segoe UI',Roboto,sans-serif;min-height:100vh}}"
        f'.page{{max-width:1440px;margin:0 auto;padding:0 0 56px}}'
        f'{_REPORT_CSS}</style>\n'
        '</head>\n<body>\n<div class="page">\n'

        # ── Hero header ────────────────────────────────────────────────────
        '<header class="hero">\n'
        '<div class="hero-top">\n'
        f'<h1 class="hero-title">Earnings Strategy Scanner</h1>\n'
        '<div class="hero-sub">'
        f'<span>{run_date}</span>'
        f'<span class="hero-sep">·</span>'
        f'<span>{total_scanned:,} stocks scanned</span>'
        f'<span class="hero-sep">·</span>'
        f'<span>{window_days}-day window</span>'
        f'<span class="hero-sep">·</span>'
        f'<span class="hero-lc">{longs} LONG</span>'
        f'<span class="hero-sep">/</span>'
        f'<span class="hero-sc">{shorts} SHORT</span>'
        '</div>\n</div>\n'

        # ── 4 summary metric cards ─────────────────────────────────────────
        '<div class="mc-row">\n'
        f'<div class="mc"><div class="mc-val">{n_sig}</div>'
        f'<div class="mc-lbl">Total Signals</div></div>\n'
        f'<div class="mc"><div class="mc-val">{avg_sc}</div>'
        f'<div class="mc-lbl">Avg Score</div></div>\n'
        f'<div class="mc"><div class="mc-val is-ticker">{_html.escape(top_lbl)}</div>'
        f'<div class="mc-lbl">Highest Score</div></div>\n'
        f'<div class="mc"><div class="mc-val">{_html.escape(next_lbl)}</div>'
        f'<div class="mc-lbl">Next Earnings</div></div>\n'
        '</div>\n</header>\n'

        # ── Sticky filter bar ──────────────────────────────────────────────
        '<div class="filter-bar">\n'
        '<div class="filter-inner">\n'
        '<div class="fg"><span class="fl">Signal</span>'
        '<div class="btn-grp">'
        '<button class="fb active" data-filter="signal" data-value="all">All</button>'
        '<button class="fb" data-filter="signal" data-value="LONG">LONG</button>'
        '<button class="fb" data-filter="signal" data-value="SHORT">SHORT</button>'
        '</div></div>\n'

        '<div class="fg">'
        '<span class="fl">Min Score:&nbsp;<span id="scoreDisplay">60</span></span>'
        '<input type="range" id="scoreSlider" min="60" max="100" value="60">'
        '</div>\n'

        '<div class="fg"><span class="fl">Earnings</span>'
        '<div class="btn-grp">'
        '<button class="fb active" data-filter="date" data-value="all">All</button>'
        '<button class="fb" data-filter="date" data-value="week">This Week</button>'
        '<button class="fb" data-filter="date" data-value="next">Next Week</button>'
        '</div></div>\n'

        '<div class="fg"><span class="fl">Sort by</span>'
        '<select class="sort-sel" id="sortSel">'
        '<option value="score">Score</option>'
        '<option value="date">Earnings Date</option>'
        '<option value="reaction">Avg Reaction</option>'
        '<option value="mcap">Market Cap</option>'
        '</select></div>\n'

        '<div class="fg">'
        '<input class="search-box" id="searchBox" type="text" '
        'placeholder="Search ticker…" autocomplete="off">'
        '</div>\n'

        '<div class="res-count" id="resCount">—</div>\n'
        '</div>\n</div>\n'

        # ── Cards grid ─────────────────────────────────────────────────────
        f'<main>\n<div class="grid" id="cardsGrid">\n{cards_html}\n</div>\n'
        '<div class="empty-state" id="emptyState" style="display:none">'
        'No signals match the current filters.</div>\n'
        '</main>\n'

        f'<footer>Top {n_sig} by score &nbsp;·&nbsp; '
        f'Score ≥ 60 &nbsp;·&nbsp; Q1 = most recent past report</footer>\n'
        '</div>\n'

        # ── Score info modal (single instance, shared across all cards) ───────
        '<div class="modal-backdrop" id="scoreModal">'
        '<div class="modal-box">'
        '<button class="modal-close" id="scoreModalClose">✕</button>'
        '<div class="modal-title">How the Score Works</div>'
        '<div class="modal-dim">'
        '<div class="modal-dim-title" style="color:#60a5fa">Consistency · 0–25 pts</div>'
        '<div class="modal-dim-body">Measures how reliably the stock moves in the same direction after earnings. '
        'Full marks require all 4 recent quarters to move the same way — '
        'a stock that always gaps up (or always gaps down) scores highest.</div>'
        '</div>'
        '<div class="modal-dim">'
        '<div class="modal-dim-title" style="color:#a78bfa">Magnitude · 0–25 pts</div>'
        '<div class="modal-dim-body">Rewards larger average post-earnings moves. '
        'A bigger typical swing means more premium to capture for options strategies '
        'and a wider gap to ride for directional trades.</div>'
        '</div>'
        '<div class="modal-dim">'
        '<div class="modal-dim-title" style="color:#22d3ee">Drift Alignment · 0–25 pts</div>'
        '<div class="modal-dim-body">Checks whether the pre-earnings drift — the price move in the '
        'days before the report — aligns with the post-earnings direction. '
        'Strong alignment suggests informed positioning ahead of the release.</div>'
        '</div>'
        '<div class="modal-dim">'
        '<div class="modal-dim-title" style="color:#fbbf24">Momentum · 0–25 pts</div>'
        '<div class="modal-dim-body">Scores the strength of the recent price trend heading into earnings. '
        'Stocks with strong directional momentum tend to continue moving after the report '
        'rather than mean-reverting.</div>'
        '</div>'
        '<div class="modal-footer">'
        'Total score = sum of all four components (max 100).<br>'
        'Scores <strong style="color:#10b981">≥ 80</strong> = strong edge &nbsp;·&nbsp; '
        '<strong style="color:#f59e0b">60–79</strong> = moderate &nbsp;·&nbsp; '
        '<strong style="color:#ef4444">&lt; 60</strong> = weak / filtered out'
        '</div>'
        '</div>'
        '</div>\n'

        f'<script>{_JS}</script>\n'
        '</body>\n</html>'
    )

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report.html")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(page)

    webbrowser.open(f"file://{out_path}")
    return out_path


# ─────────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────────

def send_report_email(report_path: str) -> None:
    sender    = os.environ.get("GMAIL_ADDRESS")
    password  = os.environ.get("GMAIL_APP_PASSWORD")
    recipients_raw = os.environ.get("EMAIL_RECIPIENTS")

    missing = [name for name, val in [
        ("GMAIL_ADDRESS",    sender),
        ("GMAIL_APP_PASSWORD", password),
        ("EMAIL_RECIPIENTS",   recipients_raw),
    ] if not val]

    if missing:
        print(f"  ⚠  Email skipped — missing env var(s): {', '.join(missing)}")
        return

    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    if not recipients:
        print("  ⚠  Email skipped — EMAIL_RECIPIENTS is empty after parsing")
        return

    try:
        with open(report_path, "r", encoding="utf-8") as fh:
            html_body = fh.read()
    except OSError as exc:
        print(f"  ⚠  Email skipped — could not read report: {exc}")
        return

    subject = f"Earnings Strategy Signals — {datetime.now().strftime('%Y-%m-%d')}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ctx) as server:
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        print(f"  ✉  Report emailed to: {', '.join(recipients)}")
    except smtplib.SMTPAuthenticationError:
        print("  ⚠  Email failed — authentication error (check GMAIL_ADDRESS and GMAIL_APP_PASSWORD)")
    except Exception as exc:
        print(f"  ⚠  Email failed — {exc}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  EARNINGS TRADING STRATEGY SCANNER")
    print(f"  Run date : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Window   : next 20 calendar days")
    print(f"  History  : last 4 earnings reports per stock")
    print(f"  Filter   : score >= 60 and signal != NO TRADE")
    print("=" * 62)

    # 1. Build the universe of tickers to check
    tickers = get_tickers()

    # 2. Find tickers with earnings in the next 7 days
    upcoming = get_upcoming_earnings(tickers, days_ahead=20)

    if not upcoming:
        print("No upcoming earnings found in the scan window. Nothing to analyse.")
        return

    # 3. Load cache, then analyse every stock in parallel
    cache            = _load_cache()
    results          = []
    new_cache        = {}   # cache misses fetched this run — written once at the end
    cache_hits       = 0
    cache_misses     = 0
    _lock            = threading.Lock()
    _print_buf       = []   # collect per-ticker lines; print after pool finishes

    def _analyse(item):
        """Worker: return (ticker, earnings_date, reactions, from_cache)."""
        ticker        = item["ticker"]
        earnings_date = item["earnings_date"]

        cached = _cache_get(cache, ticker)
        if cached is not None:
            return ticker, earnings_date, cached, True

        reactions = analyze_historical_earnings(ticker, num_quarters=4)
        return ticker, earnings_date, reactions, False

    print(f"Analysing {len(upcoming)} stock(s) with {_WORKERS} workers…\n")

    with ThreadPoolExecutor(max_workers=_WORKERS) as exe:
        future_map = {exe.submit(_analyse, item): item for item in upcoming}

        for fut in as_completed(future_map):
            try:
                ticker, earnings_date, reactions, from_cache = fut.result()
            except Exception as e:
                with _lock:
                    _print_buf.append(f"  ✗ {future_map[fut]['ticker']}: {e}")
                continue

            with _lock:
                if from_cache:
                    cache_hits += 1
                else:
                    cache_misses += 1

            if reactions is None:
                continue

            # Store freshly fetched data so we can write cache once after the pool
            if not from_cache:
                with _lock:
                    new_cache[ticker] = {
                        "timestamp": datetime.now().isoformat(),
                        "data":      reactions,
                    }

            signal = generate_signal(reactions)
            score  = score_stock(reactions)

            if signal == "NO TRADE" or score < 60:
                continue

            avg_reaction = sum(r["reaction_pct"] for r in reactions) / len(reactions)
            avg_drift    = sum(r["drift_pct"]    for r in reactions) / len(reactions)

            individual = [
                {
                    "quarter":      f"Q{i + 1}",
                    "date":         r["date"],
                    "reaction_pct": r["reaction_pct"],
                    "drift_pct":    r["drift_pct"],
                }
                for i, r in enumerate(reactions)
            ]

            with _lock:
                results.append({
                    "ticker":               ticker,
                    "signal":               signal,
                    "earnings_date":        earnings_date,
                    "score":                score,
                    "avg_reaction_pct":     round(avg_reaction, 2),
                    "avg_drift_pct":        round(avg_drift,    2),
                    "individual_reactions": individual,
                    "score_breakdown":      _score_breakdown(reactions),
                })
                _print_buf.append(
                    f"  ✓ {ticker:<6}  {signal:<6}  score={score:>3}  "
                    f"avg_reaction={_fmt(avg_reaction):>8}  drift={_fmt(avg_drift):>8}"
                    + ("  [cache]" if from_cache else "")
                )

    for line in sorted(_print_buf):   # sort so output is deterministic
        print(line)

    # Persist new entries to disk and report cache efficiency
    cache.update(new_cache)
    _save_cache(cache)
    total = cache_hits + cache_misses
    print(
        f"\n  Cache: {cache_hits}/{total} hits"
        f"  ({cache_hits * 100 // total if total else 0}% saved from API)"
        f"  — {len(new_cache)} new entries written to {_CACHE_FILE}"
    )

    # 5. Enrich the top-20 results with company name, description, and
    #    fundamentals — one ticker.info call per stock, only for finalists
    if results:
        top_n = min(len(results), 20)
        print(f"\nFetching fundamentals for {top_n} signal(s)…")
        for r in results[:top_n]:
            meta = _fetch_ticker_meta(r["ticker"])
            r["company_name"] = meta["company_name"]
            r["description"]  = meta["description"]
            r["chart_data"]   = meta["chart_data"]
            r["fundamentals"] = meta["fundamentals"]
            mcap_raw = meta["fundamentals"].get("_market_cap_raw", 0)
            r["broker_availability"] = _broker_availability(r["ticker"], mcap_raw)

    # 6. Display formatted results table
    print_results_table(results)

    # 7. Save full results to signals.json
    run_date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    output = {
        "generated_at":        datetime.now().isoformat(),
        "scan_window_days":    20,
        "min_score_threshold": 60,
        "total_upcoming":      len(upcoming),
        "total_signals":       len(results),
        "signals":             results,
    }

    output_path = "signals.json"
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Results saved → {output_path}")

    # 8. Generate the HTML report and open it in the default browser
    report_path = generate_html_report(
        results,
        meta={
            "run_date":       run_date_str,
            "total_scanned":  len(upcoming),
            "scan_window_days": 20,
        },
    )
    print(f"Report saved → {report_path}  (opening in browser…)")

    # 9. Send the HTML report by email
    send_report_email(report_path)


if __name__ == "__main__":
    main()
