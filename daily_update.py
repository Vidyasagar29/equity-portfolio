import argparse
import io
import subprocess
import sys
import time
import zipfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests


DATA_DIR = Path("data")
HISTORY_PATH = DATA_DIR / "historical_prices.csv"
NIFTY500_URL = "https://niftyindices.com/IndexConstituent/ind_nifty500list.csv"
NSE_HOME_URL = "https://www.nseindia.com/"
NSE_BHAVCOPY_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
)
NSE_LEGACY_BHAVCOPY_URL = (
    "https://nsearchives.nseindia.com/content/historical/EQUITIES/{yyyy}/{mon}/"
    "cm{dd}{mon}{yyyy}bhav.csv.zip"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


def create_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(NSE_HOME_URL, timeout=15)
    except requests.RequestException:
        pass
    return session


def request_bytes(session, url, retries=3):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            if not response.content:
                raise ValueError(f"Empty response from {url}")
            return response.content
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(attempt * 2)
    raise RuntimeError(f"Failed to download {url}: {last_error}") from last_error


def parse_trade_date(value):
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError("Date must be in YYYY-MM-DD, YYYYMMDD, DD-MM-YYYY, or DD/MM/YYYY format")


def normalize_columns(df):
    renamed = {}
    for column in df.columns:
        normalized = column.strip().upper()
        if normalized in {"SYMBOL", "TCKRSYMB"}:
            renamed[column] = "SYMBOL"
        elif normalized in {"OPEN", "OPEN_PRICE", "OPNPRC", "OPNPRIC"}:
            renamed[column] = "OPEN"
        elif normalized in {"HIGH", "HIGH_PRICE", "HGHPRC", "HGHPRIC"}:
            renamed[column] = "HIGH"
        elif normalized in {"LOW", "LOW_PRICE", "LWPRC", "LWPRIC"}:
            renamed[column] = "LOW"
        elif normalized in {"CLOSE", "CLOSE_PRICE", "CLSPRIC"}:
            renamed[column] = "CLOSE"
        elif normalized in {"VOLUME", "TTLTRD_QNTY", "TOTTRDQTY", "TTLTRADGVOL"}:
            renamed[column] = "VOLUME"
        elif normalized in {"SERIES", "SCTYSRS"}:
            renamed[column] = "SERIES"
        elif normalized == "INDUSTRY":
            renamed[column] = "INDUSTRY"
    return df.rename(columns=renamed)


def read_csv_from_zip(zip_bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        csv_files = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_files:
            raise RuntimeError("Bhavcopy ZIP does not contain a CSV file")
        with archive.open(csv_files[0]) as csv_file:
            return pd.read_csv(csv_file, keep_default_na=False)


def bhavcopy_urls(trade_date):
    return [
        NSE_BHAVCOPY_URL.format(yyyymmdd=trade_date.strftime("%Y%m%d")),
        NSE_LEGACY_BHAVCOPY_URL.format(
            yyyy=trade_date.strftime("%Y"),
            mon=trade_date.strftime("%b").upper(),
            dd=trade_date.strftime("%d"),
        ),
    ]


def download_bhavcopy(trade_date):
    session = create_session()
    errors = []
    for url in bhavcopy_urls(trade_date):
        try:
            return normalize_columns(read_csv_from_zip(request_bytes(session, url)))
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("Unable to download Bhavcopy:\n" + "\n".join(errors))


def load_nifty500():
    session = create_session()
    nifty = pd.read_csv(io.BytesIO(request_bytes(session, NIFTY500_URL)), keep_default_na=False)
    nifty = normalize_columns(nifty)
    nifty["SYMBOL"] = nifty["SYMBOL"].astype(str).str.strip().str.upper()
    nifty["INDUSTRY"] = nifty["INDUSTRY"].astype(str).str.strip()
    if "SERIES" in nifty.columns:
        nifty["SERIES"] = nifty["SERIES"].astype(str).str.strip().str.upper()
        nifty = nifty[nifty["SERIES"].eq("EQ")]
    return nifty.loc[:, ["SYMBOL", "INDUSTRY"]].drop_duplicates("SYMBOL")


def filter_nifty500(bhav, nifty):
    bhav = bhav.copy()
    bhav["SYMBOL"] = bhav["SYMBOL"].astype(str).str.strip().str.upper()
    if "SERIES" in bhav.columns:
        bhav["SERIES"] = bhav["SERIES"].astype(str).str.strip().str.upper()
        bhav = bhav[bhav["SERIES"].eq("EQ")]

    needed = ["SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
    missing = [col for col in needed if col not in bhav.columns]
    if missing:
        raise RuntimeError(f"Bhavcopy missing columns: {missing}")

    filtered = bhav[bhav["SYMBOL"].isin(set(nifty["SYMBOL"]))].copy()
    filtered = filtered.merge(nifty, on="SYMBOL", how="left")
    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
        filtered[col] = pd.to_numeric(filtered[col], errors="coerce")
    return filtered.loc[:, ["SYMBOL", "INDUSTRY", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]]


def prepare_daily_rows(filtered, trade_date):
    daily = filtered.copy()
    daily.columns = [col.lower() for col in daily.columns]
    daily["date"] = trade_date.isoformat()
    daily["symbol"] = daily["symbol"].astype(str).str.strip().str.upper()
    daily["industry"] = daily["industry"].astype(str).str.strip()
    for col in ["open", "high", "low", "close", "volume"]:
        daily[col] = pd.to_numeric(daily[col], errors="coerce")
    daily = daily.dropna(subset=["close"])
    return daily.loc[:, ["date", "symbol", "industry", "open", "high", "low", "close", "volume"]]


def load_history():
    if not HISTORY_PATH.exists():
        return pd.DataFrame(columns=["date", "symbol", "industry", "open", "high", "low", "close", "volume"])
    history = pd.read_csv(HISTORY_PATH)
    if history.empty:
        return pd.DataFrame(columns=["date", "symbol", "industry", "open", "high", "low", "close", "volume"])
    history["date"] = pd.to_datetime(history["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    history["symbol"] = history["symbol"].astype(str).str.strip().str.upper()
    return history


def save_history(history):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    history["date"] = pd.to_datetime(history["date"], errors="coerce")
    history = history.sort_values(["date", "symbol"])
    history["date"] = history["date"].dt.strftime("%Y-%m-%d")
    history.to_csv(HISTORY_PATH, index=False)


def run_step(args):
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"Failed: {' '.join(args)}")
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description="Download daily NSE Bhavcopy, update CSV history, recalc indicators, refresh portfolio and dashboard.")
    parser.add_argument("date", nargs="?", default=date.today().isoformat(), help="Trade date in YYYY-MM-DD. Default: today")
    args = parser.parse_args()

    trade_date = parse_trade_date(args.date)
    bhav = download_bhavcopy(trade_date)
    nifty = load_nifty500()
    filtered = filter_nifty500(bhav, nifty)
    daily_rows = prepare_daily_rows(filtered, trade_date)
    if daily_rows.empty:
        raise RuntimeError(f"No NIFTY 500 rows found in Bhavcopy for {trade_date.isoformat()}")

    history = load_history()
    trade_date_str = trade_date.isoformat()
    before_rows = int((history["date"] == trade_date_str).sum()) if not history.empty else 0
    expected_rows = int(daily_rows["symbol"].nunique())

    if before_rows >= expected_rows:
        print(
            f"Data for {trade_date_str} was already present "
            f"({before_rows} rows). Skipping indicator and portfolio refresh."
        )
        return

    history = history[history["date"] != trade_date_str].copy()
    history = pd.concat([history, daily_rows], ignore_index=True)
    save_history(history)

    refreshed = load_history()
    after_rows = int((refreshed["date"] == trade_date_str).sum()) if not refreshed.empty else 0
    append_confirmed = after_rows >= expected_rows and after_rows > before_rows

    if not append_confirmed:
        raise RuntimeError(
            f"Append not confirmed for {trade_date_str}: expected {expected_rows} rows, got {after_rows}."
        )

    run_step([sys.executable, "calculate_indicators.py", "--date", trade_date_str])
    run_step([sys.executable, "export_csv_data.py"])
    run_step([sys.executable, "simulate_portfolio.py"])
    run_step([sys.executable, "export_terminal_data.py"])

    print(
        f"Updated {len(daily_rows)} NIFTY 500 rows for {trade_date_str} "
        f"and refreshed technical indicators + portfolio + terminal data."
    )


if __name__ == "__main__":
    main()
