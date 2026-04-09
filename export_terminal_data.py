import json
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
OUT_DIR = Path("terminal") / "data"


def read_csv(name):
    path = DATA_DIR / name
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, default=str, separators=(",", ":")), encoding="utf-8")


def write_js_data(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    content = f"window.TERMINAL_DATA = {json.dumps(payload, default=str, separators=(',', ':'))};\n"
    path.write_text(content, encoding="utf-8")


def build_history_by_symbol(history_df, technical_df):
    if history_df.empty:
        return {}
    history_df = history_df.copy()
    history_df["date"] = pd.to_datetime(history_df["date"], errors="coerce")
    history_df = history_df.sort_values(["symbol", "date"])
    history_df["date"] = history_df["date"].dt.strftime("%Y-%m-%d")
    history_df = history_df.rename(columns={"date": "trade_date"})

    if not technical_df.empty:
        technical_df = technical_df.copy()
        technical_df["trade_date"] = pd.to_datetime(technical_df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        keep_cols = [
            "trade_date",
            "symbol",
            "sma_20",
            "sma_50",
            "sma_200",
            "macd",
            "macd_signal",
            "bollinger_upper",
            "bollinger_middle",
            "bollinger_lower",
            "rsi_14",
        ]
        technical_df = technical_df.loc[:, [col for col in keep_cols if col in technical_df.columns]]
        history_df = history_df.merge(technical_df, on=["trade_date", "symbol"], how="left")

    records = history_df.to_dict(orient="records")

    history_by_symbol = {}
    for row in records:
        history_by_symbol.setdefault(row["symbol"], []).append(row)
    return history_by_symbol


def build_metadata(rankings_df, history_df, technical_df):
    history_dates = pd.to_datetime(history_df.get("date"), errors="coerce") if not history_df.empty else pd.Series(dtype="datetime64[ns]")
    tech_dates = pd.to_datetime(technical_df.get("trade_date"), errors="coerce") if not technical_df.empty else pd.Series(dtype="datetime64[ns]")

    tables = [
        {
            "table_name": "historical_prices.csv",
            "rows": int(len(history_df)),
            "symbols": int(history_df["symbol"].nunique()) if not history_df.empty else 0,
            "start_date": history_dates.min().strftime("%Y-%m-%d") if not history_dates.empty and pd.notna(history_dates.min()) else None,
            "end_date": history_dates.max().strftime("%Y-%m-%d") if not history_dates.empty and pd.notna(history_dates.max()) else None,
        },
        {
            "table_name": "technical_indicators.csv",
            "rows": int(len(technical_df)),
            "symbols": int(technical_df["symbol"].nunique()) if not technical_df.empty else 0,
            "start_date": tech_dates.min().strftime("%Y-%m-%d") if not tech_dates.empty and pd.notna(tech_dates.min()) else None,
            "end_date": tech_dates.max().strftime("%Y-%m-%d") if not tech_dates.empty and pd.notna(tech_dates.max()) else None,
        },
        {
            "table_name": "latest_rankings.csv",
            "rows": int(len(rankings_df)),
            "symbols": int(rankings_df["symbol"].nunique()) if not rankings_df.empty else 0,
            "start_date": str(rankings_df["trade_date"].iloc[0]) if not rankings_df.empty else None,
            "end_date": str(rankings_df["trade_date"].iloc[0]) if not rankings_df.empty else None,
        },
    ]

    return {
        "rank_date": str(rankings_df["trade_date"].iloc[0]) if not rankings_df.empty else None,
        "history_rows": int(len(history_df)),
        "history_symbols": int(history_df["symbol"].nunique()) if not history_df.empty else 0,
        "history_start": tables[0]["start_date"],
        "history_end": tables[0]["end_date"],
        "tables": tables,
        "source": "csv",
    }


def build_portfolio_payload():
    portfolio = {}
    for name in ["portfolio_positions", "portfolio_trades", "portfolio_nav", "portfolio_signals"]:
        df = read_csv(f"{name}.csv")
        portfolio[name] = df.to_dict(orient="records") if not df.empty else []
    return portfolio


def export_data():
    rankings_df = read_csv("latest_rankings.csv")
    history_df = read_csv("historical_prices.csv")
    technical_df = read_csv("technical_indicators.csv")

    history_by_symbol = build_history_by_symbol(history_df, technical_df)
    metadata = build_metadata(rankings_df, history_df, technical_df)
    portfolio = build_portfolio_payload()
    rankings = rankings_df.to_dict(orient="records") if not rankings_df.empty else []

    payload = {
        "rankings": rankings,
        "historyBySymbol": history_by_symbol,
        "metadata": metadata,
        "portfolio": portfolio,
    }

    write_json(OUT_DIR / "rankings.json", rankings)
    write_json(OUT_DIR / "history.json", history_by_symbol)
    write_json(OUT_DIR / "metadata.json", metadata)
    write_json(OUT_DIR / "portfolio.json", portfolio)
    write_js_data(OUT_DIR / "terminal_data.js", payload)
    print(f"Exported terminal data to {OUT_DIR}")


if __name__ == "__main__":
    export_data()
