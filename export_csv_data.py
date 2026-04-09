from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
HISTORY_PATH = DATA_DIR / "historical_prices.csv"
TECHNICAL_PATH = DATA_DIR / "technical_indicators.csv"
LATEST_PATH = DATA_DIR / "latest_rankings.csv"


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    outputs = []

    if HISTORY_PATH.exists():
        history = pd.read_csv(HISTORY_PATH)
        history["date"] = pd.to_datetime(history["date"], errors="coerce")
        history = history.sort_values(["date", "symbol"])
        history["date"] = history["date"].dt.strftime("%Y-%m-%d")
        history.to_csv(HISTORY_PATH, index=False)
        outputs.append((HISTORY_PATH, len(history)))

    if TECHNICAL_PATH.exists():
        technical = pd.read_csv(TECHNICAL_PATH)
        technical["trade_date"] = pd.to_datetime(technical["trade_date"], errors="coerce")
        technical = technical.sort_values(["trade_date", "symbol"])
        technical["trade_date"] = technical["trade_date"].dt.strftime("%Y-%m-%d")
        technical.to_csv(TECHNICAL_PATH, index=False)
        outputs.append((TECHNICAL_PATH, len(technical)))

        if not technical.empty:
            latest_date = technical["trade_date"].max()
            latest = technical[technical["trade_date"].eq(latest_date)].sort_values(
                ["score", "symbol"], ascending=[False, True]
            )
            latest.to_csv(LATEST_PATH, index=False)
            outputs.append((LATEST_PATH, len(latest)))
    elif not LATEST_PATH.exists():
        pd.DataFrame().to_csv(LATEST_PATH, index=False)

    for path, count in outputs:
        print(f"Wrote {count} rows to {path}")


if __name__ == "__main__":
    main()
