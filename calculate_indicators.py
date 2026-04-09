import argparse
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
HISTORY_PATH = DATA_DIR / "historical_prices.csv"
TECHNICAL_PATH = DATA_DIR / "technical_indicators.csv"
LATEST_PATH = DATA_DIR / "latest_rankings.csv"


TECHNICAL_COLUMNS = [
    "trade_date",
    "symbol",
    "industry",
    "close",
    "sma_20",
    "sma_50",
    "sma_200",
    "ema_12",
    "ema_26",
    "macd",
    "macd_signal",
    "bollinger_upper",
    "bollinger_middle",
    "bollinger_lower",
    "rsi_14",
    "return_20d",
    "volatility_20d",
    "score",
    "recommendation",
    "return_6m_ex_1m",
    "return_11m_ex_1m",
    "volatility_60d",
    "atr_14_pct",
    "distance_52w_high_pct",
    "trend_score",
    "momentum_score",
    "industry_relative_score",
    "volatility_score",
    "confirmation_score",
    "industry_rank_pct",
    "peer_rank_pct",
    "universe_momentum_rank_pct",
    "risk_adjusted_momentum_rank_pct",
    "updated_at",
]


def load_history():
    if not HISTORY_PATH.exists():
        raise RuntimeError(f"Missing historical data: {HISTORY_PATH}")
    df = pd.read_csv(HISTORY_PATH, parse_dates=["date"])
    if df.empty:
        raise RuntimeError("No historical price rows found in historical_prices.csv")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.rename(columns={"date": "trade_date"})


def percentile_rank(series):
    if series.notna().sum() <= 1:
        return pd.Series(50.0, index=series.index)
    return series.rank(pct=True, method="average") * 100.0


def weighted_available(a, b, wa=0.4, wb=0.6):
    both = a.notna() & b.notna()
    only_a = a.notna() & b.isna()
    only_b = a.isna() & b.notna()
    out = pd.Series(float("nan"), index=a.index, dtype="float64")
    out.loc[both] = wa * a.loc[both] + wb * b.loc[both]
    out.loc[only_a] = a.loc[only_a]
    out.loc[only_b] = b.loc[only_b]
    return out


def compute_symbol_features(df):
    df = df.sort_values(["symbol", "trade_date"]).copy()
    grouped = df.groupby("symbol", group_keys=False)

    df["sma_20"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["sma_50"] = grouped["close"].transform(lambda s: s.rolling(50, min_periods=50).mean())
    df["sma_200"] = grouped["close"].transform(lambda s: s.rolling(200, min_periods=200).mean())
    df["ema_12"] = grouped["close"].transform(lambda s: s.ewm(span=12, adjust=False, min_periods=12).mean())
    df["ema_26"] = grouped["close"].transform(lambda s: s.ewm(span=26, adjust=False, min_periods=26).mean())
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = grouped["macd"].transform(lambda s: s.ewm(span=9, adjust=False, min_periods=9).mean())

    rolling_std_20 = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).std())
    df["bollinger_middle"] = df["sma_20"]
    df["bollinger_upper"] = df["bollinger_middle"] + 2 * rolling_std_20
    df["bollinger_lower"] = df["bollinger_middle"] - 2 * rolling_std_20

    delta = grouped["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).mean())
    avg_loss = loss.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).mean())
    rs = avg_gain / avg_loss
    df["rsi_14"] = 100 - (100 / (1 + rs))
    df.loc[(avg_loss == 0) & (avg_gain > 0), "rsi_14"] = 100

    daily_returns = grouped["close"].pct_change()
    df["return_20d"] = grouped["close"].pct_change(20) * 100
    df["volatility_20d"] = daily_returns.groupby(df["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).std() * 100
    )
    df["volatility_60d"] = daily_returns.groupby(df["symbol"]).transform(
        lambda s: s.rolling(60, min_periods=60).std() * 100
    )

    df["close_lag_21"] = grouped["close"].shift(21)
    df["close_lag_126"] = grouped["close"].shift(126)
    df["close_lag_231"] = grouped["close"].shift(231)
    df["return_6m_ex_1m"] = ((df["close_lag_21"] / df["close_lag_126"]) - 1.0) * 100
    df["return_11m_ex_1m"] = ((df["close_lag_21"] / df["close_lag_231"]) - 1.0) * 100

    rolling_high_252 = grouped["high"].transform(lambda s: s.rolling(252, min_periods=252).max())
    df["distance_52w_high_pct"] = ((df["close"] / rolling_high_252) - 1.0) * 100

    prev_close = grouped["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr_14_pct"] = (
        true_range.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).mean())
        / df["close"]
        * 100
    )

    df["sma_200_slope"] = grouped["sma_200"].diff(20)
    return df


def score_latest_day(df, as_of_date):
    latest = df[df["trade_date"].dt.date.eq(as_of_date)].copy()
    if latest.empty:
        raise RuntimeError(f"No history rows found for {as_of_date.isoformat()}")

    latest["mom_blended"] = weighted_available(latest["return_6m_ex_1m"], latest["return_11m_ex_1m"])
    latest["blended_volatility"] = 0.5 * latest["volatility_20d"] + 0.5 * latest["volatility_60d"]
    latest["risk_adjusted_momentum"] = latest["mom_blended"] / latest["blended_volatility"].replace(0, pd.NA)

    latest["universe_momentum_rank_6_pct"] = percentile_rank(latest["return_6m_ex_1m"])
    latest["universe_momentum_rank_11_pct"] = percentile_rank(latest["return_11m_ex_1m"])
    latest["universe_momentum_rank_pct"] = percentile_rank(latest["mom_blended"])
    latest["risk_adjusted_momentum_rank_pct"] = percentile_rank(latest["risk_adjusted_momentum"])

    industry = latest.groupby("industry", dropna=False).agg(
        industry_mom_6=("return_6m_ex_1m", "mean"),
        industry_mom_11=("return_11m_ex_1m", "mean"),
    ).reset_index()
    industry["industry_strength"] = weighted_available(industry["industry_mom_6"], industry["industry_mom_11"])
    industry["industry_rank_pct"] = percentile_rank(industry["industry_strength"])
    latest = latest.merge(industry[["industry", "industry_rank_pct"]], on="industry", how="left")

    latest["peer_rank_6"] = latest.groupby("industry")["return_6m_ex_1m"].transform(percentile_rank)
    latest["peer_rank_11"] = latest.groupby("industry")["return_11m_ex_1m"].transform(percentile_rank)
    latest["peer_rank_pct"] = weighted_available(latest["peer_rank_6"], latest["peer_rank_11"])

    trend_score = pd.Series(0.0, index=latest.index)
    trend_score += (latest["close"] > latest["sma_50"]).fillna(False).astype(float) * 8
    trend_score += (latest["close"] > latest["sma_200"]).fillna(False).astype(float) * 8
    trend_score += (latest["sma_50"] > latest["sma_200"]).fillna(False).astype(float) * 6
    trend_score += (latest["sma_200_slope"] > 0).fillna(False).astype(float) * 4
    trend_score += (latest["distance_52w_high_pct"] >= -10).fillna(False).astype(float) * 4
    latest["trend_score"] = trend_score.clip(0, 30)

    latest["momentum_rank_combined_pct"] = weighted_available(
        latest["universe_momentum_rank_6_pct"], latest["universe_momentum_rank_11_pct"]
    )
    latest["momentum_score"] = (latest["momentum_rank_combined_pct"] * 0.30).clip(0, 30)
    latest["industry_relative_score"] = (
        (0.5 * latest["industry_rank_pct"] + 0.5 * latest["peer_rank_pct"]) * 0.20
    ).clip(0, 20)

    volatility_score = latest["risk_adjusted_momentum_rank_pct"] * 0.15
    instability_penalty = pd.Series(0.0, index=latest.index)
    instability_penalty += ((latest["volatility_20d"] > latest["volatility_60d"] * 1.4).fillna(False).astype(float) * 4)
    instability_penalty += ((latest["atr_14_pct"] > 5).fillna(False).astype(float) * 3)
    latest["volatility_score"] = (volatility_score - instability_penalty).clip(0, 15)

    confirmation_score = pd.Series(0.0, index=latest.index)
    confirmation_score += (latest["macd"] > latest["macd_signal"]).fillna(False).astype(float) * 2
    confirmation_score += (latest["rsi_14"].between(50, 68)).fillna(False).astype(float) * 2
    confirmation_score += ((latest["close"] <= latest["bollinger_upper"] * 1.01)).fillna(False).astype(float) * 1
    latest["confirmation_score"] = confirmation_score.clip(0, 5)

    latest["score"] = (
        latest["trend_score"]
        + latest["momentum_score"]
        + latest["industry_relative_score"]
        + latest["volatility_score"]
        + latest["confirmation_score"]
    ).round(2)
    latest["score"] = latest["score"].fillna(0.0)

    hard_sell = (
        (latest["close"] < latest["sma_200"])
        | (latest["sma_50"] < latest["sma_200"])
        | (latest["return_11m_ex_1m"] < 0)
        | (latest["industry_rank_pct"] < 30)
        | (latest["volatility_20d"] > latest["volatility_60d"] * 1.8)
    )
    latest["recommendation"] = "HOLD"
    latest.loc[latest["score"] >= 75, "recommendation"] = "BUY"
    latest.loc[(latest["score"] < 50) | hard_sell.fillna(False), "recommendation"] = "SELL"
    latest.loc[(latest["score"].between(50, 74.999)) & ~hard_sell.fillna(False), "recommendation"] = "HOLD"
    latest["updated_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    return latest


def save_scores(latest):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rows = latest.where(pd.notna(latest), None).copy()
    rows["trade_date"] = rows["trade_date"].dt.strftime("%Y-%m-%d")
    trade_date = rows["trade_date"].iloc[0]
    rows = rows[TECHNICAL_COLUMNS]

    if TECHNICAL_PATH.exists():
        existing = pd.read_csv(TECHNICAL_PATH)
        existing = existing[existing["trade_date"].astype(str) != trade_date]
        combined = pd.concat([existing, rows], ignore_index=True)
    else:
        combined = rows.copy()

    combined["trade_date"] = pd.to_datetime(combined["trade_date"], errors="coerce")
    combined = combined.sort_values(["trade_date", "symbol"])
    combined["trade_date"] = combined["trade_date"].dt.strftime("%Y-%m-%d")
    combined.to_csv(TECHNICAL_PATH, index=False)

    latest_rankings = rows.sort_values(["score", "symbol"], ascending=[False, True])
    latest_rankings.to_csv(LATEST_PATH, index=False)


def main():
    parser = argparse.ArgumentParser(description="Calculate institutional-style technical indicators and rankings from CSV data.")
    parser.add_argument("--date", help="Specific trade date in YYYY-MM-DD. Default: latest date in history.")
    parser.add_argument("--reset-table", action="store_true", help="Rebuild technical_indicators.csv from scratch.")
    args = parser.parse_args()

    history = load_history()
    features = compute_symbol_features(history)
    as_of_date = pd.to_datetime(args.date).date() if args.date else features["trade_date"].dt.date.max()
    latest = score_latest_day(features, as_of_date)

    if args.reset_table and TECHNICAL_PATH.exists():
        TECHNICAL_PATH.unlink()
    save_scores(latest)

    print(
        f"Calculated {len(latest)} technical rows for {as_of_date.isoformat()} "
        f"| BUY {(latest['recommendation'] == 'BUY').sum()} "
        f"| HOLD {(latest['recommendation'] == 'HOLD').sum()} "
        f"| SELL {(latest['recommendation'] == 'SELL').sum()}"
    )


if __name__ == "__main__":
    main()
