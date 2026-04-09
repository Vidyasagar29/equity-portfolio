import argparse
from pathlib import Path

import pandas as pd


DATA_DIR = Path("data")
INITIAL_CAPITAL = 10_000_000.0
MIN_HOLDINGS = 22
MAX_HOLDINGS = 25
MAX_WEIGHT = 0.05
SECTOR_CAP = 0.25
BUY_THRESHOLD = 75.0
HOLD_THRESHOLD = 50.0


def load_csv(name):
    path = DATA_DIR / name
    if not path.exists():
        raise SystemExit(f"Missing required file: {path}")
    return pd.read_csv(path)


def prepare_data():
    prices = load_csv("historical_prices.csv")
    indicators = load_csv("technical_indicators.csv")

    prices["date"] = pd.to_datetime(prices["date"])
    indicators["trade_date"] = pd.to_datetime(indicators["trade_date"])
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    indicators["score"] = pd.to_numeric(indicators["score"], errors="coerce")
    return prices, indicators


def build_candidate_pool(day_frame, exclude_symbols=None, include_holds=True):
    exclude_symbols = set(exclude_symbols or [])
    buy_pool = day_frame[day_frame["recommendation"].eq("BUY")].copy()
    frames = [buy_pool]
    if include_holds:
        hold_pool = day_frame[
            day_frame["recommendation"].eq("HOLD") & day_frame["score"].ge(HOLD_THRESHOLD + 10)
        ].copy()
        frames.append(hold_pool)
    ranked = pd.concat(frames, ignore_index=True)
    if exclude_symbols:
        ranked = ranked[~ranked["symbol"].isin(exclude_symbols)]
    ranked = ranked.sort_values(
        ["score", "industry_relative_score", "risk_adjusted_momentum_rank_pct", "symbol"],
        ascending=[False, False, False, True],
    )
    return ranked


def choose_targets(day_frame):
    ranked = build_candidate_pool(day_frame, include_holds=False)

    selected = []
    industry_weights = {}
    target_weight = min(MAX_WEIGHT, 1.0 / max(MIN_HOLDINGS, MAX_HOLDINGS))

    for row in ranked.itertuples(index=False):
        if len(selected) >= MAX_HOLDINGS:
            break
        current_industry_weight = industry_weights.get(row.industry, 0.0)
        if current_industry_weight + target_weight > SECTOR_CAP:
            continue
        selected.append(row)
        industry_weights[row.industry] = current_industry_weight + target_weight

    if len(selected) < MIN_HOLDINGS:
        hold_ranked = build_candidate_pool(day_frame, include_holds=True)
        remaining = hold_ranked[~hold_ranked["symbol"].isin([row.symbol for row in selected])]
        for row in remaining.itertuples(index=False):
            if len(selected) >= MIN_HOLDINGS:
                break
            selected.append(row)

    if not selected:
        return pd.DataFrame(columns=day_frame.columns.tolist() + ["target_weight"])

    target = pd.DataFrame(selected)
    target["target_weight"] = 1.0 / len(target)
    target["target_weight"] = target["target_weight"].clip(upper=MAX_WEIGHT)
    cash_buffer = 1.0 - target["target_weight"].sum()
    if cash_buffer < 0:
        target["target_weight"] *= (1.0 / target["target_weight"].sum())
    return target


def pick_replacements(day_frame, positions, open_slots):
    if open_slots <= 0:
        return pd.DataFrame(columns=day_frame.columns.tolist() + ["target_weight"])

    ranked = build_candidate_pool(day_frame, exclude_symbols=positions.keys(), include_holds=False)
    selected = []
    current_industry_counts = {}
    for info in positions.values():
        current_industry_counts[info["industry"]] = current_industry_counts.get(info["industry"], 0) + 1

    max_industry_positions = max(1, int(MAX_HOLDINGS * SECTOR_CAP))

    for row in ranked.itertuples(index=False):
        if len(selected) >= open_slots:
            break
        if current_industry_counts.get(row.industry, 0) >= max_industry_positions:
            continue
        selected.append(row)
        current_industry_counts[row.industry] = current_industry_counts.get(row.industry, 0) + 1

    if len(selected) < open_slots:
        hold_ranked = build_candidate_pool(day_frame, exclude_symbols=set(positions.keys()) | {row.symbol for row in selected})
        for row in hold_ranked.itertuples(index=False):
            if len(selected) >= open_slots:
                break
            if current_industry_counts.get(row.industry, 0) >= max_industry_positions:
                continue
            selected.append(row)
            current_industry_counts[row.industry] = current_industry_counts.get(row.industry, 0) + 1

    if not selected:
        return pd.DataFrame(columns=day_frame.columns.tolist() + ["target_weight"])

    target = pd.DataFrame(selected)
    target["target_weight"] = min(MAX_WEIGHT, 1.0 / MAX_HOLDINGS)
    return target


def simulate(prices, indicators, initial_capital):
    indicator_dates = sorted(indicators["trade_date"].dt.date.unique())
    positions = {}
    cash = initial_capital
    trades = []
    nav_rows = []

    price_lookup = {
        (row.date.date(), row.symbol): row.close
        for row in prices.loc[:, ["date", "symbol", "close"]].itertuples(index=False)
        if pd.notna(row.close)
    }

    for trade_date in indicator_dates:
        day_indicators = indicators[indicators["trade_date"].dt.date.eq(trade_date)].copy()
        current_symbols = list(positions.keys())
        initial_build = not positions

        # Build the initial portfolio only once. After that, monitor existing names and replace only sold exits.
        if initial_build:
            targets = choose_targets(day_indicators)
        else:
            targets = pd.DataFrame(columns=day_indicators.columns.tolist() + ["target_weight"])

        for symbol in current_symbols:
            close_price = price_lookup.get((trade_date, symbol))
            signal_frame = day_indicators.loc[day_indicators["symbol"].eq(symbol)]
            if not signal_frame.empty:
                positions[symbol]["score"] = signal_frame["score"].iloc[0]
                positions[symbol]["recommendation"] = signal_frame["recommendation"].iloc[0]
            if close_price is None:
                continue

            should_exit = (not signal_frame.empty) and signal_frame["recommendation"].iloc[0] == "SELL"
            if should_exit:
                quantity = positions[symbol]["quantity"]
                proceeds = quantity * close_price
                cash += proceeds
                trades.append(
                    {
                        "date": trade_date.isoformat(),
                        "symbol": symbol,
                        "action": "SELL",
                        "price": round(close_price, 2),
                        "quantity": quantity,
                        "value": round(proceeds, 2),
                        "reason": "Recommendation changed to SELL",
                    }
                )
                del positions[symbol]

        # Refresh market value after exits
        portfolio_value = cash + sum(
            positions[s]["quantity"] * price_lookup.get((trade_date, s), positions[s]["last_price"])
            for s in positions
        )

        if not positions and targets.empty:
            targets = choose_targets(day_indicators)
        elif positions and len(positions) < MAX_HOLDINGS:
            open_slots = MAX_HOLDINGS - len(positions)
            targets = pick_replacements(day_indicators, positions, open_slots)

        # Enter new names only when opening the first portfolio or replacing sold positions.
        remaining_slots = len(targets)
        for row in targets.itertuples(index=False):
            close_price = price_lookup.get((trade_date, row.symbol))
            if close_price is None or close_price <= 0:
                remaining_slots = max(0, remaining_slots - 1)
                continue
            target_value = min(portfolio_value * row.target_weight, cash / max(1, remaining_slots))
            target_quantity = int(target_value // close_price)
            current_quantity = positions.get(row.symbol, {}).get("quantity", 0)
            delta = target_quantity - current_quantity
            if delta <= 0:
                if row.symbol in positions:
                    positions[row.symbol]["last_price"] = close_price
                remaining_slots = max(0, remaining_slots - 1)
                continue

            trade_value = delta * close_price
            if trade_value > cash:
                affordable = int(cash // close_price)
                delta = max(0, affordable)
                trade_value = delta * close_price
            if delta == 0:
                continue

            cash -= trade_value
            positions[row.symbol] = {
                "quantity": current_quantity + delta,
                "last_price": close_price,
                "industry": row.industry,
                "entry_date": positions.get(row.symbol, {}).get("entry_date", trade_date.isoformat()),
                "score": row.score,
                "recommendation": row.recommendation,
                "target_weight": row.target_weight,
            }
            trades.append(
                {
                    "date": trade_date.isoformat(),
                    "symbol": row.symbol,
                    "action": "BUY",
                    "price": round(close_price, 2),
                    "quantity": delta,
                    "value": round(trade_value, 2),
                    "reason": "Initial portfolio build" if initial_build else "Best available replacement",
                }
            )
            remaining_slots = max(0, remaining_slots - 1)

        # Mark to market
        holdings_value = 0.0
        for symbol in list(positions):
            mark = price_lookup.get((trade_date, symbol))
            if mark is not None:
                positions[symbol]["last_price"] = mark
            holdings_value += positions[symbol]["quantity"] * positions[symbol]["last_price"]
            latest_signal = day_indicators[day_indicators["symbol"].eq(symbol)]
            if not latest_signal.empty:
                positions[symbol]["score"] = latest_signal["score"].iloc[0]
                positions[symbol]["recommendation"] = latest_signal["recommendation"].iloc[0]

        total_nav = cash + holdings_value
        nav_base_100 = (total_nav / initial_capital) * 100 if initial_capital else 0
        nav_rows.append(
            {
                "date": trade_date.isoformat(),
                "cash": round(cash, 2),
                "holdings_value": round(holdings_value, 2),
                "total_nav": round(total_nav, 2),
                "nav_base_100": round(nav_base_100, 2),
                "positions": len(positions),
            }
        )

    positions_rows = [
        {
            "symbol": symbol,
            "industry": info["industry"],
            "quantity": info["quantity"],
            "last_price": round(info["last_price"], 2),
            "market_value": round(info["quantity"] * info["last_price"], 2),
            "entry_date": info["entry_date"],
            "score": round(info["score"], 2) if pd.notna(info["score"]) else None,
            "recommendation": info["recommendation"],
            "target_weight": round(info["target_weight"], 4),
        }
        for symbol, info in sorted(positions.items())
    ]

    return (
        pd.DataFrame(positions_rows),
        pd.DataFrame(trades),
        pd.DataFrame(nav_rows),
    )


def main():
    parser = argparse.ArgumentParser(description="Simulate a ranked NIFTY 500 portfolio using CSV data.")
    parser.add_argument("--capital", type=float, default=INITIAL_CAPITAL, help="Initial capital. Default: 10000000")
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prices, indicators = prepare_data()
    positions, trades, nav = simulate(prices, indicators, args.capital)

    positions.to_csv(DATA_DIR / "portfolio_positions.csv", index=False)
    trades.to_csv(DATA_DIR / "portfolio_trades.csv", index=False)
    nav.to_csv(DATA_DIR / "portfolio_nav.csv", index=False)
    latest_signals = indicators[indicators["trade_date"].eq(indicators["trade_date"].max())].copy()
    latest_signals.sort_values(["score", "symbol"], ascending=[False, True]).to_csv(
        DATA_DIR / "portfolio_signals.csv",
        index=False,
    )

    print(f"Wrote {len(positions)} current positions to {DATA_DIR / 'portfolio_positions.csv'}")
    print(f"Wrote {len(trades)} trades to {DATA_DIR / 'portfolio_trades.csv'}")
    print(f"Wrote {len(nav)} NAV rows to {DATA_DIR / 'portfolio_nav.csv'}")


if __name__ == "__main__":
    main()
