# NIFTY 500 Static Terminal

This project is now a CSV-first NIFTY 500 pipeline built for GitHub Actions.

## Current Flow

1. `daily_update.py`
   - downloads NSE Bhavcopy for the day
   - filters NIFTY 500 symbols
   - appends that day into `data/historical_prices.csv`
   - confirms the append happened
   - only then continues

2. `calculate_indicators.py`
   - reads `data/historical_prices.csv`
   - calculates the daily technical snapshot
   - updates `data/technical_indicators.csv`
   - updates `data/latest_rankings.csv`

3. `simulate_portfolio.py`
   - reads the latest indicator data
   - monitors the existing portfolio
   - sells holdings only when they turn `SELL`
   - fills empty slots with the best available replacements
   - updates:
     - `data/portfolio_positions.csv`
     - `data/portfolio_trades.csv`
     - `data/portfolio_nav.csv`
     - `data/portfolio_signals.csv`

4. `export_terminal_data.py`
   - reads the CSV files
   - writes `docs/data/terminal_data.js`
   - the dashboard reads that file directly


## Main Files

- `daily_update.py`
- `calculate_indicators.py`
- `simulate_portfolio.py`
- `export_csv_data.py`
- `export_terminal_data.py`
- `data/historical_prices.csv`
- `data/technical_indicators.csv`
- `data/latest_rankings.csv`
- `data/portfolio_positions.csv`
- `data/portfolio_trades.csv`
- `data/portfolio_nav.csv`
- `data/portfolio_signals.csv`
- `docs/index.html`
- `docs/styles.css`
- `docs/app.js`


## Dashboard

The dashboard is now plain static HTML/CSS/JS.

To refresh the data bundle:

```powershell
python export_terminal_data.py
```

Then open:

- `docs/index.html`

No local database server is needed.


## GitHub Actions

Workflow file:

- `.github/workflows/daily-update.yml`

It runs on weekdays at:

- `18:00 IST`
- `20:00 IST`
- `22:00 IST`

That gives you:

- first attempt at 6 PM
- retry at 8 PM if Bhavcopy is not yet available
- retry at 10 PM if still needed

Important behavior:

- if the day is already present in `historical_prices.csv`, the later retry runs skip
- portfolio monitoring runs only after a confirmed append


## GitHub Pages

The published dashboard should be served from:

- `docs/`

That makes it compatible with GitHub Pages branch settings using:

- branch: `main`
- folder: `/docs`


## Technical Model

The ranking model uses:

- trend structure
- 6M ex-1M momentum
- 11M ex-1M momentum
- industry-relative strength
- volatility adjustment
- confirmation signals

Component scores:

- `trend_score` out of 30
- `momentum_score` out of 30
- `industry_relative_score` out of 20
- `volatility_score` out of 15
- `confirmation_score` out of 5

Final score:

```text
score =
trend_score
+ momentum_score
+ industry_relative_score
+ volatility_score
+ confirmation_score
```

Recommendation:

- `BUY` for strong names
- `HOLD` for middling names
- `SELL` for weak names or hard-risk failures


## Portfolio Logic

The portfolio is not rebuilt from scratch every day.

Instead:

- build the initial portfolio once
- monitor existing holdings each indicator date
- sell only when a holding becomes `SELL`
- buy replacements from the best available universe names

Current portfolio rules:

- capital: `Rs. 1,00,00,000`
- holdings: `22` to `25`
- max weight per stock: `5%`
- sector cap used in selection


## Local Manual Run

To run the full update manually:

```powershell
python daily_update.py
```

Or use:

```powershell
.\run_update.ps1
```
