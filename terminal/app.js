let rankings = [];
let historyBySymbol = {};
let metadata = {};
let portfolio = {};
let currentPage = 0;

const fmt = (value, digits = 2) => {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return Number(value).toLocaleString("en-IN", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
};

const ratingClass = rating => rating === "BUY" ? "buy" : rating === "SELL" ? "sell" : "hold";

function table(element, columns, rows) {
  const head = `<thead><tr>${columns.map(column => `<th>${column.label}</th>`).join("")}</tr></thead>`;
  const body = `<tbody>${rows.map(row => (
    `<tr>${columns.map(column => {
      const value = column.format ? column.format(row[column.key], row) : (row[column.key] ?? "-");
      const className = column.className ? column.className(row) : "";
      return `<td class="${className}">${value}</td>`;
    }).join("")}</tr>`
  )).join("")}</tbody>`;
  element.innerHTML = head + body;
}

function filteredRankings() {
  const industryValue = industry.value;
  const ratingValue = rating.value;
  const searchValue = search.value.trim().toUpperCase();
  let rows = rankings.slice();

  if (industryValue !== "ALL") rows = rows.filter(row => row.industry === industryValue);
  if (ratingValue !== "ALL") rows = rows.filter(row => row.recommendation === ratingValue);
  if (searchValue) rows = rows.filter(row => row.symbol.includes(searchValue));
  return rows;
}

function drawLineChart(canvas, seriesList) {
  const ctx = canvas.getContext("2d");
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#070707";
  ctx.fillRect(0, 0, width, height);

  const pad = { l: 54, r: 18, t: 18, b: 34 };
  const all = seriesList.flatMap(series => series.values.filter(point => (
    point.y !== null && point.y !== undefined && !Number.isNaN(point.y)
  )));
  if (!all.length) return;

  const minX = Math.min(...all.map(point => point.x));
  const maxX = Math.max(...all.map(point => point.x));
  let minY = Math.min(...all.map(point => point.y));
  let maxY = Math.max(...all.map(point => point.y));
  const yPad = (maxY - minY) * 0.08 || 1;
  minY -= yPad;
  maxY += yPad;

  const x = value => pad.l + ((value - minX) / Math.max(1, maxX - minX)) * (width - pad.l - pad.r);
  const y = value => height - pad.b - ((value - minY) / Math.max(1, maxY - minY)) * (height - pad.t - pad.b);

  ctx.strokeStyle = "#222";
  ctx.lineWidth = 1;
  ctx.fillStyle = "#888";
  ctx.font = "11px Consolas";
  for (let i = 0; i <= 5; i += 1) {
    const gridY = pad.t + i * (height - pad.t - pad.b) / 5;
    ctx.beginPath();
    ctx.moveTo(pad.l, gridY);
    ctx.lineTo(width - pad.r, gridY);
    ctx.stroke();
    const label = maxY - i * (maxY - minY) / 5;
    ctx.fillText(label.toFixed(2), 6, gridY + 4);
  }

  for (const series of seriesList) {
    ctx.strokeStyle = series.color;
    ctx.lineWidth = series.width || 2;
    ctx.beginPath();
    let started = false;
    for (const point of series.values) {
      if (point.y === null || point.y === undefined || Number.isNaN(point.y)) continue;
      const px = x(point.x);
      const py = y(point.y);
      if (!started) {
        ctx.moveTo(px, py);
        started = true;
      } else {
        ctx.lineTo(px, py);
      }
    }
    ctx.stroke();
  }

  let legendX = pad.l;
  for (const series of seriesList) {
    ctx.fillStyle = series.color;
    ctx.fillRect(legendX, height - 18, 10, 3);
    ctx.fillStyle = "#aaa";
    ctx.fillText(series.name, legendX + 14, height - 14);
    legendX += 90;
  }
}

function drawScoreBars(canvas, items) {
  const ctx = canvas.getContext("2d");
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#070707";
  ctx.fillRect(0, 0, width, height);

  const pad = { l: 126, r: 26, t: 20, b: 18 };
  const rowGap = (height - pad.t - pad.b) / items.length;
  const maxValue = Math.max(...items.map(item => item.max), 1);

  ctx.font = "11px Consolas";
  items.forEach((item, index) => {
    const y = pad.t + index * rowGap;
    const barHeight = Math.min(18, rowGap - 6);
    const value = Number(item.value || 0);
    const barWidth = (value / maxValue) * (width - pad.l - pad.r);
    ctx.fillStyle = "#888";
    ctx.fillText(item.label, 8, y + barHeight - 2);
    ctx.fillStyle = item.color;
    ctx.fillRect(pad.l, y, barWidth, barHeight);
    ctx.strokeStyle = "#2a2a2a";
    ctx.strokeRect(pad.l, y, width - pad.l - pad.r, barHeight);
    ctx.fillStyle = "#ddd";
    ctx.fillText(`${value.toFixed(2)} / ${item.max}`, pad.l + barWidth + 8, y + barHeight - 2);
  });
}

function rolling(rows, key, period, mode = "mean") {
  return rows.map((row, index) => {
    if (index + 1 < period) return null;
    const slice = rows.slice(index + 1 - period, index + 1).map(item => Number(item[key]));
    const mean = slice.reduce((sum, value) => sum + value, 0) / period;
    if (mode === "std") {
      const variance = slice.reduce((sum, value) => sum + Math.pow(value - mean, 2), 0) / (period - 1);
      return Math.sqrt(variance);
    }
    return mean;
  });
}

function updateMetrics(rows) {
  const counts = { BUY: 0, HOLD: 0, SELL: 0 };
  rows.forEach(row => { counts[row.recommendation] = (counts[row.recommendation] || 0) + 1; });
  const avgScore = rows.length ? rows.reduce((sum, row) => sum + Number(row.score), 0) / rows.length : null;
  const industryCount = new Set(rows.map(row => row.industry).filter(Boolean)).size;

  mUniverse.textContent = rows.length.toLocaleString("en-IN");
  mBuy.textContent = counts.BUY || 0;
  mHold.textContent = counts.HOLD || 0;
  mSell.textContent = counts.SELL || 0;
  mScore.textContent = fmt(avgScore);
  mIndustries.textContent = industryCount;
}

function renderRankings() {
  const rows = filteredRankings();
  const limit = Number(document.getElementById("rows").value);
  const maxPage = Math.max(0, Math.ceil(rows.length / limit) - 1);
  currentPage = Math.min(currentPage, maxPage);
  const start = currentPage * limit;
  const end = Math.min(start + limit, rows.length);
  updateMetrics(rows);
  pageInfo.textContent = rows.length ? `${start + 1}-${end}` : "0-0";
  prevPage.disabled = currentPage === 0;
  nextPage.disabled = currentPage >= maxPage;
  table(rankTable, [
    { key: "symbol", label: "SYMBOL" },
    { key: "industry", label: "INDUSTRY" },
    { key: "score", label: "SCORE" },
    { key: "recommendation", label: "RATING", className: row => ratingClass(row.recommendation) },
    { key: "trend_score", label: "TREND", format: value => fmt(value) },
    { key: "momentum_score", label: "MOM", format: value => fmt(value) },
    { key: "industry_relative_score", label: "IND REL", format: value => fmt(value) },
    { key: "volatility_score", label: "VOL ADJ", format: value => fmt(value) },
    { key: "confirmation_score", label: "CONF", format: value => fmt(value) },
    { key: "return_6m_ex_1m", label: "6M-1M %", format: value => fmt(value) },
    { key: "return_11m_ex_1m", label: "11M-1M %", format: value => fmt(value) },
    { key: "industry_rank_pct", label: "IND %", format: value => fmt(value) },
    { key: "peer_rank_pct", label: "PEER %", format: value => fmt(value) },
    { key: "risk_adjusted_momentum_rank_pct", label: "RAM %", format: value => fmt(value) },
  ], rows.slice(start, end));
}

function renderStock() {
  const symbolValue = symbol.value;
  const ranking = rankings.find(row => row.symbol === symbolValue);
  const history = historyBySymbol[symbolValue] || [];
  if (!ranking) return;

  stockTitle.textContent = `${symbolValue} | ${ranking.industry}`;
  sClose.textContent = fmt(ranking.trend_score);
  sScore.textContent = ranking.score;
  sRating.textContent = ranking.recommendation;
  sRating.className = `value ${ratingClass(ranking.recommendation)}`;
  sRsi.textContent = fmt(ranking.momentum_score);
  sReturn.textContent = fmt(ranking.industry_relative_score);
  sVol.textContent = fmt(ranking.volatility_score);

  const points = history.map(row => ({ x: new Date(row.trade_date).getTime(), y: Number(row.close) }));
  drawLineChart(priceChart, [{ name: "CLOSE", color: "#ffb000", width: 2.5, values: points }]);
  drawScoreBars(indicatorChart, [
    { label: "TREND", value: ranking.trend_score, max: 30, color: "#ffb000" },
    { label: "MOMENTUM", value: ranking.momentum_score, max: 30, color: "#00e676" },
    { label: "IND REL", value: ranking.industry_relative_score, max: 20, color: "#00bcd4" },
    { label: "VOL ADJ", value: ranking.volatility_score, max: 15, color: "#ffffff" },
    { label: "CONFIRM", value: ranking.confirmation_score, max: 5, color: "#ff5252" },
    { label: "IND %", value: ranking.industry_rank_pct, max: 100, color: "#c084fc" },
    { label: "PEER %", value: ranking.peer_rank_pct, max: 100, color: "#f97316" },
    { label: "RAM %", value: ranking.risk_adjusted_momentum_rank_pct, max: 100, color: "#22c55e" },
  ]);

  table(historyTable, [
    { key: "trade_date", label: "DATE" },
    { key: "close", label: "CLOSE", format: value => fmt(value) },
    { key: "open", label: "OPEN", format: value => fmt(value) },
    { key: "high", label: "HIGH", format: value => fmt(value) },
    { key: "low", label: "LOW", format: value => fmt(value) },
    { key: "volume", label: "VOLUME", format: value => value ? Number(value).toLocaleString("en-IN") : "-" },
  ], history.slice(-90).reverse());
}

function renderIndustry() {
  const industryMap = new Map();
  rankings.forEach(row => {
    const key = row.industry || "UNKNOWN";
    if (!industryMap.has(key)) {
      industryMap.set(key, { industry: key, stocks: 0, avg_score: 0, buy: 0, hold: 0, sell: 0 });
    }
    const item = industryMap.get(key);
    item.stocks += 1;
    item.avg_score += Number(row.score);
    item[row.recommendation.toLowerCase()] += 1;
  });

  const rows = Array.from(industryMap.values())
    .map(row => ({ ...row, avg_score: row.avg_score / row.stocks }))
    .sort((left, right) => right.avg_score - left.avg_score);

  table(industryTable, [
    { key: "industry", label: "INDUSTRY" },
    { key: "stocks", label: "STOCKS" },
    { key: "avg_score", label: "AVG SCORE", format: value => fmt(value) },
    { key: "buy", label: "BUY" },
    { key: "hold", label: "HOLD" },
    { key: "sell", label: "SELL" },
  ], rows);

  const top = rows.slice(0, 20).reverse();
  const maxScore = Math.max(...top.map(row => Math.abs(row.avg_score)), 1);
  const canvas = industryChart;
  const ctx = canvas.getContext("2d");
  const ratio = window.devicePixelRatio || 1;
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#070707";
  ctx.fillRect(0, 0, width, height);

  const pad = { l: 170, r: 20, t: 18, b: 18 };
  const barHeight = (height - pad.t - pad.b) / top.length * 0.65;
  ctx.font = "11px Consolas";
  top.forEach((row, index) => {
    const y = pad.t + index * (height - pad.t - pad.b) / top.length;
    const barWidth = Math.abs(row.avg_score) / maxScore * (width - pad.l - pad.r);
    ctx.fillStyle = "#aaa";
    ctx.fillText(row.industry.slice(0, 22), 8, y + barHeight);
    ctx.fillStyle = row.avg_score >= 0 ? "#ffb000" : "#ff5252";
    ctx.fillRect(pad.l, y, barWidth, barHeight);
    ctx.fillStyle = "#aaa";
    ctx.fillText(row.avg_score.toFixed(2), pad.l + barWidth + 6, y + barHeight);
  });
}

function renderDatabase() {
  table(dbTable, [
    { key: "table_name", label: "TABLE" },
    { key: "rows", label: "ROWS", format: value => Number(value).toLocaleString("en-IN") },
    { key: "symbols", label: "SYMBOLS" },
    { key: "start_date", label: "START" },
    { key: "end_date", label: "END" },
  ], metadata.tables || []);
}

function renderPortfolio() {
  const positions = portfolio.portfolio_positions || [];
  const trades = portfolio.portfolio_trades || [];
  const nav = portfolio.portfolio_nav || [];
  const latest = nav.length ? nav[nav.length - 1] : null;

  pNav100.textContent = latest ? fmt(latest.nav_base_100) : "-";
  pNav.textContent = latest ? fmt(latest.total_nav) : "-";
  pCash.textContent = latest ? fmt(latest.cash) : "-";
  pHoldings.textContent = latest ? fmt(latest.holdings_value) : "-";
  pPositions.textContent = latest ? latest.positions : positions.length;
  pTrades.textContent = trades.length;

  const navSeries = nav.map(row => ({
    x: new Date(row.date).getTime(),
    y: Number(row.nav_base_100),
  }));
  drawLineChart(navChart, [{ name: "NAV 100", color: "#ffb000", width: 2.5, values: navSeries }]);

  table(portfolioTable, [
    { key: "symbol", label: "SYMBOL" },
    { key: "industry", label: "INDUSTRY" },
    { key: "quantity", label: "QTY" },
    { key: "last_price", label: "LAST", format: value => fmt(value) },
    { key: "market_value", label: "MKT VAL", format: value => fmt(value) },
    { key: "score", label: "SCORE", format: value => fmt(value) },
    { key: "recommendation", label: "RATING", className: row => ratingClass(row.recommendation) },
    { key: "target_weight", label: "WT", format: value => `${fmt(Number(value) * 100)}%` },
  ], positions);

  table(tradesTable, [
    { key: "date", label: "DATE" },
    { key: "symbol", label: "SYMBOL" },
    { key: "action", label: "ACTION", className: row => row.action === "BUY" ? "buy" : "sell" },
    { key: "price", label: "PRICE", format: value => fmt(value) },
    { key: "quantity", label: "QTY" },
    { key: "value", label: "VALUE", format: value => fmt(value) },
    { key: "reason", label: "REASON" },
  ], trades.slice(-20).reverse());
}

async function init() {
  if (window.TERMINAL_DATA) {
    rankings = window.TERMINAL_DATA.rankings || [];
    historyBySymbol = window.TERMINAL_DATA.historyBySymbol || {};
    metadata = window.TERMINAL_DATA.metadata || {};
    portfolio = window.TERMINAL_DATA.portfolio || {};
  } else {
    throw new Error("Missing terminal/data/terminal_data.js");
  }

  ticker.textContent = `RANK DATE ${metadata.rank_date || "-"} | HISTORY ${metadata.history_start || "-"} TO ${metadata.history_end || "-"} | SOURCE CSV`;
  dbStatus.textContent = `${Number(metadata.history_rows || 0).toLocaleString("en-IN")} rows | ${metadata.history_symbols || 0} historical symbols`;

  const industries = ["ALL", ...Array.from(new Set(rankings.map(row => row.industry).filter(Boolean))).sort()];
  industry.innerHTML = industries.map(value => `<option>${value}</option>`).join("");
  symbol.innerHTML = rankings.map(row => `<option>${row.symbol}</option>`).join("");

  renderRankings();
  renderStock();
  renderIndustry();
  renderDatabase();
  renderPortfolio();
}

document.querySelectorAll(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab,.panel").forEach(element => element.classList.remove("active"));
    tab.classList.add("active");
    document.getElementById(tab.dataset.tab).classList.add("active");
    if (tab.dataset.tab === "portfolio") renderPortfolio();
    if (tab.dataset.tab === "stock") renderStock();
    if (tab.dataset.tab === "industryPanel") renderIndustry();
  });
});

["industry", "rating", "search", "rows"].forEach(id => {
  document.getElementById(id).addEventListener("input", () => {
    currentPage = 0;
    renderRankings();
  });
});

symbol.addEventListener("change", renderStock);
prevPage.addEventListener("click", () => {
  currentPage = Math.max(0, currentPage - 1);
  renderRankings();
});
nextPage.addEventListener("click", () => {
  currentPage += 1;
  renderRankings();
});
refresh.addEventListener("click", () => {
  currentPage = 0;
  renderRankings();
  renderStock();
  renderIndustry();
  renderDatabase();
  renderPortfolio();
});

init().catch(error => {
  document.body.innerHTML = `<main style="padding:20px;color:#ff5252;font-family:Consolas,monospace">
    <h1>Could not load terminal data</h1>
    <p>${error.message}</p>
    <p>Run <code>python export_terminal_data.py</code> first.</p>
  </main>`;
});

window.addEventListener("resize", () => {
  renderPortfolio();
  renderStock();
  renderIndustry();
});
