# slate-data-mirror

Public mirror of Chinese-market data, refreshed by a self-hosted GitHub
Actions runner inside mainland China. Consumed by [slate](https://github.com/azzon/slate)
deploys running behind corporate proxies that cannot reach the upstream
sources directly.

## Source strategy

| Data | Source | Why |
|---|---|---|
| `market_daily/` | **Tencent gtimg** (`qt.gtimg.cn` + `web.ifzq.gtimg.cn`) | Independent rate-limit from Eastmoney; 5500 A-shares × 5y pulls in ~3h without bans. Provides open/high/low/close/volume (no amount/turnover — slate doesn't read them). |
| everything else (financials, macro, concepts, research, lhb, yjyg, margin, north_flow, shareholders, securities, news) | **AKShare** | Tencent doesn't cover fundamentals, macro, or research. AKShare's non-hist endpoints are safely accessible at 1 req / 1.5-2s. |

AKShare's `stock_zh_a_hist` (the hist endpoint hosted on `push2.eastmoney.com`)
is **not used** — it banned our IP on the first 16-worker bulk run and
stayed banned for >24h. Tencent's OHLCV replaces it end-to-end.

## What's mirrored

| Group | AKShare source | Cadence | Layout |
|---|---|---|---|
| `securities/` | Tencent universe probe + AKShare fallback | 4h | `latest.parquet` |
| `market_daily/` | `tm.fetch_daily_bars` (Tencent) | 1×/day post-close + gap-heal | `YYYY/MM/YYYY-MM-DD.parquet` (append-only) |
| `financials/` | `stock_financial_abstract` per ticker | weekly (Sun 03:00 CST) | `latest.parquet` + `history/YYYY-MM-DD.parquet` |
| `macro/` | `macro_china_{pmi,cpi,money_supply,shibor}_*` | 4h | `{pmi,cpi,m2,shibor}.parquet` |
| `north_flow/` | `stock_hsgt_hold_stock_em` | 4h | `latest.parquet` + `history/` |
| `lhb/` | `stock_lhb_detail_em` (rolling 7d) | 4h | `latest.parquet` + `history/` |
| `shareholders/` | `stock_zh_a_gdhs` | weekly | `latest.parquet` + `history/` |
| `yjyg/` | `stock_yjyg_em` (last 4 quarters) | 4h | `latest.parquet` |
| `margin/` | `stock_margin_sse` + `stock_margin_szse` | 4h | `{sse,szse}_latest.parquet` |
| `concepts/` | `stock_board_concept_{name,cons}_em` | weekly | `boards.parquet` + `members.parquet` |
| `industries/` | `stock_board_industry_{name,cons}_em` | weekly | `boards.parquet` + `members.parquet` |
| `research/` | `stock_research_report_em` | 4h | `latest.parquet` + `history/` |
| `news/` | `news_cctv` + `stock_info_global_cls` | 4h | `{cctv,cls}/YYYY-MM-DD.parquet` |
| `_status.json` | per-endpoint freshness + last error | every pass | root of `data/` |

## Workflows

Three split workflows on the same self-hosted CN runner. All throttle
AKShare calls via `MIRROR_AKSHARE_SLEEP` (1.5-2s between calls).

1. **`mirror-fast.yml`** — 5 times/day (06/10/14/18/22 CST). Runs the
   8 lightweight endpoints. Each is 1-few API calls. ~2-5 minutes.
2. **`mirror-market.yml`** — daily at 16:30 CST (+ 17:30 safety re-run).
   Pulls that day's OHLCV for ~5500 tickers via Tencent. Auto-backfills
   any missing trade-date in the last 30 days.
3. **`mirror-slow.yml`** — weekly Sunday 03:00 CST. Runs financials +
   concepts + industries + shareholders. ~3 hours. Strictly serial
   to avoid AKShare rate-limits.
4. **`bootstrap.yml`** — manual only. 5-year history backfill for
   market_daily via Tencent. Resumable; per-chunk git push.

## Consumer side (slate)

slate's `src/slate/ingest/mirror_reader.py` clones/pulls this repo into
`~/.cache/slate/data-mirror`, reads the parquet, and upserts into DuckDB.
`slate nightly` calls it first; if `_status.json.last_success` is stale
(>48h for an endpoint) the consumer refuses to overwrite existing DuckDB
rows and `slate doctor` flags the drift.

## Freshness

`data/_status.json` per endpoint: `last_success`, `last_elapsed_s`,
`fail_streak`, `last_error`, `last_meta` (row counts, etc). Anyone
looking at the repo can tell at a glance whether a given data group is
live or stale.

Automated health check runs 3×/day on a GitHub-hosted runner
(`.github/workflows/healthcheck.yml`). When an endpoint's
`fail_streak >= 3` or staleness exceeds 2× its cadence, it opens (or
updates) a single `mirror-health` issue with the details.

## Operations

- **[RUNBOOK.md](RUNBOOK.md)** — symptom → fix index for common failures
  (runner offline, upstream flake, bootstrap resume, runner upgrade).
- **[CI](.github/workflows/ci.yml)** — pure-Python unit tests on
  GitHub-hosted runners, no network access, catches regressions in
  cadence / merge / health-check logic before cron jobs run them.

## License

Code: MIT. Data: relayed from upstream sources; use at your own risk,
subject to their terms.
