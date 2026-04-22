# slate-data-mirror

Public mirror of Chinese-market data from AKShare, fetched by a
**self-hosted GitHub Actions runner** inside mainland China (the
upstream sources — Eastmoney, Sina, CNInfo, SSE/SZSE — geo-block
overseas IPs, so GitHub-hosted runners don't work).

Consumed by [slate](https://github.com/azzon/slate) deploys running
behind corporate proxies that cannot reach the upstream sources
directly.

## What's mirrored

| Group | AKShare source | Cadence | Layout |
|---|---|---|---|
| `securities/` | `stock_info_a_code_name` | 1×/day | `latest.parquet` |
| `market_daily/` | `stock_zh_a_spot_em` | up to 4×/day | `YYYY/YYYY-MM-DD.parquet` |
| `financials/` | `stock_financial_abstract` (per-ticker) | 1×/day | `latest.parquet` + `history/YYYY-MM-DD.parquet` |
| `macro/` | `macro_china_{pmi,cpi,money_supply,shibor}_*` | 1×/day | `{pmi,cpi,m2,shibor}.parquet` |
| `north_flow/` | `stock_hsgt_hold_stock_em` | 1×/day | `latest.parquet` + `history/` |
| `lhb/` | `stock_lhb_detail_em` (rolling 7d) | 1×/day | `latest.parquet` + `history/` |
| `shareholders/` | `stock_zh_a_gdhs` | weekly | `latest.parquet` + `history/` |
| `yjyg/` | `stock_yjyg_em` (last 4 quarters) | 1×/day | `latest.parquet` |
| `margin/` | `stock_margin_sse` + `stock_margin_szse` | 1×/day | `{sse,szse}_latest.parquet` |
| `concepts/` | `stock_board_concept_{name,cons}_em` | every 2d | `boards.parquet` + `members.parquet` |
| `industries/` | `stock_board_industry_{name,cons}_em` | every 2d | `boards.parquet` + `members.parquet` |
| `research/` | `stock_research_report_em` | 1×/day | `latest.parquet` + `history/` |
| `news/` | `news_cctv` + `stock_info_global_cls` | up to 4×/day | `{cctv,cls}/YYYY-MM-DD.parquet` |
| `_status.json` | per-endpoint freshness + last error | every pass | root of `data/` |

## How it runs

1. GitHub Actions cron fires 5×/day (`.github/workflows/mirror.yml`).
2. Workflow runs on the self-hosted runner (mainland IP).
3. Runner installs latest `akshare` from PyPI (AKShare ships
   anti-scrape fixes in patch releases — we always take the newest).
4. `scripts/fetch_all.py` iterates all endpoints, skipping any whose
   cadence isn't due, writes Parquet, updates `data/_status.json`.
5. Workflow commits `data/` diff and pushes via SSH remote
   (HTTPS to `github.com` is flaky from China ISPs).

Each endpoint is independent: one failure doesn't abort the pass.

## Manual ops

Re-run an endpoint immediately, from the Actions tab:

```
workflow_dispatch → only = "market_daily,north_flow"
```

Or force everything:

```
workflow_dispatch → force = true
```

## Freshness

`data/_status.json` carries for each endpoint: `last_success`,
`last_elapsed_s`, `fail_streak`, `last_error`. slate reads this and
caveats any answer that depends on stale data.

## License

Code: MIT. Data: relayed from upstream sources; use at your own
risk, subject to their terms.
