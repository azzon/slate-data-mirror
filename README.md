# slate-akshare-mirror

Mirror of AKShare data to a private GitHub repo, so slate deploys behind
corporate proxies (that can't reach `push2.eastmoney.com` / `finance.sina.com.cn`
/ `macro.sina.com.cn`) can pull pre-fetched Parquet instead of calling
AKShare directly.

**Upstream sources mirrored** (8 AKShare endpoints covering the 4 slate tables
fed by AKShare: `securities`, `market_daily`, `financials_quarterly`, `macro_daily`):

| Group | AKShare function | Output |
|---|---|---|
| securities | `stock_info_a_code_name` | `data/securities/latest.parquet` |
| market_daily | `stock_zh_a_spot_em` (today) / `stock_zh_a_hist` (backfill) | `data/market_daily/YYYY-MM-DD.parquet` |
| financials | `stock_financial_abstract` (per-ticker, ~5600) | `data/financials/latest.parquet` + dated snapshot |
| macro | `macro_china_{pmi,cpi,money_supply,shibor}_*` | `data/macro/{pmi,cpi,m2,shibor}.parquet` |

Daily `market_daily` snapshot is ~2 MB; full financials snapshot is ~50-80 MB.
Repo grows ~60 MB/month steady-state.

## Setup (on the China-side server)

```bash
git clone https://github.com/azzon/slate-akshare-mirror.git
cd slate-akshare-mirror
cp .env.example .env
# edit .env: paste GITHUB_TOKEN (fine-grained PAT, Contents: read+write, this repo only)

bash scripts/install.sh
```

`install.sh` is idempotent:
- creates `.venv` + installs `requirements.txt`
- renders `systemd/*.service`/`*.timer` with absolute paths
- installs them as user units at `~/.config/systemd/user/`
- enables + starts the timer (01:30 CST daily)

Verify:

```bash
systemctl --user list-timers akshare-mirror.timer
systemctl --user status akshare-mirror.service
journalctl --user -u akshare-mirror.service -n 100
```

## Manual / ad-hoc runs

```bash
.venv/bin/python scripts/fetch_and_push.py                          # standard daily pass
AKSHARE_MIRROR_SKIP_PUSH=1 .venv/bin/python scripts/fetch_and_push.py    # dry run
AKSHARE_MIRROR_DAYS_BACK=30 .venv/bin/python scripts/fetch_and_push.py   # backfill last 30 days
AKSHARE_MIRROR_SKIP_FINANCIALS=1 .venv/bin/python scripts/fetch_and_push.py  # skip the slow part
```

## Consuming on the slate side

Not wired yet — slate continues to hit AKShare directly (with Tencent as
primary for market_daily/securities). When the US-side proxy wall is
confirmed to block AKShare, add a loader that reads `data/*/latest.parquet`
and upserts into DuckDB before `slate nightly` runs. That's a slate-side
change, not this repo's concern.

## Token rotation

The PAT is only stored in `.env` on the China server (mode 0600, git-ignored).
If it leaks, revoke at https://github.com/settings/tokens and re-run
`install.sh` after updating `.env`.
