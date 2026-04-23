# Operator runbook

Quick-reference recovery steps for common failures. Pair with the
`healthcheck.yml` workflow that opens a GitHub issue when it sees
trouble.

## Symptom → Fix index

| What you see | Likely cause | Go to |
|---|---|---|
| `healthcheck.yml` opens issue "mirror-health: endpoints unhealthy" | one or more endpoints stale / failing | §1 Triage |
| `slate doctor` shows mirror `[red]failures` | consumer sees the producer-side failure | §1 Triage |
| Runner listed as `offline` on GitHub settings page | SSH or runner service crashed | §2 Runner offline |
| Jobs stuck `queued` for >15 min, runner online | GitHub→runner dispatch stuck | §3 Runner stuck |
| `market_daily` or `news` `fail_streak>=3` | Tencent/Eastmoney flake | §4 Upstream flake |
| Bootstrap cancelled at <100% | runner auto-update, timeout, or OOM | §5 Bootstrap resume |
| Workflow fails with "Runner version … deprecated" | GitHub deprecated our runner version | §6 Runner upgrade |
| `mirror-slow.yml` hasn't run in >2 weeks | daily retry cron stuck | §7 Slow workflow |
| `securities` drop guard tripped on nightly | upstream returned partial universe | §8 Universe drop |
| permafail file growing unbounded | too many trading-day failures accumulated | §9 Permafail |

---

## 1. Triage

First pull the mirror repo to see authoritative status:

```bash
git clone --depth 1 https://github.com/azzon/slate-data-mirror.git /tmp/mirror
python3 /tmp/mirror/scripts/healthcheck.py
```

Output will tell you which endpoints are unhealthy. Then:

- **Fast endpoint stale (news/lhb/yjyg/margin)**: go to §4.
- **Slow endpoint stale (financials/concepts/etc.)**: go to §7.
- **Every endpoint stale**: go to §2 or §3 (runner is the problem).

## 2. Runner offline

```bash
ssh aliyun-runner                                    # via HTTP CONNECT proxy or direct
systemctl status actions.runner.azzon-slate-data-mirror.aliyun-cn-runner.service
# If inactive:
systemctl start actions.runner.azzon-slate-data-mirror.aliyun-cn-runner.service
# If active but GitHub sees offline: usually a network glitch, wait 2min
# If that doesn't help, restart:
systemctl restart actions.runner.azzon-slate-data-mirror.aliyun-cn-runner.service
```

Confirm via `journalctl -u actions.runner.... -n 20` — should see
`Connected to GitHub` + `Listening for Jobs`.

If SSH itself fails: the runner OOM can take down sshd. Log in via
Alibaba Cloud ECS web console and re-run `scripts/harden_runner.sh`.

## 3. Runner stuck (jobs queued, runner "online")

Usually a stale WebSocket on GitHub's side:

```bash
ssh aliyun-runner 'systemctl restart actions.runner.azzon-slate-data-mirror.aliyun-cn-runner.service'
```

Wait 30s. Queued job should pick up.

## 4. Upstream flake (Tencent / Eastmoney)

For `market_daily`: the fetch gates on 0.5× rolling median — if upstream
returned thin data, we logged "rejected … retry next run" and didn't
write. The next 4h cron will retry automatically.

For `news`: 4h cadence; 1-2 missed runs is fine.

For persistent fail (fail_streak >= 3):
```bash
# Force a re-run from the Actions UI:
gh workflow run mirror-fast.yml -R azzon/slate-data-mirror -f force=true
```

If Eastmoney has IP-banned the runner (see `project_akshare_rate_limit.md`
in slate memory), **the ban clears in 30-60 min automatically**. Don't
retry during the ban — it extends the cooldown. Wait, then force-run.

## 5. Bootstrap resume

Bootstrap is designed to resume. After any cancel/crash:

```bash
gh workflow run bootstrap.yml -R azzon/slate-data-mirror \
    -f years=5 -f reset=false -f purge_existing=false -f workers=4
```

`reset=false` preserves `data/_bootstrap_progress.json`, so only
outstanding tickers get fetched. Previously-failed tickers are
automatically retried (merged from `failed_codes` map).

**Do not** use `reset=true` + `purge_existing=true` unless the whole
market_daily tree is corrupt — that wipes 5y of data and re-starts.

If the 6h workflow timeout is close, reduce scope:
```bash
gh workflow run bootstrap.yml -R azzon/slate-data-mirror -f years=2
```
…then re-extend years once the shorter window lands.

## 6. Runner upgrade

GitHub deprecates runner versions every ~3 months. Symptom in journal:

```
Runner version 'v2.333.1' is deprecated and cannot receive messages.
```

Fix: on the runner host, run
```bash
sudo RUNNER_VERSION=2.334.0 /home/runner/actions-runner/../scripts/upgrade_runner.sh 2.334.0
```

(Or `curl -fsSL https://raw.githubusercontent.com/azzon/slate-data-mirror/main/scripts/upgrade_runner.sh | sudo bash -s 2.334.0`.)

Script downloads via `ghfast.top` CN mirror to dodge the CN→GitHub
Release CDN throttle (see `project_github_runner_cn.md`).

Runner was registered with `--disableupdate` so GitHub won't auto-push
updates mid-job; but we still have to **manually** bump every
deprecation cycle.

## 7. Slow workflow

Primary cron: Sunday 03:00 CST. Retry: Mon-Sat 02:30 CST if last
`financials.last_success` > 6 days old.

If it has been >2 weeks:
- Check `healthcheck.py` output.
- Check runner journalctl for `fetch-slow` job:
  ```
  journalctl -u actions.runner.azzon-slate-data-mirror.aliyun-cn-runner.service \
      --since "7 days ago" | grep fetch-slow
  ```
- Force a run:
  ```bash
  gh workflow run mirror-slow.yml -R azzon/slate-data-mirror -f force=true
  ```

Typical runtime: 3 hours. If it completes in <30 min, AKShare is
either banning us OR the endpoints silently failed — check `_status.json`.

## 8. Universe drop

The mirror's `fetch_securities` refuses to write when both AKShare and
Tencent return <4500 rows. On the consumer side, `mirror_reader.ingest_securities`
has a second check that refuses ingest if the drop vs DuckDB > 20% or
absolute count < 3000.

If a legitimate universe change (mass delisting, BSE re-class) needs
to land:

```bash
# On the deploy host:
slate ingest-mirror --max-drop-pct 0.50    # allow up to 50% drop
```

Document the event in CLAUDE.md if non-trivial.

## 9. Permafail

`data/_market_daily_permafail.json` records trading days that returned
zero rows (usually old holidays Tencent's qfqday window has dropped).
If you want to force a re-try of some date:

```bash
ssh aliyun-runner
cd /home/runner/actions-runner/_work/slate-data-mirror/slate-data-mirror
python -c "
import json, pathlib
p = pathlib.Path('data/_market_daily_permafail.json')
dates = set(json.loads(p.read_text()))
dates.discard('2024-05-01')   # ← the date you want back in play
p.write_text(json.dumps(sorted(dates)))
"
```

Next cron run's gap scan will re-attempt it. If it still returns no
data, permafail re-records it.

---

## Escalation contacts

- **slate consumer side (DuckDB upsert, doctor, nightly)** — slate repo owner
- **Runner host (Alibaba ECS, sshd, systemd)** — infra owner with SSH root
- **GitHub repo (secrets, workflows)** — `azzon` GitHub account

## Expected healthy output ("green")

- `healthcheck.py` prints `OK: all 9/9 endpoints healthy` (or 13/13 once slow has run at least once)
- `data/_status.json` updates within the last 4h for fast endpoints, 24h for market_daily, 8 days for slow endpoints
- `data/market_daily/YYYY-MM-DD.parquet` exists for every trading day in the last 30 days
- Repo size grows ~2 MB/day on weekdays, flat on weekends
