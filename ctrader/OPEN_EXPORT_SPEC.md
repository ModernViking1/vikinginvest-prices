# cBot live open-positions export — spec

**Goal:** publish an authoritative snapshot of the swing/intraday book's **open positions** (and,
for completeness, **pending orders**) so reconciliation is exact instead of inferred from the lossy
execution log. Mirrors the equity book's `equity-open.json`.

**Why:** `swing-executions.json` / `executions.json` are append-only rolling logs that drop old close
events and never log pending-order expiry. Reconstructing "what's open now" by set-differencing
placed-vs-closed yields stale phantoms (on 2026-11-04 it reported 8 open when the broker had 1 open +
4 pending). The signal feed already gives pending orders authoritatively; the missing piece is a live
**open-positions snapshot** straight from the cBot's `Positions` collection.

Transport reuses the existing `repository_dispatch` → ingest-workflow → commit pattern already used for
executions and the heartbeat. Snapshot semantics: each publish **replaces** the file (not append).

---

## 1. Output file — `swing-open.json` (and `intraday-open.json` for the intraday bridge)

Schema mirrors `equity-open.json`:

```json
{
  "generated": "2026-11-04T09:06:12.000Z",
  "generated_ms": 1793783172000,
  "source": "ctrader-swing-bridge",
  "account": 1234567,
  "account_mode": "demo",
  "balance_ccy": 321878.86,
  "equity_ccy": 321825.40,
  "count": 1,
  "pending_count": 4,
  "total_float_ccy": -53.46,
  "by_strategy": { "engulf_manip": {"n": 1, "float_ccy": -53.46} },
  "positions": [
    {"position_id": 712345678, "strategy": "engulf_manip", "symbol": "SOLUSD", "dir": "bear",
     "entry": 92.373, "price": 92.90, "sl": 95.10, "tp": 86.0, "volume_units": 100.0,
     "float_r": -0.24, "float_ccy": -53.46, "open_ms": 1793771745164}
  ],
  "pending": [
    {"strategy": "twob_ix", "symbol": "FRA40", "dir": "bull", "entry": 8120.0, "sl": 8071.0,
     "volume_units": 189.0, "placed_ms": 1793780000000}
  ]
}
```

Notes: `float_r` = signed `(price − entry)/R` using the position's tracked R (`_posR`, same source the
trailing runner uses; fall back to `|entry − tp|/RR` on a restart). `float_ccy` = `p.NetProfit`.
`price` = `Bid` for a long, `Ask` for a short (the close-side mark). Only this cBot's positions
(`p.Label == OrderLabel`). `pending` is optional (the signal feed already covers it) but cheap and makes
the file self-contained.

---

## 2. cBot change — `VikingSwingBridge.cs`

Add one method (mirrors `DispatchAsync`) and one call in `OnTimer`. No new secrets — reuses
`AutoPublishToRepo` + `GhPersonalAccessToken`. One new input for cadence.

**New input (with the other `[Parameter]`s):**
```csharp
[Parameter("Open-snapshot publish seconds", DefaultValue = 90)]
public int OpenPublishSeconds { get; set; }     // 0 = off
```
**New field (with the other private fields):**
```csharp
private DateTime _lastOpenPub = DateTime.MinValue;
```
**In `OnTimer()`, next to the heartbeat block:**
```csharp
if (AutoPublishToRepo && OpenPublishSeconds > 0 && !string.IsNullOrEmpty(GhPersonalAccessToken)
    && (DateTime.UtcNow - _lastOpenPub).TotalSeconds >= OpenPublishSeconds)
{
    _lastOpenPub = DateTime.UtcNow;
    try { _ = PublishOpenAsync(); } catch { }
}
```
**New method (snapshot `Positions` + `PendingOrders`, POST as `swing-cbot-open`):**
```csharp
private async Task PublishOpenAsync()
{
    long tsMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    var sb = new System.Text.StringBuilder(1024);
    sb.Append('{');
    F(sb, "generated_ms", tsMs); sb.Append(',');
    F(sb, "source", "ctrader-swing-bridge"); sb.Append(',');
    F(sb, "account", (long)Account.Number); sb.Append(',');
    F(sb, "account_mode", Account.IsLive ? "live" : "demo"); sb.Append(',');
    F(sb, "balance_ccy", Account.Balance); sb.Append(',');
    F(sb, "equity_ccy", Account.Equity); sb.Append(',');

    // positions
    double totFloat = 0; int n = 0;
    var ps = new System.Text.StringBuilder(); ps.Append('[');
    foreach (var p in Positions)
    {
        if (p.Label != OrderLabel || p.Symbol == null) continue;
        bool isBuy = p.TradeType == TradeType.Buy;
        double price = isBuy ? p.Symbol.Bid : p.Symbol.Ask;
        double R; if (!_posR.TryGetValue(p.Id, out R) || R <= 0)
            R = (p.TakeProfit.HasValue && p.TakeProfit.Value > 0) ? Math.Abs(p.EntryPrice - p.TakeProfit.Value) / 2.0 : 0;
        double floatR = (R > 0 && price > 0) ? ((isBuy ? price - p.EntryPrice : p.EntryPrice - price) / R) : 0;
        if (n > 0) ps.Append(',');
        ps.Append('{');
        F(ps, "position_id", p.Id); ps.Append(',');
        F(ps, "strategy", StrategyOf(p)); ps.Append(',');
        F(ps, "symbol", p.Symbol.Name); ps.Append(',');
        F(ps, "dir", isBuy ? "bull" : "bear"); ps.Append(',');
        F(ps, "entry", p.EntryPrice); ps.Append(',');
        F(ps, "price", price); ps.Append(',');
        F(ps, "sl", p.StopLoss ?? 0); ps.Append(',');
        F(ps, "tp", p.TakeProfit ?? 0); ps.Append(',');
        F(ps, "volume_units", p.VolumeInUnits); ps.Append(',');
        F(ps, "float_r", Math.Round(floatR, 3)); ps.Append(',');
        F(ps, "float_ccy", p.NetProfit); ps.Append(',');
        F(ps, "open_ms", new DateTimeOffset(p.EntryTime, TimeSpan.Zero).ToUnixTimeMilliseconds());
        ps.Append('}');
        totFloat += p.NetProfit; n++;
    }
    ps.Append(']');

    // pending orders (resting limits)
    int pn = 0; var po = new System.Text.StringBuilder(); po.Append('[');
    foreach (var o in PendingOrders)
    {
        if (o.Label != OrderLabel || o.SymbolName == null) continue;
        if (pn > 0) po.Append(',');
        po.Append('{');
        F(po, "strategy", (o.Comment != null && o.Comment.Contains(":")) ? o.Comment.Split(':')[0] : o.Label); po.Append(',');
        F(po, "symbol", o.SymbolName); po.Append(',');
        F(po, "dir", o.TradeType == TradeType.Buy ? "bull" : "bear"); po.Append(',');
        F(po, "entry", o.TargetPrice); po.Append(',');
        F(po, "sl", o.StopLoss ?? 0); po.Append(',');
        F(po, "volume_units", o.VolumeInUnits);
        po.Append('}');
        pn++;
    }
    po.Append(']');

    F(sb, "count", (long)n); sb.Append(',');
    F(sb, "pending_count", (long)pn); sb.Append(',');
    F(sb, "total_float_ccy", Math.Round(totFloat, 2)); sb.Append(',');
    sb.Append("\"positions\":").Append(ps).Append(',');
    sb.Append("\"pending\":").Append(po);
    sb.Append('}');
    var snapshot = sb.ToString();   // compact JSON object; a handful of positions => well under the dispatch limit

    try
    {
        var url = $"https://api.github.com/repos/{GhRepoOwner}/{GhRepoName}/dispatches";
        var body = "{\"event_type\":\"swing-cbot-open\",\"client_payload\":{\"snapshot\":" + snapshot + "}}";
        using var req = new HttpRequestMessage(HttpMethod.Post, url);
        req.Headers.Add("Accept", "application/vnd.github+json");
        req.Headers.Add("User-Agent", "VikingSwing-cTrader-Bot/1.0");
        req.Headers.Add("Authorization", $"Bearer {GhPersonalAccessToken}");
        req.Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");
        var resp = await _http.SendAsync(req);
        if ((int)resp.StatusCode == 204) HttpOk();
        else Print($"[VikingSwing] open-snapshot dispatch returned {(int)resp.StatusCode}");
    }
    catch (Exception ex) { HttpFail(); Print($"[VikingSwing] open-snapshot failed (non-fatal): {ex.Message}"); }
}
```

**Intraday bridge** (`VikingInvestSignalBridge.cs`): add the identical method with
`event_type: "cbot-open"` and `source: "ctrader-intraday-bridge"`; the workflow writes
`intraday-open.json`.

---

## 3. Ingest workflow — `.github/workflows/ingest-swing-open.yml`

Mirrors `ingest-swing-execution.yml`, but **overwrites** the file (snapshot) instead of appending.

```yaml
name: ingest-swing-open
on:
  repository_dispatch:
    types: [swing-cbot-open]
concurrency:
  group: data-publish          # share the group the other data-publish workflows use (avoid push races)
  cancel-in-progress: false
permissions:
  contents: write
jobs:
  write:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Write snapshot + generated timestamp
        env:
          SNAP: ${{ toJSON(github.event.client_payload.snapshot) }}
        run: |
          python3 - "$SNAP" <<'PY'
          import json, sys, datetime as dt
          snap = json.loads(sys.argv[1])
          snap["generated"] = dt.datetime.now(dt.timezone.utc).isoformat()
          json.dump(snap, open("swing-open.json","w"), separators=(",",":"))
          PY
      - name: Commit
        run: |
          git config user.name  "viking-data-bot"
          git config user.email "bot@users.noreply.github.com"
          git pull --rebase origin main || true
          git add swing-open.json
          git commit -m "data: swing open snapshot [skip ci]" || exit 0
          for i in 1 2 3 4; do git push && break || { git pull --rebase origin main; sleep $((2**i)); }; done
```

(`client_payload.snapshot` arrives as a JSON object; `toJSON` re-serialises it so Python can reload and
stamp `generated`. For the intraday bridge: copy to `ingest-intraday-open.yml`, type `cbot-open`, file
`intraday-open.json`.)

---

## 4. Consumer change — `book_state.py`

Prefer the live snapshot when it's fresh; fall back to the current best-effort heuristic otherwise.

```python
OPEN_SNAPSHOTS = ['swing-open.json', 'intraday-open.json']
SNAPSHOT_FRESH_MIN = 10   # a snapshot older than this is stale (cBot down) -> fall back to the heuristic

def open_positions_live(now_ms=None):
    """Authoritative open positions from the cBot snapshots, if fresh. Returns None if unavailable/stale."""
    import os
    if now_ms is None:
        now_ms = dt.datetime.now(dt.timezone.utc).timestamp() * 1000
    out, any_fresh = [], False
    for f in OPEN_SNAPSHOTS:
        try:
            d = json.load(open(f))
        except (FileNotFoundError, ValueError):
            continue
        if (now_ms - (d.get('generated_ms') or 0)) > SNAPSHOT_FRESH_MIN * 60000:
            continue                      # stale -> ignore this book's snapshot
        any_fresh = True
        for p in d.get('positions', []):
            out.append(dict(strategy=p['strategy'], symbol=p['symbol'], dir=p['dir'],
                            filled=_ts(p.get('open_ms')), entry=p.get('entry'),
                            float_ccy=p.get('float_ccy'), book=BOOK.get(p['strategy']), source='live'))
    return out if any_fresh else None

# in main(): opos = open_positions_live() or open_positions(rows, recency)   # live first, heuristic fallback
```

Once both bridges publish, `book_state.py` reports open positions **exactly** (with live float P&L), and
the lossy-log heuristic becomes a fallback only for when a cBot is down.

---

## Rollout
1. Add the method + call site to `VikingSwingBridge.cs`; recompile/redeploy the cBot in cTrader.
2. Add `ingest-swing-open.yml`; confirm `swing-open.json` appears and updates every ~90 s.
3. Repeat for the intraday bridge → `intraday-open.json`.
4. Add the consumer block to `book_state.py`.
5. Verify `book_state.py` matches the cTrader app (positions + float), then the heuristic is fallback-only.

Gating/safety: publishing is already gated on `AutoPublishToRepo` + a PAT and is best-effort (a failed
dispatch never disrupts trading). Snapshot is a single small object (a handful of positions) — well under
the `repository_dispatch` `client_payload` size limit. The file is a **replace-on-write** snapshot, so it
never grows and always reflects the latest poll.
```
