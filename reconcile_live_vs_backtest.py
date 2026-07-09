"""Decisive reconciliation: replay the ACTUAL live-traded entries through the
historical m15 price path and compare the backtest OUTCOME model's verdict to
what actually happened live.

Why this settles the live-vs-backtest gap:
  - If backtest-replay WR on the SAME entries ≈ live WR (~41%), the outcome/fill
    model is NOT optimistic — the backtest and live agree trade-for-trade, so the
    ~82% headline backtest is simply measuring a DIFFERENT setup population than
    the cBot actually trades.
  - If backtest-replay WR >> live WR on the SAME entries, the gap is execution:
    slippage/spread/fills degrade winners the model assumes.

Method: each closed live execution carries its own entry_filled/stop/target/dir.
We locate the m15 bar at-or-after the fill timestamp in historical-ohlc.json and
walk forward (stop-before-target, same as the backtest) to a 1:1 resolution.

Run:  python reconcile_live_vs_backtest.py
"""
import json
from datetime import datetime, timezone

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
EXEC = '/home/user/vikinginvest-prices/executions.json'
WALK = 48  # m15 bars (~12h), same horizon as the exit/overlay backtests


def iso_to_ms(s):
    s = s.replace('Z', '+00:00')
    # trim sub-second precision beyond microseconds
    if '.' in s:
        head, rest = s.split('.', 1)
        frac = ''.join(c for c in rest if c.isdigit())[:6]
        tz = rest[len(''.join(c for c in rest if c.isdigit())):]
        s = f"{head}.{frac}{tz}"
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)


def replay(m15_ms, m15, fill_ms, entry, stop, target, direction):
    """Return +1/-1 for the backtest-model outcome, or None if unresolved / no path."""
    # first bar strictly after the fill
    import bisect
    i = bisect.bisect_right(m15_ms, fill_ms)
    if i >= len(m15):
        return None
    R = abs(entry - stop)
    if R <= 0:
        return None
    for j in range(i, min(i + WALK, len(m15))):
        b = m15[j]
        if direction == 'bull':
            if b['l'] <= stop:
                return -1.0            # stop-before-target (conservative)
            if b['h'] >= target:
                return 1.0
        else:
            if b['h'] >= stop:
                return -1.0
            if b['l'] <= target:
                return 1.0
    return None


def main():
    hist = json.load(open(HIST))['pairs']
    ex = json.load(open(EXEC))['executions']
    closed = [e for e in ex if e.get('event') == 'closed']

    # normalize each pair's m15 once
    cache = {}
    for pk, pd in hist.items():
        m15 = pd.get('m15', [])
        ms = [iso_to_ms(b['t']) for b in m15]
        cache[pk] = (ms, m15)

    matched = uncovered = 0
    agree = 0
    bt_win = live_win = 0
    quad = {'ww': 0, 'll': 0, 'wl': 0, 'lw': 0}  # backtest x live
    rows = []
    for e in closed:
        pk = e.get('pair')
        if pk not in cache:
            uncovered += 1
            continue
        entry = e.get('entry_filled'); stop = e.get('stop'); target = e.get('target')
        direction = e.get('dir'); ts = e.get('ts')
        if not all(isinstance(x, (int, float)) for x in (entry, stop, target, ts)) or direction not in ('bull', 'bear'):
            uncovered += 1
            continue
        ms, m15 = cache[pk]
        bt = replay(ms, m15, ts, entry, stop, target, direction)
        if bt is None:
            uncovered += 1
            continue
        live_r = e.get('realized_r') or 0
        live = 1.0 if live_r > 0 else (-1.0 if live_r < 0 else 0.0)
        if live == 0:
            uncovered += 1
            continue
        matched += 1
        bt_w = bt > 0; lv_w = live > 0
        if bt_w:
            bt_win += 1
        if lv_w:
            live_win += 1
        if bt_w == lv_w:
            agree += 1
        quad['ww' if bt_w and lv_w else 'll' if not bt_w and not lv_w else 'wl' if bt_w else 'lw'] += 1

    print(f"matched live→path trades : {matched}   (uncovered: {uncovered})")
    print(f"backtest-model WR on SAME entries : {100*bt_win/max(1,matched):.1f}%")
    print(f"live actual WR on same entries    : {100*live_win/max(1,matched):.1f}%")
    print(f"trade-for-trade agreement          : {100*agree/max(1,matched):.1f}%")
    print(f"  backtest WIN  & live WIN : {quad['ww']}")
    print(f"  backtest LOSS & live LOSS: {quad['ll']}")
    print(f"  backtest WIN  & live LOSS: {quad['wl']}   (would-be execution slippage)")
    print(f"  backtest LOSS & live WIN : {quad['lw']}")
    print(f"\nHeadline full-set backtest WR (audit_lookahead): ~82%")
    print("If backtest-model WR here ≈ live WR, the outcome model is accurate and")
    print("the 82% headline measures a different setup population than the cBot trades.")


if __name__ == '__main__':
    main()
