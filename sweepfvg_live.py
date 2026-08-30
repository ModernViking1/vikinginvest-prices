"""Live sweepfvg_ix signals for the intraday feed (imported by build_signals_json).

sweepfvg_ix (liquidity-sweep + FVG + fib-retrace reversal, m15 indices) was PROMOTED
2026-08-24. Its shadow model used a far RR3/session exit and read -0.116R forward — but
at a FIXED RR2 (what the cBot executes) the edge is robust: forward n=188 +0.165R, full
history +0.165R (identical → not overfit), BOTH OOS halves + (+0.149/+0.181). The trailing/
RR3 exit was destroying a real edge; RR2 is where it lives, so this emits at RR2.

Same stateful pattern as mmove_live/absorb_live: persist open setups (sweepfvg-open.json),
re-emit each until the feed shows its target or stop hit (so the cBot rides to RR2/-1R,
never flattened when the transient signal ages out), fresh entries only, fail-open.
NOT loaded by the dashboard. Never trades on a live account except via the cBot's own gate.
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from liquidity_sweep_fvg_research import variant_B as _sweepfvg_signals

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')
STATE = os.path.join(_HERE, 'sweepfvg-open.json')
POCKETS = ['de40', 'dj30', 'fra40', 'ftse100', 'jp225', 'nas100', 'spx500']   # index pockets
FRESH_MIN = 30            # a setup may ENTER tracking only within 30 min of the m15 data end
MAX_HOLD_MIN = 24 * 60    # stop tracking after 24h even if unresolved (safety bound)
RR = 2.0                  # emit at the fixed RR2 the edge validates at (NOT the native RR3)


def _load_pockets():
    """{pk: normalized m15 bars} for index pockets with enough history."""
    out = {}
    try:
        pairs = json.load(open(HIST)).get('pairs', {})
    except Exception:
        return out
    for pk in POCKETS:
        try:
            m15 = _bars_norm(pairs.get(pk, {}).get('m15', []))
            if len(m15) >= 400:
                out[pk] = m15
        except Exception:
            continue
    return out


def _load_state():
    try:
        return json.load(open(STATE)).get('open', {})
    except Exception:
        return {}


def _resolved(m15, armed_ts, stop, target, d):
    """True if any bar AFTER the entry bar hit the stop or the target (either => closed)."""
    for b in m15:
        if b['_ts'] <= armed_ts:
            continue
        if d == 'bull':
            if b['l'] <= stop or b['h'] >= target:
                return True
        else:
            if b['h'] >= stop or b['l'] <= target:
                return True
    return False


def _row(pos, now_ms, held):
    return {
        # id MUST be pair:armedAt_ms:method so split(':')[2] == the strategy tag.
        'id': pos['id'], 'pair': pos['pair'], 'sym': pos['pair'].upper(),
        'cls': 'index', 'method': 'sweepfvg_ix', 'r_size': 1.0,
        'dir': pos['dir'], 'state': 'triggered',
        'entry': pos['entry'], 'stop': pos['stop'], 'target': pos['target'],
        'ew': None, 'tl': None, 'nw': None, 'cl': None,
        'armedAt': pos['armedAt'], 'triggeredAt': pos['armedAt'], 'lastSeenAt': now_ms,
        'source': 'server-detector-sweepfvg', 'event_aligned': None, 'held': held,
    }


def build_sweepfvg_rows(now_ms):
    """Rows for OPEN sweepfvg_ix setups (fresh entries + persisted holds). Fail-open."""
    rows = []
    try:
        feeds = _load_pockets()
        if not feeds:
            return rows
        open_state = _load_state()          # {id: {pair, dir, entry, stop, target, armedAt}}
        new_open = {}

        # 1) Carry forward still-open positions; drop resolved/expired.
        for sid, pos in open_state.items():
            armed_ts = pos['armedAt'] / 1000.0
            m15 = feeds.get(pos['pair'])
            if m15 is not None:
                if m15[-1]['_ts'] - armed_ts > MAX_HOLD_MIN * 60:
                    continue
                if _resolved(m15, armed_ts, pos['stop'], pos['target'], pos['dir']):
                    continue
            elif now_ms / 1000.0 - armed_ts > MAX_HOLD_MIN * 60:
                continue
            new_open[sid] = pos
            rows.append(_row(pos, now_ms, held=True))

        # 2) Admit NEW setups whose entry bar is still fresh (prevents late entries).
        for pk, m15 in feeds.items():
            try:
                fresh_after = m15[-1]['_ts'] - FRESH_MIN * 60
                for (ei, entry, stop, d) in _sweepfvg_signals(m15):
                    if ei >= len(m15) or m15[ei]['_ts'] < fresh_after:
                        continue
                    R = abs(entry - stop)
                    if R <= 0:
                        continue
                    armed_ms = int(m15[ei]['_ts'] * 1000)
                    sid = f"{pk}:{armed_ms}:sweepfvg_ix"
                    if sid in new_open:
                        continue
                    target = round(entry + (RR * R if d == 'bull' else -RR * R), 8)
                    if _resolved(m15, m15[ei]['_ts'], round(stop, 8), target, d):
                        continue
                    pos = {'id': sid, 'pair': pk, 'dir': d,
                           'entry': round(entry, 8), 'stop': round(stop, 8),
                           'target': target, 'armedAt': armed_ms}
                    new_open[sid] = pos
                    rows.append(_row(pos, now_ms, held=False))
            except Exception:
                continue          # one bad pocket must not sink the others

        try:
            json.dump({'open': new_open}, open(STATE, 'w'))
        except Exception:
            pass
    except Exception:
        return rows
    return rows


if __name__ == '__main__':
    import time
    r = build_sweepfvg_rows(int(time.time() * 1000))
    print(f"sweepfvg_ix open rows: {len(r)}")
    for x in r:
        print(x['id'], x['dir'], 'held' if x['held'] else 'new')
