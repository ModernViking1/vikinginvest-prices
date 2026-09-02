"""Live fma_sweep_cm signals for the intraday feed (imported by build_signals_json).

fma_sweep_cm (liquidity-sweep + 50-EMA-reclaim reversal, m15 non-gold commodities) —
sweep a swing extreme that closes back inside, then a 50-EMA reclaim in the reversal
direction confirms; fade at fixed RR2. Its observer model was ALREADY scored at fixed RR2
(FMA_RR=2.0), so unlike holygrail its edge is not a trailing artifact — it survives
fixed-RR re-validation: full history n=554 +0.083R (both OOS halves +), forward +0.115R
(forward>full => not overfit), robust across RR1.5-2 and every hold horizon. Platinum
(xptusd) was a net drag (-0.086R), so this is SCOPED to the four carrying pockets
(silver/WTI/oil/natgas): n=426 +0.134R, OOS halves +0.127/+0.141.

Same stateful pattern as crt_live/sweepfvg_live/mmove_live/absorb_live: persist open
setups (fma-open.json), re-emit each until the feed shows its target or stop hit (so the
cBot rides to RR2/-1R, never flattened when the transient signal ages out), fresh entries
only, fail-open. NOT loaded by the dashboard. Executes only via the cBot's own gate
(commodity pockets, demo), which applies the house 0.5R-arm/0.25R-trail overlay like
every other intraday strategy.
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from fma_sweep_reversal_research import fma_signals

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')
STATE = os.path.join(_HERE, 'fma-open.json')
POCKETS = ['xagusd', 'usoil', 'wtiusd', 'natgas']   # non-gold comm, platinum dropped (net drag)
FRESH_MIN = 30            # a setup may ENTER tracking only within 30 min of the m15 data end
MAX_HOLD_MIN = 48 * 60    # ~2-day horizon, matching the observer's 192-bar bracket
RR = 2.0                  # fixed RR2 — the setting the observer validates at


def _load_pockets():
    """{pk: normalized m15 bars} for commodity pockets with enough history."""
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
        'cls': 'comm', 'method': 'fma_sweep_cm', 'r_size': 1.0,
        'dir': pos['dir'], 'state': 'triggered',
        'entry': pos['entry'], 'stop': pos['stop'], 'target': pos['target'],
        'ew': None, 'tl': None, 'nw': None, 'cl': None,
        'armedAt': pos['armedAt'], 'triggeredAt': pos['armedAt'], 'lastSeenAt': now_ms,
        'source': 'server-detector-fma', 'event_aligned': None, 'held': held,
    }


def build_fma_rows(now_ms):
    """Rows for OPEN fma_sweep_cm setups (fresh entries + persisted holds). Fail-open."""
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
                for (ei, entry, stop, d, _opp) in fma_signals(m15):
                    if ei >= len(m15) or m15[ei]['_ts'] < fresh_after:
                        continue
                    R = abs(entry - stop)
                    if R <= 0:
                        continue
                    armed_ms = int(m15[ei]['_ts'] * 1000)
                    sid = f"{pk}:{armed_ms}:fma_sweep_cm"
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
    r = build_fma_rows(int(time.time() * 1000))
    print(f"fma_sweep_cm open rows: {len(r)}")
    for x in r:
        print(x['id'], x['dir'], 'held' if x['held'] else 'new')
