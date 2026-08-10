"""Live mmove_m15 signals for the intraday feed (imported by build_signals_json).

mmove_m15 (imbalance / fair-value-gap retrace continuation, m15, RR2) was promoted from
observer to the MAIN live intraday strategy on 2026-08-08 (user decision) — knowingly
ahead of the n>=40 forward gate (+0.81R at n=24), mitigated by running on a DEMO account.
Scoped to its validated pockets {xrpusd, xauusd, xagusd, fra40}, exempt from the crypto-
only LIVE_CLASSES gate. Self-contained + fail-open: any error here returns no rows and
never breaks the rest of signals.json.

PERSISTENCE (2026-08-10): the cBot flattens a position the instant its signal leaves
signals.json. A purely-transient emitter (emit only while the entry bar is "fresh") made
the signal vanish ~30 min after entry, so the cBot force-exited at market (reason=
manual-or-broker) long before the +2R target / -1R stop — the live record then measured a
time-exit, not the RR2 model. So we keep a state file (mmove-open.json) and RE-EMIT every
open signal each cycle until the feed shows its target or stop hit, exactly how
detect_triggers rides macdp to target-hit/stop-hit. A setup enters tracking only while
fresh (no late entries); it's dropped when resolved or after MAX_HOLD (safety bound).
Carry-forward rows are flagged held=True so build_signals_json exempts them from cooloff
(cooloff blocks NEW entries, it must not flatten an open position).
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr

_HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(_HERE, 'historical-ohlc.json')
STATE = os.path.join(_HERE, 'mmove-open.json')
POCKETS = ['xrpusd', 'xauusd', 'xagusd', 'fra40']
_CLS = {'xrpusd': 'crypto', 'xauusd': 'comm', 'xagusd': 'comm', 'fra40': 'index'}
IMP = 1.0; RETR = 24; BUF = 0.10; COOL = 3; RR = 2.0
FRESH_MIN = 30            # a setup may ENTER tracking only within 30 min of the data end
MAX_HOLD_MIN = 24 * 60   # stop tracking after 24h even if unresolved (safety bound)


def _mmove_signals(bars):
    """m15 FVG retrace-continuation (mirrors detect_mmove_m15 in the shadow harness)."""
    n = len(bars); out = []; last = -1
    for i in range(15, n - 2):
        if i <= last:
            continue
        a = atr(bars, 14, i) or 0.0
        if a <= 0:
            continue
        body = bars[i]['c'] - bars[i]['o']
        if body >= IMP * a and bars[i + 1]['l'] > bars[i - 1]['h']:            # bullish FVG
            g_bot = bars[i - 1]['h']; g_top = bars[i + 1]['l']
            for r in range(i + 2, min(i + 2 + RETR, n - 1)):
                b = bars[r]
                if b['l'] <= g_top and b['c'] > g_bot:
                    entry = b['c']; stop = g_bot - BUF * a
                    if stop < entry:
                        out.append((bars[r + 1]['_ts'], entry, stop, 'bull'))
                    last = r + COOL; break
                if b['c'] < g_bot:
                    break
        elif -body >= IMP * a and bars[i + 1]['h'] < bars[i - 1]['l']:         # bearish FVG
            g_top = bars[i - 1]['l']; g_bot = bars[i + 1]['h']
            for r in range(i + 2, min(i + 2 + RETR, n - 1)):
                b = bars[r]
                if b['h'] >= g_bot and b['c'] < g_top:
                    entry = b['c']; stop = g_top + BUF * a
                    if stop > entry:
                        out.append((bars[r + 1]['_ts'], entry, stop, 'bear'))
                    last = r + COOL; break
                if b['c'] > g_top:
                    break
    return out


def _load_pockets():
    """{pk: normalized m15 bars} for pockets with enough history."""
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
        # id MUST be pair:armedAt_ms:method so split(':')[2] == the strategy tag
        # (matches _signal_row + the cBot's StrategyFromSignalId + dashboard _stratOf).
        'id': pos['id'], 'pair': pos['pair'], 'sym': pos['pair'].upper(),
        'cls': _CLS.get(pos['pair']), 'method': 'mmove_m15', 'r_size': 1.0,
        'dir': pos['dir'], 'state': 'triggered',
        'entry': pos['entry'], 'stop': pos['stop'], 'target': pos['target'],
        'ew': None, 'tl': None, 'nw': None, 'cl': None,
        'armedAt': pos['armedAt'], 'triggeredAt': pos['armedAt'], 'lastSeenAt': now_ms,
        'source': 'server-detector-mmove', 'event_aligned': None, 'held': held,
    }


def build_mmove_rows(now_ms):
    """Rows for OPEN mmove_m15 setups (fresh entries + persisted holds). Fail-open."""
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
                    continue                # aged out — cBot manages its own SL/TP from here
                if _resolved(m15, armed_ts, pos['stop'], pos['target'], pos['dir']):
                    continue                # target/stop hit — cBot closed it; stop emitting
            elif now_ms / 1000.0 - armed_ts > MAX_HOLD_MIN * 60:
                continue                    # feed missing but wall-clock says expire
            new_open[sid] = pos
            rows.append(_row(pos, now_ms, held=True))

        # 2) Admit NEW setups whose entry bar is still fresh (prevents late entries).
        for pk, m15 in feeds.items():
            try:
                fresh_after = m15[-1]['_ts'] - FRESH_MIN * 60
                for (ets, entry, stop, d) in _mmove_signals(m15):
                    if ets < fresh_after:
                        continue
                    R = abs(entry - stop)
                    if R <= 0:
                        continue
                    armed_ms = int(ets * 1000)
                    sid = f"{pk}:{armed_ms}:mmove_m15"
                    if sid in new_open:
                        continue
                    target = round(entry + (RR * R if d == 'bull' else -RR * R), 8)
                    # A setup that already hit stop/target within its fresh window must NOT be
                    # (re-)admitted — otherwise a fast-resolving trade gets re-emitted and the
                    # cBot could re-enter a position that already closed.
                    if _resolved(m15, ets, round(stop, 8), target, d):
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
            pass                            # state write is best-effort; never fail the build
    except Exception:
        return rows
    return rows


if __name__ == '__main__':
    import time
    r = build_mmove_rows(int(time.time() * 1000))
    print(f"mmove_m15 open rows: {len(r)}")
    for x in r:
        print(x['id'], x['dir'], 'held' if x['held'] else 'new')
