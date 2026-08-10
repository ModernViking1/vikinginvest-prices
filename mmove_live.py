"""Fresh mmove_m15 signals for the LIVE intraday feed (imported by build_signals_json).

mmove_m15 (imbalance / fair-value-gap retrace continuation, m15, RR2) was promoted from
observer to the MAIN live intraday strategy on 2026-08-08 (user decision) — knowingly
ahead of the n>=40 forward gate (+0.81R at n=24), mitigated by running on a DEMO account.
Scoped to its validated pockets {xrpusd, xauusd, xagusd, fra40}, exempt from the crypto-
only LIVE_CLASSES gate. Self-contained + fail-open: any error here returns no rows and
never breaks the rest of signals.json.

Only setups whose entry bar is within FRESH_MIN of the data end are emitted, so the cBot
trades genuinely-new setups (deduped by id), not the whole backtest history.
"""
import json
import os

from backtest_rsi_per_class import _bars_norm
from five_strategies_research import atr

HIST = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'historical-ohlc.json')
POCKETS = ['xrpusd', 'xauusd', 'xagusd', 'fra40']
_CLS = {'xrpusd': 'crypto', 'xauusd': 'comm', 'xagusd': 'comm', 'fra40': 'index'}
IMP = 1.0; RETR = 24; BUF = 0.10; COOL = 3; RR = 2.0
FRESH_MIN = 30          # emit only setups whose entry bar is within the last 30 minutes


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


def build_mmove_rows(now_ms):
    """Return signals.json rows for fresh mmove_m15 setups (fail-open -> [])."""
    rows = []
    try:
        pairs = json.load(open(HIST)).get('pairs', {})
    except Exception:
        return rows
    for pk in POCKETS:
        try:
            m15 = _bars_norm(pairs.get(pk, {}).get('m15', []))
            if len(m15) < 400:
                continue
            fresh_after = m15[-1]['_ts'] - FRESH_MIN * 60
            for (ets, entry, stop, d) in _mmove_signals(m15):
                if ets < fresh_after:
                    continue
                R = abs(entry - stop)
                if R <= 0:
                    continue
                tgt = entry + RR * R if d == 'bull' else entry - RR * R
                # id MUST be pair:armedAt_ms:method so split(':')[2] == the strategy tag
                # (matches _signal_row + the cBot's StrategyFromSignalId + dashboard _stratOf).
                rows.append({
                    'id': f"{pk}:{int(ets * 1000)}:mmove_m15",
                    'pair': pk, 'sym': pk.upper(), 'cls': _CLS.get(pk), 'method': 'mmove_m15',
                    'r_size': 1.0, 'dir': d, 'state': 'triggered',
                    'entry': round(entry, 8), 'stop': round(stop, 8), 'target': round(tgt, 8),
                    'ew': None, 'tl': None, 'nw': None, 'cl': None,
                    'armedAt': int(ets * 1000), 'triggeredAt': int(ets * 1000), 'lastSeenAt': now_ms,
                    'source': 'server-detector-mmove', 'event_aligned': None,
                })
        except Exception:
            continue          # one bad pocket must not sink the others
    return rows
