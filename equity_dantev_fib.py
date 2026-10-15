"""Denis/Dantev golden-zone fib on EQUITIES (3yr broker OHLC, per market).

Mirrors the FX study on the equity book: does the golden-zone pullback work on
single-stock/ETF bars, and does the TREND-ALIGNMENT gate (the parameter that
rescued it on FX) carry over? Two exits compared:
  RR2    — Deni's fixed 2R target (his style)
  trail  — the equity book's native trailing runner (score_trail_open), so the
           result is directly comparable to our live equity strategies.
Filters: base (unfiltered, = original dantev) vs trend (SMA50>100>200 align).

    python equity_dantev_fib.py --hist <merged broker ohlc>
"""
import json, argparse
from collections import defaultdict
import unified_shadow_harness as H
from backtest_rsi_per_class import _bars_norm
from five_strategies_research import agg4h, atr, cost
from dantev_fib_research import zigzag, walk_to, PRD, Z_HI, Z_LO, STOP_LVL, ATR_BUF, BREAK_WIN, COOLDOWN, HOLD
from dantev_winner_overlay import smas
from equity_broker_backtest import market


def scan(bars, tf):
    """Yield (ts, dir, entry, stop, R, trend_ok) for each golden-zone pullback."""
    piv = zigzag(bars, PRD); n = len(bars); last = -1
    sm = smas(bars); s50, s100, s200 = sm[50], sm[100], sm[200]
    for k in range(1, len(piv)):
        p0, p1 = piv[k-1], piv[k]
        if p0[2] == 'L' and p1[2] == 'H': d = 'bull'; L, H_ = p0[1], p1[1]
        elif p0[2] == 'H' and p1[2] == 'L': d = 'bear'; H_, L = p0[1], p1[1]
        else: continue
        rng = H_ - L
        if rng <= 0: continue
        if d == 'bull': zhi = H_-Z_HI*rng; zlo = H_-Z_LO*rng; void = H_-STOP_LVL*rng
        else: zlo = L+Z_HI*rng; zhi = L+Z_LO*rng; void = L+STOP_LVL*rng
        start = p1[0]+PRD+1; ei = None
        for j in range(start, min(start+BREAK_WIN, n-1)):
            b = bars[j]
            if d == 'bull':
                if b['l'] < void: break
                if b['l'] <= zhi and b['c'] > b['o'] and b['c'] > zlo: ei = j+1; break
            else:
                if b['h'] > void: break
                if b['h'] >= zlo and b['c'] < b['o'] and b['c'] < zhi: ei = j+1; break
        if ei is None or ei <= last or ei >= n: continue
        i = ei-1
        if s200[i] is None: continue
        trend_ok = (s50[i] > s100[i] > s200[i]) if d == 'bull' else (s50[i] < s100[i] < s200[i])
        entry = bars[ei]['o']; a = atr(bars, 14, ei-1) or 0.0
        stop = (void-ATR_BUF*a) if d == 'bull' else (void+ATR_BUF*a)
        if (d == 'bull' and stop >= entry) or (d == 'bear' and stop <= entry): continue
        yield (bars[ei]['_ts'], d, entry, stop, abs(entry-stop), trend_ok, ei)
        last = ei+COOLDOWN


def agg(seq):
    seq = sorted(seq); rs = [r for _, r in seq]; n = len(rs)
    if not n: return None
    m = n//2
    w = sum(1 for r in rs if r > 0)
    return dict(n=n, wr=100*w/n, exp=sum(rs)/n, tot=sum(rs),
                o1=sum(rs[:m])/m if m else 0, o2=sum(rs[m:])/(n-m) if n-m else 0)


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='viking_intl_ohlc.json')
    args = ap.parse_args()
    pairs = json.load(open(args.hist)).get('pairs', {})
    # store[(exit, filt, tf)] and store_mkt[mkt][(exit,filt,tf)]
    store = defaultdict(list); store_mkt = defaultdict(lambda: defaultdict(list)); nsym = 0
    for sym, layers in pairs.items():
        mkt = market(layers.get('sym') or sym)
        h1 = _bars_norm(layers.get('h1') or [])
        if len(h1) < 400: continue
        nsym += 1
        for tf, bars in {'h1': h1, '4h': agg4h(h1)}.items():
            if len(bars) < 250: continue
            for ts, d, entry, stop, R, trend_ok, ei in scan(bars, tf):
                tgt = entry + 2*R if d == 'bull' else entry - 2*R
                orr = walk_to(bars, ei, entry, stop, tgt, d, HOLD[tf])
                otr = H.score_trail_open(bars, ts, entry, stop, d, H.TRAIL_HOLD, H.TRAIL_ARM, H.TRAIL_DIST)
                for exit_name, o in (('RR2', orr), ('trail', otr[1] if otr[0] == 'resolved' else None)):
                    if o is None: continue
                    net = o - cost(o, entry, R)
                    for filt in (('base',) + (('trend',) if trend_ok else ())):
                        store[(exit_name, filt, tf)].append((ts, net))
                        store_mkt[mkt][(exit_name, filt, tf)].append((ts, net))

    def line(lbl, rows):
        a = agg(rows)
        if not a: print(f"  {lbl:<26} n=0"); return
        v = 'PASS' if (a['exp'] > 0 and a['o1'] > 0 and a['o2'] > 0 and a['n'] >= 40) else ('thin' if a['n'] < 40 else 'fail')
        print(f"  {lbl:<26} n={a['n']:>4} WR={a['wr']:>5.1f}% exp={a['exp']:>+7.3f}R tot={a['tot']:>+7.1f} OOS[{a['o1']:>+6.3f}/{a['o2']:>+6.3f}] {v}")

    print(f"Denis/Dantev golden-zone fib on EQUITIES — {nsym} symbols, 3yr, OOS-split\n")
    print("=== ALL EQUITY SYMBOLS ===")
    for ex in ('RR2', 'trail'):
        for tf in ('h1', '4h'):
            for fl in ('base', 'trend'):
                line(f"{ex} {tf} [{fl}]", store[(ex, fl, tf)])
        print()
    print("=== BY MARKET (RR2 & trail, trend-gated) ===")
    for mkt in sorted(store_mkt):
        print(f"--- {mkt} ---")
        for ex in ('RR2', 'trail'):
            for tf in ('h1', '4h'):
                line(f"{ex} {tf} [trend]", store_mkt[mkt][(ex, 'trend', tf)])
        print()


if __name__ == '__main__':
    main()
