#!/usr/bin/env python3
"""H7 adversarial winner-check.

For the five minor pairs in the consolidated-diagnosis screenshots,
replay macd-primary over the historical 15m window, bucket every
candidate signal by confluence, and simulate a 1:1 outcome. H7's
marginal effect vs the pre-H7 live rule (conf>=1) is to remove the
conf==1 bucket. So we report, per pair, wins vs losses in the conf==1
bucket (what H7 kills) alongside the conf 2-3 bucket (what it keeps).
"""
import json, sys
import detect_triggers as dt

PAIRS = ['cadjpy', 'usdsgd', 'euraud', 'nzdjpy', 'eursgd']
LOOKBACK_STRUCT = 8
EXPIRY_BARS = 64          # bars to fill the structural limit
RESOLVE_CAP  = 96         # bars to resolve once filled

def macd_dir_at(macd_line, sig_line, i):
    m0, m1 = macd_line[i-1], macd_line[i]
    s0, s1 = sig_line[i-1], sig_line[i]
    if None in (m0, m1, s0, s1):
        return None
    if m0 <= s0 and m1 > s1:  return 'bull'
    if m0 >= s0 and m1 < s1:  return 'bear'
    return None

def simulate(m15, i, direction, entry, stop, target):
    n = len(m15)
    filled = False
    for j in range(i+1, min(i+1+EXPIRY_BARS, n)):
        b = m15[j]
        if not filled:
            if (direction == 'bull' and b['l'] <= entry) or \
               (direction == 'bear' and b['h'] >= entry):
                filled = True
        if filled:
            for jj in range(j, min(j+RESOLVE_CAP, n)):
                bb = m15[jj]
                if direction == 'bull':
                    if bb['l'] <= stop:   return 'loss', jj
                    if bb['h'] >= target: return 'win', jj
                else:
                    if bb['h'] >= stop:   return 'loss', jj
                    if bb['l'] <= target: return 'win', jj
            return 'expired', jj
    return 'nofill', i

def run_pair(pair, hist):
    p = hist['pairs'].get(pair)
    if not p: return None
    m15 = p.get('m15', []); h1_all = p.get('h1', []); daily_all = p.get('daily', [])
    closes = [b['c'] for b in m15]
    if len(closes) < 60: return None
    macd_line, sig_line = dt.macd_series(closes, 12, 26, 9)

    # find crosses
    crosses = []
    for i in range(35, len(closes)):
        d = macd_dir_at(macd_line, sig_line, i)
        if d: crosses.append((i, d))

    buckets = {}          # conf -> {'win':n,'loss':n,'expired':n,'nofill':n}
    last_resolved = -1
    for i, macd_dir in crosses:
        if i <= last_resolved:      # non-overlapping, like the class backtests
            continue
        T = m15[i]['t']
        m15_slice = m15[:i+1]
        h1_asof = [b for b in h1_all if b['t'] <= T]
        d_asof  = [b for b in daily_all if b['t'] <= T]
        if len(d_asof) < 30 or len(h1_asof) < 12:
            continue
        h1_built = dt.build_h1_series(pair, m15_slice, {pair: {'h1': h1_asof}})
        nw = dt.calc_independent_dir(m15_slice, 5)
        tl = dt.calc_independent_dir(h1_built, 8)
        cl = dt.calc_4h_cloud_dir(h1_built)
        ew = dt.calc_independent_dir(d_asof, 8)
        try:
            auto = dt.auto_detect_ew(d_asof)
            ewp = auto.get('ew') if auto.get('ok') else None
            if ewp and ewp.get('dir') in ('bull','bear') \
               and ewp.get('confidence',0) >= dt.AUTO_EW_MIN_CONFIDENCE \
               and ewp.get('pattern') in dt.AUTO_EW_VALID_PATTERNS:
                ew = ewp['dir']
        except Exception:
            pass
        h1_closes = [b['c'] for b in h1_built if b.get('c') is not None]
        h1_rsi = dt.calc_rsi(h1_closes, 14) if len(h1_closes) >= 15 else None

        # ---- detector core (mirror detect_macd_primary pre-gate) ----
        if dt._htf_blocks(macd_dir, cl, enabled=dt.MACDP_HTF_FILTER):
            continue
        if h1_rsi is None: continue
        if macd_dir == 'bull' and h1_rsi >= 50: continue
        if macd_dir == 'bear' and h1_rsi <= 50: continue
        conf = sum(1 for lyr in (ew, tl, nw, cl) if lyr == macd_dir)

        cb = m15[i]
        sl = m15[max(0, i-LOOKBACK_STRUCT):i]
        if macd_dir == 'bull':
            entry = cb['l']; struct = min((b['l'] for b in sl), default=None)
            if struct is None or struct >= entry: continue
            stop = struct; r = entry - stop; target = entry + r
        else:
            entry = cb['h']; struct = max((b['h'] for b in sl), default=None)
            if struct is None or struct <= entry: continue
            stop = struct; r = stop - entry; target = entry - r
        if r <= 0: continue
        if dt._stop_too_tight(r, entry, 'minor'): continue

        outcome, jj = simulate(m15, i, macd_dir, entry, stop, target)
        last_resolved = jj
        b = buckets.setdefault(conf, {'win':0,'loss':0,'expired':0,'nofill':0})
        b[outcome] += 1
    return buckets

def wr(b):
    d = b['win'] + b['loss']
    return (100.0*b['win']/d) if d else None

def main():
    hist = json.load(open('historical-ohlc.json'))
    grand = {}
    print(f"{'pair':8} {'conf':>4} {'W':>3} {'L':>3} {'WR%':>6} {'exp':>3} {'nofill':>6}")
    print('-'*44)
    for pair in PAIRS:
        res = run_pair(pair, hist)
        if not res:
            print(f"{pair:8} (no data)"); continue
        for conf in sorted(res):
            b = res[conf]
            w = wr(b); ws = f"{w:5.1f}" if w is not None else "   - "
            print(f"{pair:8} {conf:>4} {b['win']:>3} {b['loss']:>3} {ws:>6} {b['expired']:>3} {b['nofill']:>6}")
            g = grand.setdefault(conf, {'win':0,'loss':0,'expired':0,'nofill':0})
            for k in b: g[k]+=b[k]
        print()
    print('='*44); print('AGGREGATE across the 5 pairs:')
    for conf in sorted(grand):
        b = grand[conf]; w = wr(b); ws = f"{w:5.1f}" if w is not None else "   - "
        print(f"{'ALL':8} {conf:>4} {b['win']:>3} {b['loss']:>3} {ws:>6} {b['expired']:>3} {b['nofill']:>6}")

if __name__ == '__main__':
    main()
