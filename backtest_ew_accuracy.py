#!/usr/bin/env python3
"""Auto-EW accuracy vs trade outcome.

Question: does the Auto-EW macro directional call actually predict
whether a trade reaches its 1:1 target vs its stop? EW is one of the
four confluence layers; this isolates it. For every macd-primary signal
across FX (majors+minors) we tag the EW read and compare target-hit
rate (= WR at 1:1) when EW AGREES with the trade direction vs DISAGREES
vs is NEUTRAL. We test both the structural EW and the high-confidence
Auto-EW (the pivot-hierarchy detector shown on the diagnostic panel).
"""
import json
import detect_triggers as dt

LOOKBACK_STRUCT = 8
EXPIRY_BARS = 64
RESOLVE_CAP  = 96

def macd_dir_at(ml, sl, i):
    m0, m1, s0, s1 = ml[i-1], ml[i], sl[i-1], sl[i]
    if None in (m0, m1, s0, s1): return None
    if m0 <= s0 and m1 > s1: return 'bull'
    if m0 >= s0 and m1 < s1: return 'bear'
    return None

def simulate(m15, i, d, entry, stop, target):
    n = len(m15); filled = False
    for j in range(i+1, min(i+1+EXPIRY_BARS, n)):
        b = m15[j]
        if not filled and ((d=='bull' and b['l']<=entry) or (d=='bear' and b['h']>=entry)):
            filled = True
        if filled:
            for jj in range(j, min(j+RESOLVE_CAP, n)):
                bb = m15[jj]
                if d=='bull':
                    if bb['l']<=stop: return 'loss', jj
                    if bb['h']>=target: return 'win', jj
                else:
                    if bb['h']>=stop: return 'loss', jj
                    if bb['l']<=target: return 'win', jj
            return 'expired', jj
    return 'nofill', i

def run_pair(pair, hist, rows):
    p = hist['pairs'].get(pair)
    if not p: return
    m15, h1_all, daily_all = p.get('m15',[]), p.get('h1',[]), p.get('daily',[])
    closes = [b['c'] for b in m15]
    if len(closes) < 60: return
    ml, sl = dt.macd_series(closes, 12, 26, 9)
    last_resolved = -1
    for i in range(35, len(closes)):
        md = macd_dir_at(ml, sl, i)
        if not md or i <= last_resolved: continue
        T = m15[i]['t']; m15_slice = m15[:i+1]
        h1_asof = [b for b in h1_all if b['t'] <= T]
        d_asof  = [b for b in daily_all if b['t'] <= T]
        if len(d_asof) < 30 or len(h1_asof) < 12: continue
        h1_built = dt.build_h1_series(pair, m15_slice, {pair: {'h1': h1_asof}})
        nw = dt.calc_independent_dir(m15_slice, 5)
        tl = dt.calc_independent_dir(h1_built, 8)
        cl = dt.calc_4h_cloud_dir(h1_built)
        ew_struct = dt.calc_independent_dir(d_asof, 8)
        auto_dir, auto_conf, auto_carried = None, None, False
        try:
            a = dt.auto_detect_ew(d_asof)
            ewp = a.get('ew') if a.get('ok') else None
            if ewp and ewp.get('dir') in ('bull','bear'):
                auto_dir, auto_conf = ewp['dir'], ewp.get('confidence', 0)
                if auto_conf >= dt.AUTO_EW_MIN_CONFIDENCE and \
                   ewp.get('pattern') in dt.AUTO_EW_VALID_PATTERNS:
                    auto_carried = True
        except Exception:
            pass
        ew_blend = auto_dir if auto_carried else ew_struct
        if dt._htf_blocks(md, cl, enabled=dt.MACDP_HTF_FILTER): continue
        h1c = [b['c'] for b in h1_built if b.get('c') is not None]
        h1_rsi = dt.calc_rsi(h1c, 14) if len(h1c) >= 15 else None
        if h1_rsi is None: continue
        if md=='bull' and h1_rsi>=50: continue
        if md=='bear' and h1_rsi<=50: continue
        cb = m15[i]; slc = m15[max(0,i-LOOKBACK_STRUCT):i]
        if md=='bull':
            entry=cb['l']; st=min((b['l'] for b in slc), default=None)
            if st is None or st>=entry: continue
            stop=st; r=entry-stop; target=entry+r
        else:
            entry=cb['h']; st=max((b['h'] for b in slc), default=None)
            if st is None or st<=entry: continue
            stop=st; r=stop-entry; target=entry-r
        if r<=0 or dt._stop_too_tight(r, entry, dt.PAIR_CLASS.get(pair)): continue
        outcome, jj = simulate(m15, i, md, entry, stop, target)
        last_resolved = jj
        if outcome not in ('win','loss'): continue
        rows.append({
            'pair': pair, 'dir': md, 'outcome': outcome,
            'ew_blend': ew_blend, 'ew_struct': ew_struct,
            'auto_dir': auto_dir, 'auto_conf': auto_conf,
            'auto_carried': auto_carried,
        })

def rel(ew_dir, trade_dir):
    if ew_dir not in ('bull','bear'): return 'neutral'
    return 'agree' if ew_dir == trade_dir else 'disagree'

def tally(rows, keyfn):
    out = {}
    for r in rows:
        k = keyfn(r)
        if k is None: continue
        b = out.setdefault(k, [0,0])
        b[0 if r['outcome']=='win' else 1] += 1
    return out

def show(title, t, order):
    print(title)
    for k in order:
        if k not in t: continue
        w, l = t[k]; d = w+l
        wr = f"{100*w/d:5.1f}%" if d else "   - "
        print(f"   {k:9} {w:>3}W {l:>3}L  n={d:>3}  WR={wr}")
    print()

def main():
    hist = json.load(open('historical-ohlc.json'))
    fx = [p for p,c in dt.PAIR_CLASS.items() if c in ('major','minor')
          and p in hist['pairs']]
    rows = []
    for pair in fx:
        run_pair(pair, hist, rows)
    print(f"FX pairs analysed: {len(fx)} · decided signals: {len(rows)}\n")

    show("BLENDED EW (production layer) vs trade direction:",
         tally(rows, lambda r: rel(r['ew_blend'], r['dir'])),
         ['agree','disagree','neutral'])

    show("STRUCTURAL EW only (daily pivot dir) vs trade direction:",
         tally(rows, lambda r: rel(r['ew_struct'], r['dir'])),
         ['agree','disagree','neutral'])

    auto_rows = [r for r in rows if r['auto_carried']]
    print(f"High-confidence AUTO-EW carried the read on {len(auto_rows)} "
          f"of {len(rows)} signals.")
    show("AUTO-EW (high-conf, pivot-hierarchy) vs trade direction:",
         tally(auto_rows, lambda r: rel(r['auto_dir'], r['dir'])),
         ['agree','disagree'])

    # Confidence dose-response (all signals where auto-EW had a direction)
    def conf_band(r):
        c = r['auto_conf']
        if c is None: return None
        rl = rel(r['auto_dir'], r['dir'])
        if rl == 'neutral': return None
        band = '>=0.70' if c >= 0.70 else ('0.55-0.70' if c >= 0.55 else '<0.55')
        return f"{band}/{rl}"
    t = tally(rows, conf_band)
    show("AUTO-EW confidence x agreement (dose-response):", t,
         ['>=0.70/agree','>=0.70/disagree','0.55-0.70/agree',
          '0.55-0.70/disagree','<0.55/agree','<0.55/disagree'])

if __name__ == '__main__':
    main()
