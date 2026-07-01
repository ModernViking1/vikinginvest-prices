#!/usr/bin/env python3
"""EW-disagreement veto vs H7 — head-to-head.

One replay pass over FX (majors+minors). For every macd-primary signal we
record confluence, outcome, and whether the EW macro read disagrees with
the trade (both the blended production read and the high-confidence
Auto-EW). We then score candidate entry policies on the MINOR subset —
where H7 lives — plus the veto applied across all FX.

Policies (minors):
  pre-H7      : confluence >= 1                 (the old live rule)
  H7          : confluence >= 2                 (current production)
  veto-blend  : confluence >= 1, drop blended-EW disagreements
  veto-auto   : confluence >= 1, drop high-conf Auto-EW disagreements
  veto+H7     : confluence >= 2 AND not blended-EW disagree

Metric at 1:1 R: win=+1R, loss=-1R. netR = W-L, EV = netR/n.
A good filter removes a sub-50% (net-negative) cohort while keeping
the positive one.
"""
import json
import detect_triggers as dt

LOOKBACK_STRUCT, EXPIRY_BARS, RESOLVE_CAP = 8, 64, 96

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
    pc = dt.PAIR_CLASS.get(pair)
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
        conf = sum(1 for lyr in (ew_blend, tl, nw, cl) if lyr == md)
        cb = m15[i]; slc = m15[max(0,i-LOOKBACK_STRUCT):i]
        if md=='bull':
            entry=cb['l']; st=min((b['l'] for b in slc), default=None)
            if st is None or st>=entry: continue
            stop=st; r=entry-stop; target=entry+r
        else:
            entry=cb['h']; st=max((b['h'] for b in slc), default=None)
            if st is None or st<=entry: continue
            stop=st; r=stop-entry; target=entry-r
        if r<=0 or dt._stop_too_tight(r, entry, pc): continue
        outcome, jj = simulate(m15, i, md, entry, stop, target)
        last_resolved = jj
        if outcome not in ('win','loss'): continue
        rows.append({
            'pair': pair, 'class': pc, 'dir': md, 'win': outcome=='win',
            'conf': conf,
            'blend_disagree': ew_blend in ('bull','bear') and ew_blend != md,
            'auto_disagree': auto_carried and auto_dir != md,
        })

def score(rows, keep):
    kept = [r for r in rows if keep(r)]
    w = sum(1 for r in kept if r['win']); l = len(kept)-w
    n = len(kept); netR = w-l
    wr = (100*w/n) if n else None
    ev = (netR/n) if n else None
    return n, w, l, wr, netR, ev

def line(label, s, base_n=None):
    n, w, l, wr, netR, ev = s
    wrs = f"{wr:5.1f}%" if wr is not None else "   -  "
    evs = f"{ev:+.3f}" if ev is not None else "  -  "
    dn  = f"  (Δn {n-base_n:+d})" if base_n is not None else ""
    print(f"  {label:16} n={n:>3}  {w:>3}W/{l:>3}L  WR={wrs}  netR={netR:>+4d}  EV={evs}R{dn}")

def removed(rows, base_keep, policy_keep, label):
    """Show the cohort a policy removes relative to a base policy."""
    rem = [r for r in rows if base_keep(r) and not policy_keep(r)]
    w = sum(1 for r in rem if r['win']); l = len(rem)-w; n=len(rem)
    wr = f"{100*w/n:5.1f}%" if n else "   -  "
    print(f"    {label} removes n={n} ({w}W/{l}L, WR={wr}) — good if <50%")

def main():
    hist = json.load(open('historical-ohlc.json'))
    fx = [p for p,c in dt.PAIR_CLASS.items() if c in ('major','minor')
          and p in hist['pairs']]
    rows = []
    for pair in fx: run_pair(pair, hist, rows)
    minors = [r for r in rows if r['class']=='minor']
    majors = [r for r in rows if r['class']=='major']
    print(f"FX signals: {len(rows)}  (minors {len(minors)}, majors {len(majors)})\n")

    # ---- MINORS: head-to-head (H7 lives here) ----
    print("="*72)
    print("MINORS — entry-policy head-to-head")
    print("="*72)
    preH7  = lambda r: r['conf']>=1
    H7     = lambda r: r['conf']>=2
    vblend = lambda r: r['conf']>=1 and not r['blend_disagree']
    vauto  = lambda r: r['conf']>=1 and not r['auto_disagree']
    vH7    = lambda r: r['conf']>=2 and not r['blend_disagree']
    b = score(minors, preH7)[0]
    line("pre-H7 (conf>=1)", score(minors, preH7))
    line("H7 (conf>=2)",     score(minors, H7),     b)
    line("veto-blend",       score(minors, vblend), b)
    line("veto-auto",        score(minors, vauto),  b)
    line("veto+H7",          score(minors, vH7),    b)
    print("\n  What each filter strips out of the pre-H7 set:")
    removed(minors, preH7, H7,     "H7        ")
    removed(minors, preH7, vblend, "veto-blend")
    removed(minors, preH7, vauto,  "veto-auto ")

    # ---- ALL FX: the veto isn't inherently minor-only ----
    print("\n"+"="*72)
    print("ALL FX — EW-disagreement veto (blended) as a standalone gate")
    print("="*72)
    allkeep = lambda r: True
    vall    = lambda r: not r['blend_disagree']
    b2 = score(rows, allkeep)[0]
    line("no veto (all)",  score(rows, allkeep))
    line("veto-blend",     score(rows, vall), b2)
    removed(rows, allkeep, vall, "veto-blend")

if __name__ == '__main__':
    main()
