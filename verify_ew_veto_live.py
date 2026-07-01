#!/usr/bin/env python3
"""Verify the EW-disagreement veto is LIVE and effective.

Exercises the REAL production detect_macd_primary over the recent m15
window for FX (major+minor), toggling detect_triggers.EW_DISAGREE_VETO to
count how many signals the veto actually suppresses (counter-EW trades)
vs how many pass. Also confirms the absolute pip-floor has real targets in
the recent window. Read-only.
"""
import json, datetime
import detect_triggers as dt

WINDOW_DAYS = 14
LOOKBACK_STRUCT = 8

def macd_dir_at(ml, sl, i):
    m0, m1, s0, s1 = ml[i-1], ml[i], sl[i-1], sl[i]
    if None in (m0, m1, s0, s1): return None
    if m0 <= s0 and m1 > s1: return 'bull'
    if m0 >= s0 and m1 < s1: return 'bear'
    return None

def run_pair(pair, hist, cutoff_iso, acc):
    p = hist['pairs'].get(pair)
    if not p: return
    m15, h1_all, daily_all = p.get('m15',[]), p.get('h1',[]), p.get('daily',[])
    closes = [b['c'] for b in m15]
    if len(closes) < 60: return
    ml, sl = dt.macd_series(closes, 12, 26, 9)
    pc = dt.PAIR_CLASS.get(pair)
    for i in range(35, len(closes)):
        if m15[i]['t'] < cutoff_iso:      # recent window only
            continue
        md = macd_dir_at(ml, sl, i)
        if not md: continue
        T = m15[i]['t']; sliceM = m15[:i+1]
        h1_asof = [b for b in h1_all if b['t'] <= T]
        d_asof  = [b for b in daily_all if b['t'] <= T]
        if len(d_asof) < 30 or len(h1_asof) < 12: continue
        h1b = dt.build_h1_series(pair, sliceM, {pair: {'h1': h1_asof}})
        nw = dt.calc_independent_dir(sliceM, 5)
        tl = dt.calc_independent_dir(h1b, 8)
        cl = dt.calc_4h_cloud_dir(h1b)
        ew = dt.calc_independent_dir(d_asof, 8)
        try:
            a = dt.auto_detect_ew(d_asof); ewp = a.get('ew') if a.get('ok') else None
            if ewp and ewp.get('dir') in ('bull','bear') \
               and ewp.get('confidence',0) >= dt.AUTO_EW_MIN_CONFIDENCE \
               and ewp.get('pattern') in dt.AUTO_EW_VALID_PATTERNS:
                ew = ewp['dir']
        except Exception: pass
        h1c = [b['c'] for b in h1b if b.get('c') is not None]
        rsi = dt.calc_rsi(h1c, 14) if len(h1c) >= 15 else None

        # Real production detector — veto ON vs OFF
        dt.EW_DISAGREE_VETO = False
        res_off = dt.detect_macd_primary(sliceM, ew, tl, nw, cl, rsi, pc)
        dt.EW_DISAGREE_VETO = True
        res_on = dt.detect_macd_primary(sliceM, ew, tl, nw, cl, rsi, pc)

        if res_off is not None:
            acc['fired_off'] += 1
            if res_on is None:
                acc['vetoed'] += 1
                acc['examples'].append((T[:16], pair.upper(), md, 'ew='+str(ew)))
            else:
                acc['passed'] += 1

        # pip-floor: does a sub-floor structural stop exist here?
        slc = m15[max(0,i-LOOKBACK_STRUCT):i]
        if md == 'bull':
            entry=m15[i]['l']; st=min((b['l'] for b in slc), default=None)
            r = (entry-st) if (st is not None and st<entry) else None
        else:
            entry=m15[i]['h']; st=max((b['h'] for b in slc), default=None)
            r = (st-entry) if (st is not None and st>entry) else None
        if r and r > 0 and dt._stop_too_tight(r, entry, pc):
            acc['pipfloor_hits'] += 1

def main():
    hist = json.load(open('historical-ohlc.json'))
    # recent-window cutoff from the freshest m15 timestamp across FX
    fx = [p for p,c in dt.PAIR_CLASS.items() if c in ('major','minor') and p in hist['pairs']]
    maxts = ''
    for p in fx:
        m = hist['pairs'][p].get('m15') or []
        if m: maxts = max(maxts, m[-1]['t'])
    cut_dt = datetime.datetime.fromisoformat(maxts.replace('Z','+00:00').split('.')[0]+'+00:00') \
             - datetime.timedelta(days=WINDOW_DAYS)
    cutoff_iso = cut_dt.strftime('%Y-%m-%dT%H:%M:%S')
    print(f"Recent window: last {WINDOW_DAYS}d (m15 >= {cutoff_iso}), FX pairs: {len(fx)}")
    print(f"Flag as deployed: EW_DISAGREE_VETO = {dt.EW_DISAGREE_VETO}, MIN_STOP_PIPS_FX = {dt.MIN_STOP_PIPS_FX}\n")

    acc = {'fired_off':0,'passed':0,'vetoed':0,'pipfloor_hits':0,'examples':[]}
    for p in fx:
        run_pair(p, hist, cutoff_iso, acc)

    off = acc['fired_off']
    print("=== EW-VETO (real detect_macd_primary, veto toggled) ===")
    print(f"  FX macd-primary signals WITHOUT veto: {off}")
    print(f"  ...pass WITH veto (EW-aligned):        {acc['passed']}")
    print(f"  ...SUPPRESSED by veto (counter-EW):    {acc['vetoed']}"
          + (f"  ({100*acc['vetoed']/off:.0f}% of would-be signals)" if off else ""))
    if acc['examples']:
        print("  suppressed examples (time, pair, dir, ew):")
        for e in acc['examples'][:12]:
            print("    ", e)
    print("\n=== PIP-FLOOR (absolute FX stop floor) ===")
    print(f"  sub-floor FX structural stops caught in window: {acc['pipfloor_hits']}")
    verdict = "LIVE + EFFECTIVE" if acc['vetoed'] > 0 else \
              ("LIVE (no counter-EW triggers in window)" if off else "no FX triggers in window")
    print(f"\nVERDICT: EW-veto {verdict}.")

if __name__ == '__main__':
    main()
