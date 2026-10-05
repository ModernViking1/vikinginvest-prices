"""Win-rate / RR from the equity demo — reads equity-executions.json (written by equity_bridge.py
from MT5's closed deals) and prints realised WR / expectancy (R) per strategy, per market, per
symbol. R-multiples, so size doesn't matter. Upload the file and run:

  python equity_wr.py --hist equity-executions.json
"""
import argparse, json
from collections import defaultdict


def _mkt(sym):
    s = (sym or '').upper()
    if '.ETR' in s: return 'DE'
    if '.LSE' in s: return 'UK'
    if '.NAS' in s or '.NYSE' in s or '.US' in s: return 'US'
    return '?'


def _agg(rs):
    rs = [r for r in rs if r is not None]
    n = len(rs)
    if not n:
        return None
    return dict(n=n, wr=100.0 * sum(1 for r in rs if r > 0) / n, exp=sum(rs) / n, tot=sum(rs))


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--hist', default='equity-executions.json')
    a = ap.parse_args()
    d = json.load(open(a.hist))
    ex = d.get('executions', [])
    closed = [e for e in ex if e.get('realized_r') is not None]
    print(f"=== equity demo WR/RR · {d.get('generated','')} · {len(closed)} resolved / {len(ex)} total ===\n")
    if not closed:
        print("No resolved trades with an R-multiple yet — let the demo run."); return
    by_s = defaultdict(list); by_m = defaultdict(list); by_sym = defaultdict(list); pnl = 0.0
    for e in closed:
        by_s[e['strategy']].append(e['realized_r'])
        by_m[_mkt(e['sym'])].append(e['realized_r'])
        by_sym[e['sym']].append(e['realized_r'])
        pnl += e.get('profit_ccy', 0) or 0
    allr = [e['realized_r'] for e in closed]; A = _agg(allr)
    print(f"OVERALL  n={A['n']}  WR={A['wr']:.0f}%  exp={A['exp']:+.3f}R  totR={A['tot']:+.1f}  (demo P/L {pnl:+.0f})\n")
    for title, grp in (('by strategy', by_s), ('by market', by_m), ('by symbol', by_sym)):
        print(title + ':')
        for k in sorted(grp, key=lambda x: -_agg(grp[x])['tot']):
            g = _agg(grp[k])
            print(f"  {k:<16} n={g['n']:<4} WR={g['wr']:>3.0f}%  exp={g['exp']:+.3f}R  totR={g['tot']:+.1f}")
        print()
    print("vs backtest (net, both-OOS): UK +0.192R / DE +0.164R. Watch live converge to those.")


if __name__ == '__main__':
    main()
