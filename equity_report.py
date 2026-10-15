"""One-shot equity live report: (a) realized risk % per trade and (b) net-of-commission
P&L by strategy / region / symbol, since inception + this week. Reads equity-executions.json.

Answers the two post-cap-raise questions in one command:
  (a) are US trades now sizing to ~0.5% risk (was ~0.05% throttled at the 20% cap)?
  (b) is each strategy / region / symbol net-positive AFTER commission?

    python equity_report.py                 # equity base $1,000,000
    python equity_report.py --equity 990000
"""
import json, os, argparse, datetime as dt
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))


def mkt(s):
    s = (s or '').upper()
    for suf, m in (('.NAS', 'US'), ('.NYSE', 'US'), ('.ETR', 'DE'), ('.LSE', 'UK'),
                   ('.PAR', 'FR'), ('.TSE', 'JP'), ('.HK', 'HK'), ('.MAD', 'ES')):
        if suf in s:
            return m
    return '?'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--equity', type=float, default=1_000_000.0)
    ap.add_argument('--file', default=os.path.join(HERE, 'equity-executions.json'))
    args = ap.parse_args()
    ex = json.load(open(args.file)).get('executions', [])
    if not ex:
        print('no executions'); return
    last = max((e.get('closed_ms') or 0) for e in ex)
    print(f"Equity report — {len(ex)} closed trades, latest {dt.datetime.utcfromtimestamp(last/1000):%Y-%m-%d %H:%M}Z, "
          f"equity base ${args.equity:,.0f}\n")

    # ---------- (a) realized risk % ----------
    def riskpct(e):
        v = e.get('volume'); en = e.get('entry'); sl = e.get('init_sl')
        if not v or not en or sl is None:
            return None
        return 100 * v * abs(en - sl) / args.equity
    print("(a) REALIZED RISK % PER TRADE (target 0.5%; low US = still cap-throttled):")
    byr = defaultdict(list)
    for e in ex:
        rp = riskpct(e)
        if rp is not None:
            byr[mkt(e.get('sym'))].append((rp, 'm15' if 'm15' in (e.get('strategy') or '') else 'wide'))
    for m in sorted(byr):
        rs = sorted(x[0] for x in byr[m]); med = rs[len(rs)//2]
        m15 = [x[0] for x in byr[m] if x[1] == 'm15']
        m15med = sorted(m15)[len(m15)//2] if m15 else float('nan')
        print(f"    {m:<4} n={len(rs):<4} median={med:>6.3f}%  (m15 median={m15med:>6.3f}%)")

    # ---------- (b) net P&L ----------
    def agg(rows):
        rr = [e['realized_r'] for e in rows if isinstance(e.get('realized_r'), (int, float))]
        n = len(rr); w = sum(1 for r in rr if r > 0)
        g = sum(e.get('gross_ccy') or 0 for e in rows); c = sum(e.get('commission') or 0 for e in rows)
        net = sum(e.get('profit_ccy') or 0 for e in rows)
        return len(rows), (100*w/n if n else 0), (sum(rr)/n if n else 0), g, c, net

    def block(title, rows):
        print(f"\n(b) {title}")
        print(f"    {'key':<18}{'tr':>5}{'WR':>5}{'avgR':>7}{'gross$':>10}{'comm$':>10}{'NET$':>10}")
        groups = defaultdict(list)
        for key, e in rows:
            groups[key].append(e)
        for k in sorted(groups, key=lambda k: -agg(groups[k])[5]):
            tr, wr, ar, g, c, net = agg(groups[k])
            print(f"    {str(k):<18}{tr:>5}{wr:>4.0f}%{ar:>+7.2f}{g:>+10.0f}{c:>+10.0f}{net:>+10.0f}")

    monday = (dt.datetime.utcnow() - dt.timedelta(days=dt.datetime.utcnow().weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    wk_ms = int(monday.timestamp()*1000)
    wk = [e for e in ex if (e.get('closed_ms') or 0) >= wk_ms]

    block("BY STRATEGY (since inception)", [(e.get('strategy', '?'), e) for e in ex])
    block("BY REGION (since inception)", [(mkt(e.get('sym')), e) for e in ex])
    block("BY REGION (this week)", [(mkt(e.get('sym')), e) for e in wk])
    # US per-symbol (the expansion focus)
    usrows = [(e.get('sym'), e) for e in ex if mkt(e.get('sym')) == 'US']
    if usrows:
        block("US PER-SYMBOL (since inception)", usrows)

    _, _, _, g, c, net = agg(ex)
    print(f"\nTOTAL since inception: gross {g:+.0f}  commission {c:+.0f}  NET {net:+.0f}")


if __name__ == '__main__':
    main()
