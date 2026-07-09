"""Score the shadow-logged signals (index/major/minor 4/4 macd-primary — the
classes we moved to observation-only) against the historical price path under
the SAME realistic limit-fill model the live gate is built on, so we can decide
whether any of them earns promotion to live BEFORE risking capital.

Each shadow entry carries pair/class/dir/entry/stop/target/trigger_ts. We locate
the m15 path after the trigger, model the cBot's limit entry (fill only if the
level is touched within EXPIRY bars, else no-fill), then walk to stop/target
(stop-before-target within a bar, conservative). Signals too recent to have
resolved a full 1:1 are reported as PENDING and excluded from WR/expectancy.

Run:  python shadow_scorer.py
"""
import json, bisect
from collections import defaultdict
from datetime import datetime, timezone

HIST = '/home/user/vikinginvest-prices/historical-ohlc.json'
SHADOW = '/home/user/vikinginvest-prices/shadow-signals.json'
EXPIRY = 3       # 45 min limit expiry (cBot LimitExpiryMin=45)
WALK = 48        # ~12h to resolve a 1:1


def iso_ms(s):
    s = str(s).replace('Z', '+00:00')
    if '.' in s:
        head, rest = s.split('.', 1)
        digits = ''.join(c for c in rest if c.isdigit())[:6]
        tz = rest[len(''.join(c for c in rest if c.isdigit())):]
        s = f"{head}.{digits}{tz}"
    try:
        return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp() * 1000)
    except Exception:
        try:
            return int(datetime.fromisoformat(s).timestamp() * 1000)
        except Exception:
            return None


def score(m15_ms, m15, trig_ms, entry, stop, target, d):
    """Return 'win'/'loss'/'nofill'/'pending'."""
    if abs(entry - stop) <= 0:
        return 'skip'
    i = bisect.bisect_right(m15_ms, trig_ms)   # first bar after trigger
    # limit fill within EXPIRY bars
    fill = None
    for j in range(i, min(i + EXPIRY, len(m15))):
        b = m15[j]
        if (d == 'bull' and b['l'] <= entry) or (d == 'bear' and b['h'] >= entry):
            fill = j
            break
    if fill is None:
        # no bars at all after trigger yet -> pending; else genuine no-fill
        return 'pending' if i >= len(m15) - EXPIRY else 'nofill'
    end = min(fill + 1 + WALK, len(m15))
    for j in range(fill + 1, end):
        b = m15[j]
        if d == 'bull':
            if b['l'] <= stop: return 'loss'
            if b['h'] >= target: return 'win'
        else:
            if b['h'] >= stop: return 'loss'
            if b['l'] <= target: return 'win'
    # ran out of bars before resolving
    return 'pending' if end >= len(m15) else 'loss'


def main():
    hist = json.load(open(HIST))['pairs']
    entries = json.load(open(SHADOW)).get('entries', [])
    cache = {}
    for pk, pd in hist.items():
        m15 = pd.get('m15', [])
        cache[pk] = ([iso_ms(b['t']) for b in m15], m15)
    agg = defaultdict(lambda: {'win': 0, 'loss': 0, 'nofill': 0, 'pending': 0, 'skip': 0})
    conf_agg = defaultdict(lambda: {'win': 0, 'loss': 0})
    for e in entries:
        pk = e.get('pair'); cls = e.get('class', '?'); d = e.get('dir')
        entry = e.get('entry'); stop = e.get('stop'); tgt = e.get('target')
        tms = iso_ms(e.get('trigger_ts'))
        if pk not in cache or None in (entry, stop, tgt, tms) or d not in ('bull', 'bear'):
            agg[cls]['skip'] += 1
            continue
        ms, m15 = cache[pk]
        r = score(ms, m15, tms, entry, stop, tgt, d)
        agg[cls][r] += 1
        if r in ('win', 'loss'):
            conf_agg[(cls, e.get('confluence'))][r] += 1
    print("SHADOW-SIGNAL SCORING (realistic limit fill) — observation-only classes")
    print(f"{'class':<8} {'resolved':>9} {'WR%':>6} {'win':>4} {'loss':>5} {'nofill':>7} {'pending':>8}")
    for cls in sorted(agg):
        a = agg[cls]; res = a['win'] + a['loss']
        wr = 100 * a['win'] / res if res else 0
        print(f"{cls:<8} {res:>9} {wr:>5.1f}% {a['win']:>4} {a['loss']:>5} {a['nofill']:>7} {a['pending']:>8}")
    print("\nBy class + confluence (resolved only):")
    for (cls, conf), a in sorted(conf_agg.items()):
        res = a['win'] + a['loss']
        print(f"  {cls:<7} conf={conf}: n={res} WR={100*a['win']/res:.1f}%")
    print(f"\ntotal shadow entries: {len(entries)}")


if __name__ == '__main__':
    main()
