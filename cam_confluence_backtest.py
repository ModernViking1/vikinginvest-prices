"""cam_rev × order-book CONFLUENCE backtest.

QUESTION
--------
Does a dense retail liquidity shelf sitting ON the faded cam_rev level improve the fade?
i.e. when price spikes to R3 and there's a thick wall of resting retail orders / open
positions right there, is the rejection-fade more reliable than when the level is empty?

If yes, the order-book shelf becomes a live filter/sizer on top of the (already proven) R+
edge. If flat, it's descriptive colour only and we don't wire it into sizing.

HOW
---
Forward study — OANDA serves no deep order-book history, so this can only evaluate cam_rev
signals that fired AFTER we began collecting orderbook-history.jsonl (see oanda_orderbook.py
--history, inlined in fetch-data at ~20-min cadence). For each cam_rev signal:
  1. detect via H.detect_cam_rev (same detector as live); the faded level is s['pivot']
     (R3 for a bear fade, S3 for a bull fade).
  2. find the order-book snapshot nearest at-or-before the signal's entry_ts for that pair
     (within MATCH_TOL_MIN); read the shelf density at that level (order_pct, pos_pct).
  3. score the bracket with H.score_sess (immediate-fill, resolved-only, bracket-honest —
     the same idealised model the other cam_* studies use; live limit-exec runs ~0.2R lower).
Then bucket the resolved trades by shelf density and compare WR / expectancy.

Commits nothing. Prints the result to the log. Until enough matched+resolved trades exist it
reports the collection progress and the ETA instead of a noisy small-sample table.

  python cam_confluence_backtest.py --hist deep-ohlc.json --book orderbook-history.jsonl
"""
import argparse, bisect, json, os
from collections import defaultdict
from datetime import datetime, timezone

import unified_shadow_harness as H
from detect_triggers import PAIR_CLASS
from backtest_rsi_per_class import _bars_norm

CLASSES = {'major', 'minor', 'comm'}       # the classes OANDA serves a book for (majors/minors/metals)
MATCH_TOL_MIN = 40                          # a snapshot must be within this many min before the signal
MIN_RESOLVED = 30                           # below this, report progress instead of a table
# shelf-density buckets on (order_pct + pos_pct) at the faded level
DENSE_HI = 4.0                              # >= this total % = a thick shelf
DENSE_LO = 1.5                              # < this = effectively empty


def _ts(s):
    return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def _load_book(path):
    """orderbook-history.jsonl -> {pair: ([snap_ts...], [ {R3:[lvl,ord,pos],...} ... ]) } sorted."""
    per = defaultdict(lambda: ([], []))
    if not os.path.exists(path):
        return {}, 0
    n = 0
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            snap = json.loads(line)
            t = _ts(snap["t"]); n += 1
        except Exception:
            continue
        for pk, d in (snap.get("pairs") or {}).items():
            lv = d.get("lv")
            if lv:
                per[pk][0].append(t); per[pk][1].append(lv)
    # sort each pair by ts
    out = {}
    for pk, (ts, lvs) in per.items():
        order = sorted(range(len(ts)), key=lambda i: ts[i])
        out[pk] = ([ts[i] for i in order], [lvs[i] for i in order])
    return out, n


def _shelf_at(book_pk, sig_ts, level_name):
    """Nearest snapshot at-or-before sig_ts within MATCH_TOL_MIN -> (order_pct, pos_pct) or None."""
    if not book_pk:
        return None
    ts, lvs = book_pk
    j = bisect.bisect_right(ts, sig_ts) - 1
    if j < 0:
        return None
    if (sig_ts - ts[j]) / 60.0 > MATCH_TOL_MIN:
        return None
    rec = lvs[j].get(level_name)
    if not rec or len(rec) < 3:
        return None
    return float(rec[1]), float(rec[2])          # order_pct, pos_pct


def _bucket(total):
    return 'dense' if total >= DENSE_HI else ('empty' if total < DENSE_LO else 'mid')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--hist', default='deep-ohlc.json')
    ap.add_argument('--book', default='orderbook-history.jsonl')
    args = ap.parse_args()

    book, n_snaps = _load_book(args.book)
    if n_snaps == 0:
        print(f"=== cam_rev confluence · no history yet ({args.book} empty/missing) ===")
        print("Collection is inlined in fetch-data (~20-min cadence). Re-run once snapshots exist.")
        return 0
    # collection span
    all_ts = [t for pk in book for t in book[pk][0]]
    span_days = (max(all_ts) - min(all_ts)) / 86400.0 if all_ts else 0.0

    pairs = json.load(open(args.hist)).get('pairs', {})
    stats = defaultdict(list)          # bucket -> [R...]  (combined order+pos density)
    o_stats = defaultdict(list)        # bucket by ORDER pct only
    p_stats = defaultdict(list)        # bucket by POSITION pct only
    n_sig = matched = resolved = 0
    for pk, layers in pairs.items():
        if PAIR_CLASS.get(pk) not in CLASSES or pk not in book or not isinstance(layers, dict):
            continue
        m15 = _bars_norm(layers.get('m15') or []); daily = _bars_norm(layers.get('daily') or [])
        if len(m15) < 500 or len(daily) < 30:
            continue
        for s in H.detect_cam_rev(pk, m15, daily):
            n_sig += 1
            name = 'R3' if s['dir'] == 'bear' else 'S3'      # the faded level
            shelf = _shelf_at(book.get(pk), s['entry_ts'], name)
            if shelf is None:
                continue
            matched += 1
            o_pct, p_pct = shelf
            st, oc = H.score_sess(m15, s['entry_ts'], s['entry'], s['stop'], s['target'],
                                  s['dir'], H.CAM_HOLD)
            if st != 'resolved':
                continue
            resolved += 1
            stats[_bucket(o_pct + p_pct)].append(oc)
            o_stats[_bucket(o_pct)].append(oc)
            p_stats[_bucket(p_pct)].append(oc)

    print(f"=== cam_rev × order-book confluence · {args.hist} × {args.book} ===")
    print(f"collection: {n_snaps} snapshots over {span_days:.1f} days · "
          f"cam_rev signals in window={n_sig} · matched a snapshot={matched} · resolved={resolved}\n")
    if resolved < MIN_RESOLVED:
        need = MIN_RESOLVED - resolved
        rate = resolved / max(span_days, 0.01)
        eta = need / rate if rate > 0 else float('inf')
        print(f"Not enough matched+resolved trades yet (have {resolved}, want >= {MIN_RESOLVED}).")
        if rate > 0:
            print(f"Accruing ~{rate:.1f} resolved matched fades/day — ~{eta:.0f} more days to a read.")
        else:
            print("No resolved matched fades yet — let collection run and re-check.")
        return 0

    def table(title, d):
        print(f"{title}")
        for b in ('dense', 'mid', 'empty'):
            rs = d.get(b, [])
            if not rs:
                print(f"   {b:6} (n<1)"); continue
            wr = 100.0 * sum(1 for r in rs if r > 0) / len(rs)
            exp = sum(rs) / len(rs)
            print(f"   {b:6} n={len(rs):<4} WR={wr:3.0f}%  exp={exp:+.3f}R")
        print()

    table(f"by COMBINED shelf (order+pos %, dense>={DENSE_HI} / empty<{DENSE_LO}):", stats)
    table("by ORDER-book shelf only:", o_stats)
    table("by POSITION-book shelf only:", p_stats)
    print("Reading it: if 'dense' clearly beats 'empty' on WR/exp, the shelf is a real filter/sizer")
    print("on the R+ fade. If flat across buckets, it's descriptive colour — don't wire it to sizing.")
    return 0


if __name__ == '__main__':
    main()
