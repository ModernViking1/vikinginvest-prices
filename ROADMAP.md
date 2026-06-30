# Viking Invest — Institutional Roadmap

Phased path from "built + tested algorithm" to credibly raising
institutional / hedge-fund capital. Companion to `BACKLOG.md` (tactical)
— this is the strategic arc. Not investor-facing as-is.

## Guiding principle

The technology is the easy ~20%. The thing that actually unlocks
institutional allocations is a **real, audited, multi-year track record
inside a proper regulatory + risk wrapper.** Win rate is a retail metric;
institutions allocate on Sharpe / Sortino / max-drawdown / Calmar /
capacity / correlation — and on the people. Sequence accordingly, and do
NOT over-build infrastructure before the edge is proven.

## Architecture — what changes, what doesn't

| Layer | Today | Institutional-grade | Verdict |
|---|---|---|---|
| Presentation (dashboard) | monolithic HTML | web UI reading an API | HTML is **fine** — it's the shop window |
| Signal engine | duplicated Python (`detect_triggers.py`) + JS (in HTML) | one authoritative, tested, **server-side** service | **must consolidate** — engine in a browser is an IP + credibility liability |
| Data / track record | static JSON on CDN | DB with **immutable, auditable** trail | **must change** for DD |
| Execution | cBot on a laptop | monitored, redundant, colocated | **must change** (VPS is step 1, not the last) |
| Pipeline | GitHub Actions cron | monitored service w/ alerting + failover | **must change** |

Key: most of the engine is already server-side Python. The liability is
the **duplication** (logic drifts between Python and the HTML's JS). The
move is to make the Python engine the single source of truth and turn the
HTML into a thin client. This is a consolidation, NOT a from-scratch
rebuild — and it should be **triggered by the track-record milestone**,
not done speculatively.

## Phase 0 — Prove the edge (NOW, ~1–3 months)

- Finish + test the algo on demo; fix execution quality (entry-deviation
  gate — see BACKLOG). Come off the change-freeze with a clean post-fix
  sample. Goal: live demo Δ-vs-sim stabilising and credible.
- Build the institutional-metrics surface (Sharpe / Sortino / Calmar /
  max-DD / monthly distribution / benchmark correlation) on the
  Performance tab + Investor PDF — on backtest data first, clearly
  labelled, so the framework exists when the live record matures.

## Phase 1 — Real money, real record (~3–9 months)

- Move from demo → **small real capital**. You cannot build an
  institutional record on demo; this also forces real slippage/fills.
- Stand up **independent verification** (audited broker statements / fund
  admin / verified third-party record). 12+ months is the minimum bar;
  start the clock now.
- Harden infra to ODD-readiness: VPS/colo execution, monitoring +
  alerting (publish-health heartbeat), DB-backed audit trail, disaster
  recovery, documented risk framework (max DD, position/leverage limits,
  kill-switch).
- Consolidate the engine server-side (the architecture cleanup above).

## Phase 2 — Regulatory + first allocators (~9–18 months)

- **Regulatory vehicle (UK / FCA):** pick the route —
  - SMA / managed account (LPOA) — lightest; client keeps custody.
  - Fund (Cayman/Lux/UK AIF) — heavy: admin, auditor, legal.
  - License the signals to funds — lightest regulation, lowest revenue.
  - FCA authorization or appointed-representative under a host is the gate.
- **First allocators = family offices + sophisticated HNW**, NOT hedge
  funds yet. Far lighter DD; natural first money; builds record + refs.
- Reframe the pitch on **institutional metrics + capacity** (how much
  capital before slippage eats the edge — the execution-cost report
  already shows this strategy has real capacity limits).

## Phase 3 — Institutional / hedge-fund raise (~18–36 months)

- Multi-year audited record + regulatory wrapper + ODD-ready ops.
- Mitigate **key-person risk**: academic validation (KTP / university
  partnership — see grants), a credentialed advisor/CIO, or partnering
  with an established manager.
- Pitch deck on Sharpe / Sortino / Calmar / max-DD / monthly return
  distribution / benchmark correlation / capacity / team — win rate
  demoted to a footnote.

## Funding the journey (parallel, non-blocking)

- R&D Tax Credits (retrospective, no dilution) — engage a specialist now.
- Innovate UK Smart Grants / KTP — fund the engine consolidation +
  academic validation. (See earlier grants research.)
- SEIS/EIS for equity raises if needed (Viking Invest Ltd EIS via
  Condition B per earlier analysis).

## What NOT to do

- Don't migrate to a "proper platform" before the edge is proven — classic
  founder time-sink. The HTML + Python footprint is right for Phase 0–1.
- Don't pitch hedge funds first — start with family offices.
- Don't lead with win rate to institutions.
