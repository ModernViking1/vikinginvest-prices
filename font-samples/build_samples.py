#!/usr/bin/env python3
"""Generate four font-sample variants of index.html for review.

The actual /index.html is left untouched. Each sample lives at
font-samples/sample-N-<slug>.html and is reachable from the deploy
under /font-samples/sample-N-<slug>.html.

A small sticky banner at the top of each sample identifies which
combo is on screen so screenshots stay self-labelling.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / 'index.html'
OUT = ROOT / 'font-samples'

# Each sample picks one headline font + one body font and (optionally)
# overrides the headline weight to match the user's spec.
SAMPLES = [
    {
        'slug': '1-cormorant-inter',
        'label': 'Sample 1 · Cormorant Garamond SemiBold + Inter',
        'fonts_href': (
            'https://fonts.googleapis.com/css2?'
            'family=Cormorant+Garamond:wght@600;700&'
            'family=Inter:wght@300;400;500;600;700;900&display=swap'
        ),
        'headline_family': "'Cormorant Garamond', Georgia, serif",
        'headline_weight': 600,
        'body_family': "'Inter', system-ui, sans-serif",
    },
    {
        'slug': '2-instrument-manrope',
        'label': 'Sample 2 · Instrument Serif + Manrope',
        'fonts_href': (
            'https://fonts.googleapis.com/css2?'
            'family=Instrument+Serif:ital@0;1&'
            'family=Manrope:wght@300;400;500;600;700;800&display=swap'
        ),
        'headline_family': "'Instrument Serif', Georgia, serif",
        # Instrument Serif ships with weight 400 only — don't force a
        # heavier weight or the browser will synthesise bold and the
        # whole point of the font is lost.
        'headline_weight': 400,
        'body_family': "'Manrope', system-ui, sans-serif",
    },
    {
        'slug': '3-jakarta-jakarta',
        'label': 'Sample 3 · Plus Jakarta Sans ExtraBold + Plus Jakarta Sans',
        'fonts_href': (
            'https://fonts.googleapis.com/css2?'
            'family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap'
        ),
        'headline_family': "'Plus Jakarta Sans', system-ui, sans-serif",
        'headline_weight': 800,
        'body_family': "'Plus Jakarta Sans', system-ui, sans-serif",
    },
    {
        'slug': '4-baskerville-inter',
        'label': 'Sample 4 · Libre Baskerville + Inter',
        'fonts_href': (
            'https://fonts.googleapis.com/css2?'
            'family=Libre+Baskerville:wght@400;700&'
            'family=Inter:wght@300;400;500;600;700;900&display=swap'
        ),
        'headline_family': "'Libre Baskerville', Georgia, serif",
        'headline_weight': 700,
        'body_family': "'Inter', system-ui, sans-serif",
    },
]

# Source rule for h1,h2 in index.html — captured verbatim so the
# substitution is exact and we don't fight whitespace.
H_HEADLINE_RULE = (
    "h1,h2{font-family:'Space Grotesk',sans-serif;"
    "line-height:1.1;letter-spacing:-1.2px;font-weight:700;}"
)
H3_HEADLINE_RULE = (
    "h3{font-family:'Space Grotesk',sans-serif;"
    "line-height:1.3;letter-spacing:-0.3px;font-weight:600;}"
)

BANNER_HTML = """<div class="font-sample-banner">
  <strong>{label}</strong>
  <span class="font-sample-banner-nav">
    <a href="./sample-1-cormorant-inter.html">1</a>
    <a href="./sample-2-instrument-manrope.html">2</a>
    <a href="./sample-3-jakarta-jakarta.html">3</a>
    <a href="./sample-4-baskerville-inter.html">4</a>
    <a href="../index.html">live</a>
  </span>
</div>"""

BANNER_CSS = """.font-sample-banner{
  position:fixed;top:0;left:0;right:0;z-index:1000;
  background:#1c1a17;color:#f0ede8;
  padding:8px 16px;font-family:system-ui,sans-serif;font-size:12px;
  display:flex;justify-content:space-between;align-items:center;gap:12px;
  border-bottom:1px solid #c8860a;
}
.font-sample-banner strong{font-weight:700;letter-spacing:0.3px;}
.font-sample-banner-nav{display:flex;gap:6px;}
.font-sample-banner-nav a{
  color:#f0ede8;padding:2px 8px;border:1px solid #5a554c;border-radius:3px;
  font-size:11px;font-weight:700;letter-spacing:0.5px;text-decoration:none;
  transition:background .1s,color .1s;
}
.font-sample-banner-nav a:hover{background:#c8860a;color:#1c1a17;border-color:#c8860a;}
body{padding-top:34px;}"""


def build_sample(src_html: str, sample: dict) -> str:
    out = src_html

    # ── 1. Replace Google Fonts <link href> ─────────────────────────
    out = re.sub(
        r'<link href="https://fonts\.googleapis\.com/css2\?[^"]*" rel="stylesheet">',
        f'<link href="{sample["fonts_href"]}" rel="stylesheet">',
        out,
    )

    # ── 2. Title — make it obvious which sample is loaded ───────────
    out = re.sub(
        r'<title>[^<]*</title>',
        f'<title>{sample["label"]} — Viking Invest</title>',
        out,
    )

    # ── 3. Headline rules — override BEFORE the global replacement ──
    new_h12 = (
        f"h1,h2{{font-family:{sample['headline_family']};"
        f"line-height:1.1;letter-spacing:-1.2px;"
        f"font-weight:{sample['headline_weight']};}}"
    )
    new_h3 = (
        f"h3{{font-family:{sample['headline_family']};"
        f"line-height:1.3;letter-spacing:-0.3px;"
        f"font-weight:{max(sample['headline_weight'] - 100, 400)};}}"
    )
    assert H_HEADLINE_RULE in out, 'h1,h2 rule not found verbatim'
    assert H3_HEADLINE_RULE in out, 'h3 rule not found verbatim'
    out = out.replace(H_HEADLINE_RULE, new_h12)
    out = out.replace(H3_HEADLINE_RULE, new_h3)

    # ── 4. Per-section override: any heading that has its own
    #     font-weight needs to be moved to the new headline weight too,
    #     otherwise a serif at 900 looks heavy and a sans at 700 looks
    #     light relative to the rest of the headline hierarchy. ─────
    section_heading_weights = [
        ('.hero h1{font-size:clamp(34px,5.2vw,58px);font-weight:900;max-width:880px;}',
         f".hero h1{{font-size:clamp(34px,5.2vw,58px);font-weight:{sample['headline_weight']};max-width:880px;}}"),
        ('.sec-head h2{font-size:clamp(26px,3.4vw,38px);font-weight:800;margin:14px 0 12px;}',
         f".sec-head h2{{font-size:clamp(26px,3.4vw,38px);font-weight:{sample['headline_weight']};margin:14px 0 12px;}}"),
        ('.platform h2{font-size:clamp(26px,3.4vw,38px);font-weight:800;color:#fff;margin:14px 0 16px;}',
         f".platform h2{{font-size:clamp(26px,3.4vw,38px);font-weight:{sample['headline_weight']};color:#fff;margin:14px 0 16px;}}"),
        ('.about h2{font-size:clamp(26px,3.4vw,38px);font-weight:800;margin:14px 0 14px;}',
         f".about h2{{font-size:clamp(26px,3.4vw,38px);font-weight:{sample['headline_weight']};margin:14px 0 14px;}}"),
        ('.contact h2{font-size:clamp(28px,3.8vw,42px);font-weight:900;color:#fff;margin:14px 0 14px;}',
         f".contact h2{{font-size:clamp(28px,3.8vw,42px);font-weight:{sample['headline_weight']};color:#fff;margin:14px 0 14px;}}"),
    ]
    for old, new in section_heading_weights:
        assert old in out, f'section heading rule not found: {old[:60]}...'
        out = out.replace(old, new)

    # ── 5. Global swap of remaining 'Space Grotesk' usages to body ──
    out = out.replace("'Space Grotesk',sans-serif", sample['body_family'])

    # ── 6. Make dashboard links root-absolute so they still work
    #     when the sample is served from /font-samples/. ────────────
    out = out.replace('href="./dashboard.html"', 'href="/dashboard.html"')

    # ── 7. Drop the visitor-counter beacon — samples shouldn't
    #     pollute the production unique-visitor count. ──────────────
    out = re.sub(
        r'<script>\s*// Unique-visitor beacon.*?</script>',
        '',
        out,
        flags=re.DOTALL,
    )

    # ── 8. Inject the sample banner CSS + DOM. The banner lives
    #     above the sticky nav so it's always reachable for hopping
    #     between samples. ──────────────────────────────────────────
    out = out.replace('</style>', BANNER_CSS + '\n</style>', 1)
    banner = BANNER_HTML.format(label=sample['label'])
    out = out.replace('<body>', '<body>\n' + banner, 1)

    return out


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    src = SRC.read_text()
    for sample in SAMPLES:
        rendered = build_sample(src, sample)
        target = OUT / f"sample-{sample['slug']}.html"
        target.write_text(rendered)
        print(f'  wrote {target.relative_to(ROOT)}  ({len(rendered):,} bytes)')

    # Index file linking all four samples
    cards = '\n'.join(
        f'<a class="card" href="./sample-{s["slug"]}.html">'
        f'<strong>{s["label"]}</strong>'
        f'<span>open sample &rarr;</span></a>'
        for s in SAMPLES
    )
    index = """<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Viking Invest — font samples</title>
<style>
body{font-family:system-ui,sans-serif;background:#f0ede8;color:#1c1a17;
  max-width:640px;margin:60px auto;padding:0 20px;line-height:1.55;}
h1{font-size:22px;margin-bottom:8px;}
p{color:#4a4640;margin-bottom:32px;}
a.card{display:block;background:#fff;border:1px solid #ddd8d0;border-radius:8px;
  padding:18px 22px;margin-bottom:12px;color:#1c1a17;text-decoration:none;
  transition:transform .12s,border-color .15s,box-shadow .15s;}
a.card:hover{transform:translateY(-2px);border-color:#c8860a;
  box-shadow:0 4px 16px rgba(0,0,0,.08);}
a.card strong{display:block;font-size:15px;margin-bottom:4px;}
a.card span{font-size:13px;color:#9a9088;}
a.live{margin-top:24px;display:inline-block;font-size:13px;color:#c8860a;}
</style></head><body>
<h1>Viking Invest — font samples</h1>
<p>Four font-pair candidates against the live consultancy page. Pick one, the
  live <code>/index.html</code> stays untouched until you've chosen.</p>
__CARDS__
<a class="live" href="/index.html">&larr; back to live consultancy page</a>
</body></html>""".replace('__CARDS__', cards)
    (OUT / 'index.html').write_text(index)
    print(f'  wrote font-samples/index.html')


if __name__ == '__main__':
    main()
