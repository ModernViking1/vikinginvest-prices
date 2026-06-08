"""Viking Invest — early-investor pitch generator.

Produces investor-pitch.pdf from the structured copy below. Kept as a
script (not a Markdown doc → PDF chain) so the deck stays single-file,
versioned, and trivial to regenerate when the numbers change.
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
    KeepTogether
)
from reportlab.pdfgen import canvas

INK = HexColor('#0e1726')
ACCENT = HexColor('#1a7a4a')
GOLD = HexColor('#b88a2c')
BEAR = HexColor('#c0281a')
MUTED = HexColor('#6b7280')
RULE = HexColor('#d8dde5')
BG_SOFT = HexColor('#f5f7fa')

styles = getSampleStyleSheet()

def make_styles():
    return {
        'h1': ParagraphStyle('h1', parent=styles['Heading1'],
                             fontName='Helvetica-Bold', fontSize=26,
                             leading=30, textColor=INK,
                             spaceBefore=0, spaceAfter=4),
        'tagline': ParagraphStyle('tag', parent=styles['Normal'],
                                  fontName='Helvetica-Oblique', fontSize=11,
                                  leading=15, textColor=MUTED,
                                  spaceAfter=14),
        'h2': ParagraphStyle('h2', parent=styles['Heading2'],
                             fontName='Helvetica-Bold', fontSize=14,
                             leading=18, textColor=ACCENT,
                             spaceBefore=16, spaceAfter=6),
        'h3': ParagraphStyle('h3', parent=styles['Heading3'],
                             fontName='Helvetica-Bold', fontSize=11,
                             leading=14, textColor=INK,
                             spaceBefore=8, spaceAfter=3),
        'body': ParagraphStyle('body', parent=styles['BodyText'],
                               fontName='Helvetica', fontSize=10,
                               leading=14, textColor=INK,
                               spaceAfter=6, alignment=TA_LEFT),
        'bullet': ParagraphStyle('bullet', parent=styles['BodyText'],
                                 fontName='Helvetica', fontSize=10,
                                 leading=14, textColor=INK,
                                 leftIndent=14, bulletIndent=2,
                                 spaceAfter=3),
        'pull': ParagraphStyle('pull', parent=styles['BodyText'],
                               fontName='Helvetica-Oblique', fontSize=11,
                               leading=16, textColor=ACCENT,
                               leftIndent=10, rightIndent=10,
                               spaceBefore=8, spaceAfter=12),
        'caption': ParagraphStyle('caption', parent=styles['BodyText'],
                                  fontName='Helvetica', fontSize=8.5,
                                  leading=11, textColor=MUTED,
                                  spaceAfter=4),
    }

def header_footer(canv: canvas.Canvas, doc):
    canv.saveState()
    canv.setFont('Helvetica-Bold', 9)
    canv.setFillColor(INK)
    canv.drawString(20*mm, 287*mm, 'VIKING INVEST')
    canv.setFont('Helvetica', 8.5)
    canv.setFillColor(MUTED)
    canv.drawRightString(190*mm, 287*mm, 'Investor Pitch — June 2026')
    canv.setStrokeColor(RULE)
    canv.setLineWidth(0.4)
    canv.line(20*mm, 284*mm, 190*mm, 284*mm)
    canv.setFont('Helvetica', 8)
    canv.drawCentredString(105*mm, 12*mm,
                           'Confidential — for early-investor discussions only · vikinginvest.org')
    canv.drawRightString(190*mm, 12*mm, f'Page {doc.page}')
    canv.restoreState()

def bullet_para(text, s):
    return Paragraph(f'• {text}', s['bullet'])

def build():
    out_path = 'investor-pitch.pdf'
    doc = SimpleDocTemplate(out_path, pagesize=A4,
                            leftMargin=20*mm, rightMargin=20*mm,
                            topMargin=22*mm, bottomMargin=18*mm,
                            title='Viking Invest — Investor Pitch',
                            author='Viking Invest')
    s = make_styles()
    story = []

    # ── Title block ───────────────────────────────────────────────
    story += [
        Paragraph('Viking Invest', s['h1']),
        Paragraph('Open-engine retail trading infrastructure — '
                  'the dashboard, the backtest and the alert are the same code.',
                  s['tagline']),
    ]

    # ── The opportunity ──────────────────────────────────────────
    story += [
        Paragraph('The opportunity', s['h2']),
        Paragraph(
            'Retail traders spend an estimated <b>$1.2bn / year</b> on signal '
            'subscriptions, mentorship courses and analyst services across '
            'forex, crypto, indices and commodities. Three things are wrong '
            'with the current market:',
            s['body']),
        bullet_para('<b>Signal services are black boxes.</b> A Telegram alert '
                    'says "BUY EUR/USD" with no math behind it. Published win '
                    'rates can\'t be independently audited. Brokers pay '
                    'affiliate kickbacks per trade — so providers are '
                    'incentivised to fire more signals, not better ones.', s),
        bullet_para('<b>Analyst services don\'t scale.</b> Elliott Wave '
                    'Forecast ($99–399/mo) and EWM Interactive sell a human\'s '
                    'wave count. Quality is real, but it\'s one chart at a '
                    'time and the count can quietly change between updates.', s),
        bullet_para('<b>Mentor courses sell theory, not infrastructure.</b> '
                    'Paul Bratby and similar sell you a curriculum, then you '
                    'apply it manually on TradingView. The student does all '
                    'the multi-timeframe work by eye.', s),
        Paragraph(
            'There is no product in this market that gives a retail trader '
            '<b>institutional-grade multi-timeframe alignment</b>, '
            '<b>open methodology</b>, <b>same-code alerts</b> and '
            '<b>auditable backtests</b> at the same time.',
            s['pull']),
    ]

    # ── What we built ────────────────────────────────────────────
    story += [
        Paragraph('What we\'ve built', s['h2']),
        Paragraph(
            'A live 4/4-confluence trading dashboard covering ~45 instruments '
            '(FX majors / minors / crosses, precious metals, oil, the major '
            'indices, and the top-10 crypto). One engine produces three '
            'surfaces:',
            s['body']),
    ]

    surfaces = [
        ['Surface', 'What it is', 'Who it serves'],
        ['Dashboard',
         'Live HTML page, every pair scored across four timeframes (EW, 1H, 15m, 4H cloud)',
         'Active trader watching the market'],
        ['Backtest engine',
         'Same rules walked over 365 days of OHLC, equity curve + per-pair WR',
         'Trader validating before risking capital'],
        ['Telegram alerts',
         'Python detector mirroring the JS engine byte-for-byte; alerts on 4/4 + retest',
         'Trader who can\'t watch the screen'],
    ]
    t = Table(surfaces, colWidths=[26*mm, 78*mm, 56*mm])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), ACCENT),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 9),
        ('LEADING', (0, 0), (-1, -1), 12),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, BG_SOFT]),
        ('LINEBELOW', (0, 0), (-1, -1), 0.3, RULE),
    ]))
    story += [t, Spacer(1, 8)]

    story += [
        Paragraph(
            'The dashboard is publicly viewable today. ~70% aggregate win rate '
            'over the documented Apr 2025–Apr 2026 backtest window across the '
            'wick-primary cohort. Every rule change is versioned '
            '(<font face="Courier" size="9">RULES_VERSION</font>) and the '
            'failure-mode panel surfaces hypotheses with sample-size honesty '
            'before they\'re adopted.',
            s['body']),
    ]

    story += [PageBreak()]

    # ── The moat ─────────────────────────────────────────────────
    story += [
        Paragraph('Why we win — the five-point moat', s['h2']),
    ]

    moat = [
        ('1. One engine, three surfaces',
         'Dashboard, backtest and Telegram alerts are produced by '
         'byte-for-byte mirrored code (JS <font face="Courier" size="9">'
         'detectIntradaySignal</font> ↔ Python <font face="Courier" size="9">'
         'detect_intraday_signal</font>). A subscriber\'s alert is the same '
         'function call that generated the published equity curve. No one '
         'else in the retail space does this — competitors split alerts, '
         'dashboards and backtests across teams or vendors, so the numbers '
         'never reconcile.'),
        ('2. Open, versioned, auditable rules',
         '<font face="Courier" size="9">RULES_VERSION</font> fingerprint '
         '(currently 2026-06-09d), inline change-log, and a live '
         'failure-mode panel that lists hypotheses (1H RSI 80/20 gate, fib '
         'half-size entry, counter-bar invalidation) with sample size and '
         'forward measurement BEFORE adoption. Subscribers can audit the '
         'logic; competitors can\'t even show it.'),
        ('3. Per-asset-class methodology routing',
         'FX/crypto use wick-extreme entry, commodities/indices use a Fib '
         '38% half-size entry, encoded in <font face="Courier" size="9">'
         '_btProfileFor()</font>. Most competitors apply one playbook to '
         'everything — Viking accepts that asset classes behave differently '
         'and routes accordingly. Hidden complexity that compounds the '
         'edge.'),
        ('4. Same-bar invalidation discipline',
         'Pre-trigger invalidation set — stop-breached, opposing-CHoCH, '
         '2× counter-bars, 1H RSI extreme — collapses setups to CANCELLED '
         'with a named reason instead of hiding them. Subscribers see the '
         'trades we suppressed AND why. Signal services don\'t — they only '
         'show you fires.'),
        ('5. No broker affiliate distortion',
         'Zero revenue from "join my broker via this link" kickbacks. '
         'Standard retail signal services earn a per-lot rebate from the '
         'broker, which creates documented bias toward more trades, not '
         'better trades. Viking\'s monetisation will be subscription-only — '
         'aligned with subscriber outcomes, not trade volume.'),
    ]
    for title, body in moat:
        story.append(KeepTogether([
            Paragraph(title, s['h3']),
            Paragraph(body, s['body']),
        ]))

    # ── The sharpest line ────────────────────────────────────────
    story += [
        Paragraph(
            '"Competitors sell you their conclusion. Viking shows you the '
            'math the conclusion was derived from — and the same code runs '
            'your alerts."',
            s['pull']),
    ]

    story += [PageBreak()]

    # ── Tech stack ───────────────────────────────────────────────
    story += [
        Paragraph('Tech & defensibility', s['h2']),
        Paragraph(
            'The product is deliberately boring underneath — cheap to run, '
            'easy to ship, hard to fork once the auditable history compounds.',
            s['body']),
    ]
    tech = [
        ['Layer', 'Stack', 'Cost', 'Defensibility'],
        ['Data ingestion',
         'GitHub Actions cron · OANDA / Coinbase / public price APIs',
         '$0 (within free CI tier)',
         'Multi-source fallback published in code'],
        ['Engine',
         'Vanilla JS dashboard + Python detector — same algorithms',
         '$0 (static HTML, npm/jsDelivr CDN)',
         '~3y of versioned rule history once we publish the changelog'],
        ['Storage',
         'JSON on the repo + Supabase (auth, profiles, subscriptions)',
         '~$25/mo at current scale',
         'RLS + admin-only SECURITY DEFINER RPCs'],
        ['Alerts',
         'Telegram bot, per-user routing by alert class + region',
         '$0 (Telegram is free)',
         'Same-engine alerts are the moat, not the channel'],
        ['Distribution',
         'Public dashboard URL · npm package · GitHub Pages',
         '$0',
         'SEO-friendly, no app-store gatekeeper'],
    ]
    t2 = Table(tech, colWidths=[28*mm, 56*mm, 28*mm, 48*mm])
    t2.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), ACCENT),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8.5),
        ('LEADING', (0, 0), (-1, -1), 11),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, BG_SOFT]),
        ('LINEBELOW', (0, 0), (-1, -1), 0.3, RULE),
    ]))
    story += [t2, Spacer(1, 10)]

    # ── Competitive landscape ────────────────────────────────────
    story += [
        Paragraph('Competitive landscape', s['h2']),
    ]
    comp = [
        ['Competitor', 'Shape', 'What they sell', 'Where Viking wins'],
        ['Paul Bratby / Trade The Fifth',
         'Mentor + course',
         'Elliott Wave methodology, MT5 indicators, YouTube education',
         'Live dashboard does the work the student does by hand'],
        ['Elliott Wave Forecast / EWM Interactive',
         'Analyst service',
         '$99–399/mo human-curated wave counts across ~10 markets',
         'Automated, ~45 markets, audit-trail of every rule change'],
        ['AltSignals / Learn2Trade / fxpremiere',
         'Telegram signal seller',
         '$40–150/mo curated "BUY EUR/USD" alerts, opaque WR claims',
         'Open rules, same code runs backtest AND alert, no broker kickback'],
    ]
    t3 = Table(comp, colWidths=[40*mm, 24*mm, 50*mm, 46*mm])
    t3.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), ACCENT),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8.5),
        ('LEADING', (0, 0), (-1, -1), 11),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
        ('RIGHTPADDING', (0, 0), (-1, -1), 5),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, BG_SOFT]),
        ('LINEBELOW', (0, 0), (-1, -1), 0.3, RULE),
    ]))
    story += [t3]

    story += [PageBreak()]

    # ── Traction ─────────────────────────────────────────────────
    story += [
        Paragraph('Traction & proof points', s['h2']),
        bullet_para('<b>~70% aggregate win rate</b> across the wick-primary '
                    'cohort over the Apr 2025–Apr 2026 backtest window (37 of '
                    '~45 instruments, ex. low-correlation crosses recently '
                    'dropped to improve aggregate quality).', s),
        bullet_para('<b>~45 instruments live</b> — FX majors, minors, crosses, '
                    'XAU/XAG, BRENT, DAX/FTSE/DJ/NAS/SPX/CAC/Nikkei, BTC + 9 '
                    'alt-coins.', s),
        bullet_para('<b>Engine, alerts and backtest reconcile to the same '
                    'rule set</b> — every change has a fingerprint, every '
                    'subscriber can audit.', s),
        bullet_para('<b>Public dashboard, zero paywall today</b> — early '
                    'audience building, no churn risk at conversion.', s),
        bullet_para('<b>Supabase + Telegram pipeline already in place</b> — '
                    'auth, profile, alert-class preferences, admin gate. '
                    'Subscription monetisation is the next switch flip, not a '
                    're-architecture.', s),

        Paragraph('Where the money goes', s['h2']),
        Paragraph(
            'We are looking for early-stage capital to:',
            s['body']),
        bullet_para('Stand up the paid tier (Stripe + Supabase entitlement '
                    'plumbing already scoped; ~2 weeks to launch).', s),
        bullet_para('Triple the instrument coverage (US equities, top-50 '
                    'crypto, more indices) — each new pair = a new audience '
                    'segment.', s),
        bullet_para('Marketing: SEO, content, paid acquisition through the '
                    'free-dashboard funnel. The free product is the lead '
                    'magnet.', s),
        bullet_para('Hire one quant engineer to extend the failure-mode '
                    'panel into a public research engine — convert the '
                    'auditable history into earned authority.', s),

        Paragraph('Why now', s['h2']),
        Paragraph(
            'Retail trader trust in black-box signal services is at a '
            'multi-year low — every prop-firm scandal and Telegram-rug '
            'story compounds it. The market is ready for a verifiable, '
            'open-engine alternative. We have the engine. We are 12 months '
            'into building the auditable history that becomes uncloneable.',
            s['body']),
        Paragraph(
            'The product is live. The math is open. The next 90 days are '
            'about turning that into a subscription business.',
            s['pull']),

        Spacer(1, 12),
        Paragraph(
            '<b>Contact:</b> kmma@vikinginvest.org · vikinginvest.org · '
            'GitHub repo available under NDA',
            s['caption']),
    ]

    doc.build(story, onFirstPage=header_footer, onLaterPages=header_footer)
    print(f'Wrote {out_path}')

if __name__ == '__main__':
    build()
