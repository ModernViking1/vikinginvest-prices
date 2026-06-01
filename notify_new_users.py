"""Poll Supabase auth.users and Telegram-alert on new registrations.

Runs on a cron schedule via the GitHub Actions workflow at
.github/workflows/notify-new-users.yml. Compares the current Supabase
user list against notified-users.json (committed to the repo by the
workflow); sends a Telegram message for every user not previously
notified, then updates the state file.

Requires three repo secrets:
  SUPABASE_URL                 e.g. https://opwdsuusdmsaicoyqxti.supabase.co
  SUPABASE_SERVICE_ROLE_KEY    Auth admin key (Project Settings -> API)
  TELEGRAM_BOT_TOKEN           Existing bot, same as detect_triggers.py uses
  TELEGRAM_CHAT_ID             Existing admin chat id

The service-role key gives full DB access — keep it in repo secrets,
never log it, never echo it.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

STATE_PATH = 'notified-users.json'
PAGE_SIZE = 200  # Supabase max per page


def _env(name: str) -> str:
    v = os.environ.get(name, '').strip()
    if not v:
        print(f'FATAL: missing env {name}', flush=True)
        sys.exit(1)
    return v


def load_state() -> dict:
    try:
        with open(STATE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {'notified_ids': [], 'last_run_at': None}


def save_state(state: dict) -> None:
    with open(STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2, sort_keys=True)
        f.write('\n')


def fetch_all_users(supabase_url: str, service_key: str) -> list[dict]:
    """Page through Supabase Auth Admin API users endpoint."""
    out: list[dict] = []
    page = 1
    while True:
        qs = urllib.parse.urlencode({'page': page, 'per_page': PAGE_SIZE})
        url = f'{supabase_url.rstrip("/")}/auth/v1/admin/users?{qs}'
        req = urllib.request.Request(
            url,
            headers={
                'Authorization': f'Bearer {service_key}',
                'apikey': service_key,
                'Accept': 'application/json',
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            print(f'FATAL: Supabase HTTP {e.code} on page {page}: {e.read().decode("utf-8", "replace")}', flush=True)
            sys.exit(1)

        users = body.get('users', [])
        if not users:
            break
        out.extend(users)
        if len(users) < PAGE_SIZE:
            break
        page += 1
        if page > 50:  # defensive runaway guard
            print('WARN: stopped paginating at page 50', flush=True)
            break
    return out


def send_telegram(token: str, chat_id: str, text: str) -> bool:
    url = f'https://api.telegram.org/bot{token}/sendMessage'
    data = urllib.parse.urlencode({
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'Markdown',
        'disable_web_page_preview': 'true',
    }).encode('utf-8')
    req = urllib.request.Request(url, data=data)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
        return True
    except Exception as e:  # noqa: BLE001
        print(f'WARN: telegram send failed: {e}', flush=True)
        return False


def format_new_user_message(user: dict) -> str:
    email = user.get('email') or '(no email)'
    user_id = user.get('id', '')
    created_at = user.get('created_at', '')
    meta = user.get('user_metadata') or user.get('raw_user_meta_data') or {}
    tg = meta.get('telegram_id') or '_not provided_'

    # Try to compactly format created_at
    try:
        dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        created = dt.strftime('%Y-%m-%d %H:%M UTC')
    except Exception:  # noqa: BLE001
        created = created_at

    return (
        '\U0001F195 *New Viking Invest signup*\n'
        f'`{email}`\n'
        f'Telegram ID: `{tg}`\n'
        f'Signed up: {created}\n'
        f'User ID: `{user_id}`'
    )


def main() -> int:
    supabase_url = _env('SUPABASE_URL')
    service_key = _env('SUPABASE_SERVICE_ROLE_KEY')
    telegram_token = _env('TELEGRAM_BOT_TOKEN')
    telegram_chat = _env('TELEGRAM_CHAT_ID')

    state = load_state()
    notified: set[str] = set(state.get('notified_ids', []))
    users = fetch_all_users(supabase_url, service_key)
    print(f'Fetched {len(users)} users; {len(notified)} previously notified', flush=True)

    # On first run, treat ALL existing users as already-notified so we don't
    # spam the chat with a backfill. After the first run state file is
    # committed, only genuinely new signups will trigger an alert.
    is_first_run = state.get('last_run_at') is None and not notified
    if is_first_run:
        print('First run — baselining existing users as already-notified', flush=True)
        notified.update(u['id'] for u in users if u.get('id'))
        state['notified_ids'] = sorted(notified)
        state['last_run_at'] = datetime.utcnow().isoformat() + 'Z'
        save_state(state)
        return 0

    new_users = [u for u in users if u.get('id') and u['id'] not in notified]
    print(f'{len(new_users)} new user(s) to notify', flush=True)

    sent = 0
    for u in new_users:
        msg = format_new_user_message(u)
        if send_telegram(telegram_token, telegram_chat, msg):
            sent += 1
            notified.add(u['id'])

    if sent:
        state['notified_ids'] = sorted(notified)
        state['last_run_at'] = datetime.utcnow().isoformat() + 'Z'
        save_state(state)
        print(f'Sent {sent} Telegram alerts and updated state', flush=True)
    else:
        # Still bump last_run_at so we can tell the cron is alive.
        state['last_run_at'] = datetime.utcnow().isoformat() + 'Z'
        save_state(state)
        print('No new users this run', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
