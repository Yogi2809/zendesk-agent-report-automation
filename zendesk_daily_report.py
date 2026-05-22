"""
Zendesk Daily Agent Metrics Report — cars24
Runs on day X, reports data for X-1 (yesterday, IST midnight-to-midnight).
"""

import os
import sys
import json
import time
import threading
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(_dir, "env"))

SUBDOMAIN        = os.getenv("ZENDESK_SUBDOMAIN")
EMAIL            = os.getenv("ZENDESK_EMAIL")
API_TOKEN        = os.getenv("ZENDESK_API_TOKEN")
SLACK_BOT_TOKEN  = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

AUTH     = (f"{EMAIL}/token", API_TOKEN)
BASE_URL = f"https://{SUBDOMAIN}.zendesk.com/api/v2"

# ── Yesterday window in IST (UTC+5:30) ───────────────────────────────────────
IST          = timezone(timedelta(hours=5, minutes=30))
_now_ist     = datetime.now(IST)
_x_minus_1   = _now_ist - timedelta(days=1)

WINDOW_START = datetime(
    _x_minus_1.year, _x_minus_1.month, _x_minus_1.day,
    0, 0, 0, tzinfo=IST
).astimezone(timezone.utc)
WINDOW_END   = WINDOW_START + timedelta(days=1)

REPORT_DATE  = _x_minus_1.strftime("%d %b %Y")
SENT_DATE    = _now_ist.strftime("%d %b %Y")
SEARCH_DATE  = _x_minus_1.strftime("%Y-%m-%d")


def api_get(url, params=None, _retries=3):
    """GET with retry on HTTP 429 (rate limit) and transient network errors."""
    # verify=False: corporate proxy uses a self-signed cert; suppress SSL errors
    for attempt in range(_retries):
        try:
            r = requests.get(url, auth=AUTH, params=params,
                             verify=False, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                print(f"   ⏳ Rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as e:
            print(f"   ⚠️  HTTP error: {e}")
            return {}
        except Exception as e:
            if attempt < _retries - 1:
                print(f"   ⚠️  Request failed (attempt {attempt+1}/{_retries}): {e} — retrying...")
                time.sleep(5)
            else:
                raise RuntimeError(f"API call failed after {_retries} attempts: {url}") from e
    return {}


def fetch_active_zendesk_ids():
    """Return set of user IDs that are currently active agents in Zendesk."""
    active_ids = set()
    url    = f"{BASE_URL}/users.json"
    params = {"role": "agent", "per_page": 100}
    while url:
        data   = api_get(url, params)
        for u in data.get("users", []):
            if u.get("active", False):
                active_ids.add(u["id"])
        url    = data.get("next_page")
        params = {}
    return active_ids


def stream_agent_ticket_ids(agent_id_set):
    """
    Stream ALL ticket events in the IST window via the incremental events API.

    Unlike the search-based pre-filter, this catches every ticket any tracked
    agent touched — including non-comment updates on tickets they no longer own.
    Returns the union set of all such ticket IDs for the audit scan.
    """
    start_unix = int(WINDOW_START.timestamp())
    end_unix   = int(WINDOW_END.timestamp())

    print(f"📥 Streaming ticket events for {REPORT_DATE} (IST)...", flush=True)
    print(f"   UTC window: {WINDOW_START.strftime('%Y-%m-%dT%H:%M:%SZ')} → "
          f"{WINDOW_END.strftime('%Y-%m-%dT%H:%M:%SZ')}", flush=True)

    ticket_ids    = set()
    page          = 0
    agent_matches = 0

    url    = f"{BASE_URL}/incremental/ticket_events.json"
    params = {"start_time": start_unix}

    while url:
        data = api_get(url, params)
        if not data:
            break

        events = data.get("ticket_events", [])
        page  += 1
        done   = False

        for ev in events:
            ts = ev.get("timestamp", 0)
            if ts >= end_unix:
                done = True
                break
            if ts < start_unix:
                continue
            uid = ev.get("updater_id")
            if uid in agent_id_set:
                ticket_ids.add(ev["ticket_id"])
                agent_matches += 1

        print(f"   page {page}: {len(events)} events, "
              f"{agent_matches} agent matches so far", flush=True)

        if done or len(events) < 1000:
            break

        url    = data.get("next_page")
        params = {}

    print(f"   ✅ Stream complete — {len(ticket_ids)} unique tickets to scan.\n",
          flush=True)
    return ticket_ids


def fetch_audit_counts(ticket_ids, agent_id_set):
    """
    Parallel per-ticket audit scan with 10 workers.

    Tracks per-agent, within the IST window:
      updates          — number of audits authored by the agent
      comments         — audits that contain a Comment event
      public_comments  — Comment events with public=True
      internal_comments— Comment events with public=False
      commented_tickets— set of ticket IDs where agent left a comment
      solved_tickets   — set of ticket IDs where agent authored status→solved change

    All signals come from audit author_id + event type, matching Zendesk Explore exactly.
    """
    counts = {
        aid: {"updates": 0, "comments": 0, "public_comments": 0,
              "internal_comments": 0, "commented_tickets": set(),
              "solved_tickets": set()}
        for aid in agent_id_set
    }
    lock      = threading.Lock()
    progress  = {"done": 0}
    total     = len(ticket_ids)

    def scan_one(tid):
        local            = {}
        current_assignee = None   # track through full history (not window-filtered)
        agent_solve_time = {}     # {agent_id: last solve timestamp within window}
        reopen_times     = []     # timestamps when ticket went solved→non-solved in window
        url              = f"{BASE_URL}/tickets/{tid}/audits.json"
        while url:
            data = api_get(url)
            if not data:
                break
            for audit in data.get("audits", []):
                created = datetime.fromisoformat(
                    audit["created_at"].replace("Z", "+00:00"))

                # Track assignee through all history (outside window filter too)
                for ev in audit.get("events", []):
                    if ev.get("type") in ("Create", "Change") \
                            and ev.get("field_name") == "assignee_id":
                        try:
                            current_assignee = int(ev["value"])
                        except (TypeError, ValueError):
                            pass

                if not (WINDOW_START <= created < WINDOW_END):
                    continue

                aid = audit.get("author_id")

                for ev in audit.get("events", []):
                    if ev.get("type") == "Change" and ev.get("field_name") == "status":
                        new_s = ev.get("value", "")
                        old_s = ev.get("previous_value", "")

                        # Rule A: agent authored status → "solved"
                        if new_s == "solved" and aid in agent_id_set:
                            local.setdefault(aid, {
                                "updates": 0, "comments": 0,
                                "public_comments": 0, "internal_comments": 0,
                                "commented_tickets": set(), "solved_tickets": set(),
                            })["solved_tickets"].add(tid)
                            agent_solve_time[aid] = created

                        # Rule B: agent authored status → "closed" directly from
                        # new/pending (skipping solved state entirely).
                        # prev=open and prev=hold excluded — Explore doesn't credit those.
                        elif new_s == "closed" and old_s in ("new", "pending") \
                                and aid in agent_id_set:
                            local.setdefault(aid, {
                                "updates": 0, "comments": 0,
                                "public_comments": 0, "internal_comments": 0,
                                "commented_tickets": set(), "solved_tickets": set(),
                            })["solved_tickets"].add(tid)

                        # Re-open tracking: ticket moved back from solved within window
                        elif old_s == "solved" and new_s in ("open", "new", "pending"):
                            reopen_times.append(created)

                # Updates / comments: always credit the author
                if aid not in agent_id_set:
                    continue
                rec = local.setdefault(aid, {"updates": 0, "comments": 0,
                    "public_comments": 0, "internal_comments": 0,
                    "commented_tickets": set(), "solved_tickets": set()})
                rec["updates"] += 1
                for ev in audit.get("events", []):
                    if ev.get("type") == "Comment":
                        rec["comments"] += 1
                        if ev.get("public", False):
                            rec["public_comments"] += 1
                        else:
                            rec["internal_comments"] += 1
                        rec["commented_tickets"].add(tid)
            url = data.get("next_page")

        # Re-open check: if an agent's solve was followed by a re-open within the
        # same window, Explore won't count it — discard from their solved set.
        for aid, solve_ts in agent_solve_time.items():
            if any(rt > solve_ts for rt in reopen_times):
                if aid in local:
                    local[aid]["solved_tickets"].discard(tid)

        return local

    print(f"🔎 Scanning audits for {total} tickets (10 parallel workers)...", flush=True)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(scan_one, tid): tid for tid in ticket_ids}
        for future in as_completed(futures):
            local = future.result()
            with lock:
                progress["done"] += 1
                if progress["done"] % 100 == 0:
                    print(f"   ... {progress['done']}/{total} tickets scanned", flush=True)
                for aid, rec in local.items():
                    c = counts[aid]
                    c["updates"]           += rec["updates"]
                    c["comments"]          += rec["comments"]
                    c["public_comments"]   += rec["public_comments"]
                    c["internal_comments"] += rec["internal_comments"]
                    c["commented_tickets"] |= rec["commented_tickets"]
                    c["solved_tickets"]    |= rec["solved_tickets"]

    print("   ✅ Audit scan complete.\n", flush=True)
    return counts


def fetch_created_count(agent_id):
    """Tickets created/submitted by agent on SEARCH_DATE (search API, account-tz date)."""
    data = api_get(f"{BASE_URL}/search/count.json",
                   params={"query": f"type:ticket submitter:{agent_id} created:{SEARCH_DATE}"})
    return data.get("count", 0)


def load_agents(active_zendesk_ids):
    """
    Load agents.json, mark each agent as active/inactive based on
    live Zendesk agent list.
    """
    path = os.path.join(_dir, "agents.json")
    with open(path) as f:
        config = json.load(f)
    return [{**a, "active": a["id"] in active_zendesk_ids} for a in config]


def build_all_stats(agents, audit_counts):
    """Combine audit counts + search counts into final per-agent stat dicts."""
    print(f"📊 Assembling stats for {len(agents)} agents...")
    all_stats = []

    for i, agent in enumerate(agents, 1):
        aid = agent["id"]
        ac  = audit_counts.get(aid, {
            "updates": 0, "comments": 0,
            "public_comments": 0, "internal_comments": 0,
            "commented_tickets": set(), "solved_tickets": set(),
        })

        # solved: from audit scan (agent authored status→solved — matches Explore)
        # created: from search API (submitter:{id} created:{date})
        solved  = len(ac["solved_tickets"])
        created = fetch_created_count(aid) if agent["active"] else 0

        all_stats.append({
            "name"              : agent["name"],
            "category"          : agent["category"],
            "band"              : agent["band"],
            "active"            : agent["active"],
            "updates"           : ac["updates"],
            "comments"          : ac["comments"],
            "public_comments"   : ac["public_comments"],
            "internal_comments" : ac["internal_comments"],
            "tickets_w_comment" : len(ac["commented_tickets"]),
            "tickets_solved"    : solved,
            "tickets_created"   : created,
        })
        print(f"   [{i:>2}/{len(agents)}] {agent['name']:<25} "
              f"upd={ac['updates']} cmts={ac['comments']} "
              f"slvd={solved} crtd={created}"
              + (" ⚠ inactive" if not agent["active"] else ""))

    return all_stats


def build_slack_message(all_stats):
    """
    Build the formatted Slack message matching the Zendesk Explore UI columns:
    Updates | Comments | Public comments | Internal comments |
    Tickets updated w/comment | Tickets solved | Tickets created
    """
    W_NUM  = 3
    W_NAME = 22
    W_CAT  = 16
    W_COL  = 6

    def num_row(num, name, cat, upd, cmts, pub, int_, twc, slvd, crtd, flag=""):
        return (
            f"{str(num):<{W_NUM}} "
            f"{name:<{W_NAME}} "
            f"{cat:<{W_CAT}} "
            f"{str(upd):>{W_COL}}"
            f"{str(cmts):>{W_COL}}"
            f"{str(pub):>{W_COL}}"
            f"{str(int_):>{W_COL}}"
            f"{str(twc):>{W_COL}}"
            f"{str(slvd):>{W_COL}}"
            f"{str(crtd):>{W_COL}}"
            + flag
        )

    header = (
        f"{'#':<{W_NUM}} "
        f"{'Agent Name':<{W_NAME}} "
        f"{'Category':<{W_CAT}} "
        f"{'Upd':>{W_COL}}"
        f"{'Cmts':>{W_COL}}"
        f"{'Pub':>{W_COL}}"
        f"{'Int':>{W_COL}}"
        f"{'T.Upd':>{W_COL}}"
        f"{'Slvd':>{W_COL}}"
        f"{'Crtd':>{W_COL}}"
    )

    total_width = W_NUM + 1 + W_NAME + 1 + W_CAT + 1 + W_COL * 7
    divider = "─" * total_width
    thin    = "·" * total_width

    lines = [
        "📊 *cars24 — Daily Agent Report*",
        f"📅 *{REPORT_DATE}*   _|   Sent: {SENT_DATE}_",
        f"👥 *Total Agents: {len(all_stats)}*",
        "```",
        header,
        divider,
    ]

    current_band = None
    row_num      = 0
    band_totals  = {}

    for s in all_stats:
        if s["band"] != current_band:
            if current_band and current_band in band_totals:
                bt = band_totals[current_band]
                lines.append(thin)
                lines.append(num_row(
                    "", f"SUBTOTAL {current_band}", "",
                    bt["u"], bt["c"], bt["p"], bt["i"],
                    bt["t"], bt["s"], bt["cr"]
                ))
                lines.append("")

            current_band = s["band"]
            lines.append(
                f"  ── Band: {current_band} "
                + "─" * max(0, total_width - 12 - len(current_band))
            )
            band_totals[current_band] = {
                "u": 0, "c": 0, "p": 0, "i": 0,
                "t": 0, "s": 0, "cr": 0,
            }

        row_num += 1
        flag = "  ⚠" if not s["active"] else ""
        lines.append(num_row(
            row_num,
            s["name"], s["category"],
            s["updates"], s["comments"],
            s["public_comments"], s["internal_comments"],
            s["tickets_w_comment"], s["tickets_solved"],
            s["tickets_created"], flag
        ))

        bt = band_totals[current_band]
        bt["u"]  += s["updates"]
        bt["c"]  += s["comments"]
        bt["p"]  += s["public_comments"]
        bt["i"]  += s["internal_comments"]
        bt["t"]  += s["tickets_w_comment"]
        bt["s"]  += s["tickets_solved"]
        bt["cr"] += s["tickets_created"]

    if current_band and current_band in band_totals:
        bt = band_totals[current_band]
        lines.append(thin)
        lines.append(num_row(
            "", f"SUBTOTAL {current_band}", "",
            bt["u"], bt["c"], bt["p"], bt["i"],
            bt["t"], bt["s"], bt["cr"]
        ))

    lines.extend([
        divider,
        num_row(
            "", "GRAND TOTAL", "",
            sum(s["updates"]           for s in all_stats),
            sum(s["comments"]          for s in all_stats),
            sum(s["public_comments"]   for s in all_stats),
            sum(s["internal_comments"] for s in all_stats),
            sum(s["tickets_w_comment"] for s in all_stats),
            sum(s["tickets_solved"]    for s in all_stats),
            sum(s["tickets_created"]   for s in all_stats),
        ),
        "```",
        "_Upd=Updates | Cmts=Comments | Pub=Public comments | Int=Internal comments | T.Upd=Tickets w/comment | Slvd=Solved | Crtd=Created_",
        "_⚠ = Agent no longer active in Zendesk_",
    ])

    return "\n".join(lines)


def send_to_slack(message):
    print("\n📤 Sending report to Slack...")
    client = WebClient(token=SLACK_BOT_TOKEN)
    try:
        client.chat_postMessage(
            channel=SLACK_CHANNEL_ID,
            text=message,
            mrkdwn=True,
        )
        print("✅ Report sent successfully!")
    except SlackApiError as e:
        print(f"❌ Slack error: {e.response['error']}")
        raise


def main():
    print("=" * 60)
    print("  cars24 — Zendesk Daily Agent Report")
    print(f"  Reporting date : {REPORT_DATE}")
    print(f"  Triggered on   : {SENT_DATE}")
    print("=" * 60)

    missing = [k for k, v in {
        "ZENDESK_SUBDOMAIN": SUBDOMAIN,
        "ZENDESK_EMAIL":     EMAIL,
        "ZENDESK_API_TOKEN": API_TOKEN,
        "SLACK_BOT_TOKEN":   SLACK_BOT_TOKEN,
        "SLACK_CHANNEL_ID":  SLACK_CHANNEL_ID,
    }.items() if not v]
    if missing:
        print(f"❌ Missing env values: {', '.join(missing)}")
        sys.exit(1)

    active_ids   = fetch_active_zendesk_ids()
    agents       = load_agents(active_ids)
    agent_ids    = {a["id"] for a in agents}

    ticket_ids   = stream_agent_ticket_ids(agent_ids)
    audit_counts = fetch_audit_counts(ticket_ids, agent_ids)
    all_stats    = build_all_stats(agents, audit_counts)

    message = build_slack_message(all_stats)
    print("\n" + "=" * 80)
    print("📋 REPORT PREVIEW:")
    print("=" * 80)
    print(message)
    print("=" * 80 + "\n")

    send_to_slack(message)
    print("\n🎉 Done!")


if __name__ == "__main__":
    main()
