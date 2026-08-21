#!/usr/bin/env python3
"""Where are the 523 connection failures, and do the SSE chat cells confirm the defect?"""
import json, glob
from collections import defaultdict

print("=== connection failures by cell ===")
tot = 0
rows = []
for f in sorted(glob.glob('results/*-manifest.json')):
    m = json.load(open(f))
    if m.get('status') != 'ok': continue
    tag = m['tag']; cell = m.get('cell', {})
    try:
        mt = json.load(open(f'results/{tag}-summary.json')).get('metrics', {})
        cf = (mt.get('rtb_connect_failures') or {}).get('count') or 0
        er = (mt.get('rtb_delivery_errors') or {}).get('rate')
        rc = (mt.get('rtb_messages_received') or {}).get('count') or 0
    except Exception:
        continue
    tot += cf
    if cf: rows.append((cf, tag, cell.get('arm'), cell.get('tier'), er, rc))
rows.sort(reverse=True)
for cf, tag, arm, tier, er, rc in rows:
    print(f"  {cf:5d} failures  {tag:42s} tier={tier:<5} err_rate={er} msgs={rc}")
print(f"  TOTAL: {tot}")

print("\n=== per-arm and per-tier totals ===")
by_arm = defaultdict(int); by_tier = defaultdict(int)
for cf, tag, arm, tier, er, rc in rows:
    by_arm[arm] += cf; by_tier[tier] += cf
for k in sorted(by_arm): print(f"  arm  {k:12s} {by_arm[k]:6d}")
for k in sorted(by_tier): print(f"  tier {k:<12} {by_tier[k]:6d}")

print("\n=== SSE chat: uplink vs messages, per run ===")
for f in sorted(glob.glob('results/sse-chat-*-manifest.json')):
    m = json.load(open(f)); tag = m['tag']
    c = (m.get('serverStats') or {}).get('counters', {})
    print(f"  {tag:28s} ingress={c.get('ingress_messages')} uplink={c.get('uplink_requests')} "
          f"sent={c.get('messages_sent')} active={(m.get('serverStats') or {}).get('gauges',{}).get('connections_active')}")

print("\n=== ws chat, for comparison ===")
for f in sorted(glob.glob('results/ws-chat-*-manifest.json')):
    m = json.load(open(f)); tag = m['tag']
    c = (m.get('serverStats') or {}).get('counters', {})
    print(f"  {tag:28s} ingress={c.get('ingress_messages')} sent={c.get('messages_sent')}")
