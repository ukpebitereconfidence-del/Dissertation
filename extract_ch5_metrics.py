#!/usr/bin/env python3
"""Extract the Chapter 5 metrics not present in pilot_report.txt."""
import json, glob, statistics as st
from collections import defaultdict

runs = []
for f in sorted(glob.glob('results/*-manifest.json')):
    m = json.load(open(f))
    if m.get('status') != 'ok': continue
    tag = m['tag']; cell = m.get('cell', {})
    r = dict(arm=cell.get('arm'), wl=cell.get('workload'), tier=cell.get('tier'),
             mm=cell.get('mismatched', False), dur=cell.get('duration_s') or 1)
    s = m.get('serverStats') or {}
    cn = s.get('counters', {}) or {}
    r['rss'] = (s.get('memory') or {}).get('rss')
    r['heap'] = (s.get('memory') or {}).get('heapUsed')
    r['cpu'] = (s.get('cpu') or {}).get('cpuPercent')
    r['sent'] = cn.get('messages_sent'); r['bytes'] = cn.get('bytes_sent')
    r['reqs'] = cn.get('http_requests'); r['empty'] = cn.get('http_empty_responses')
    r['uplink'] = cn.get('uplink_requests'); r['connerr'] = cn.get('connection_errors')
    b = s.get('broadcaster') or {}
    r['published'] = b.get('published')
    try:
        p = json.load(open(f'results/{tag}-probe.json'))
        r.update({k: p['latencyMs'].get(k) for k in ('p50','p95','p99','mean')})
    except Exception: pass
    try:
        mt = json.load(open(f'results/{tag}-summary.json')).get('metrics', {})
        g = lambda n,k: (mt.get(n) or {}).get(k)
        r['k6_msgs']=g('rtb_messages_received','count')
        r['k6_polls']=g('rtb_poll_requests','count')
        r['k6_empty']=g('rtb_poll_empty_responses','count')
        r['k6_cf']=g('rtb_connect_failures','count')
        r['k6_conn']=g('rtb_connect_duration_ms','p(95)')
        r['k6_up']=g('rtb_uplink_requests','count')
    except Exception: pass
    runs.append(r)

def agg(field, fmt='{:10.1f}'):
    d = defaultdict(list)
    for r in runs:
        v = r.get(field)
        if v is not None: d[(r['wl'], r['tier'], r['arm'], r['mm'])].append(v)
    print(f'\n=== {field} (mean of replications) ===')
    print(f"{'workload':14s}{'tier':>6s}  {'ws':>11s}{'sse':>11s}{'poll-short':>12s}{'poll-long':>11s}")
    for wl in ('chat','notification','dashboard'):
        for tier in (10,100,1000):
            row = []
            for arm in ('ws','sse','poll-short','poll-long'):
                v = d.get((wl,tier,arm,False))
                row.append(fmt.format(st.mean(v)) if v else f"{'-':>10s}")
            print(f'{wl:14s}{tier:>6d}  {row[0]:>11s}{row[1]:>11s}{row[2]:>12s}{row[3]:>11s}')
    mm = {k:v for k,v in d.items() if k[3]}
    if mm:
        print('  mismatched short-polling cells:')
        for k in sorted(mm): print(f"    {k[0]} c{k[1]}  {fmt.format(st.mean(mm[k]))}")

agg('p99')
agg('rss', '{:10.0f}')
agg('heap', '{:10.0f}')
agg('cpu', '{:10.1f}')

print('\n=== throughput: messages delivered per second (server-side) ===')
d = defaultdict(list)
for r in runs:
    if r.get('sent'): d[(r['wl'],r['tier'],r['arm'],r['mm'])].append(r['sent']/r['dur'])
print(f"{'workload':14s}{'tier':>6s}  {'ws':>11s}{'sse':>11s}{'poll-short':>12s}{'poll-long':>11s}")
for wl in ('chat','notification','dashboard'):
    for tier in (10,100,1000):
        row=[]
        for arm in ('ws','sse','poll-short','poll-long'):
            v=d.get((wl,tier,arm,False)); row.append(f'{st.mean(v):10.0f}' if v else f"{'-':>10s}")
        print(f'{wl:14s}{tier:>6d}  {row[0]:>11s}{row[1]:>11s}{row[2]:>12s}{row[3]:>11s}')

print('\n=== wasted work: empty poll responses as a fraction of poll requests ===')
for r in sorted(runs, key=lambda x:(x['wl'],x['tier'])):
    if r['arm'].startswith('poll') and r.get('k6_polls'):
        frac = (r.get('k6_empty') or 0)/r['k6_polls']
        print(f"  {r['arm']:11s} {r['wl']:13s} c{r['tier']:<5d}{'-mm' if r['mm'] else '   '} "
              f"polls={r['k6_polls']:8d} empty={r.get('k6_empty') or 0:8d} frac={frac:6.3f}")

print('\n=== auxiliary uplink requests (chat only) ===')
for r in sorted(runs, key=lambda x:(x['arm'],x['tier'])):
    if r['wl']=='chat':
        print(f"  {r['arm']:11s} c{r['tier']:<5d} server_uplink={r.get('uplink')} k6_uplink={r.get('k6_up')}")

print('\n=== connection failures and establishment time ===')
tot_cf = sum((r.get('k6_cf') or 0) for r in runs)
print(f'  total connect failures across all runs: {tot_cf}')
print(f'  total connection_errors (server side): {sum((r.get("connerr") or 0) for r in runs)}')
d = defaultdict(list)
for r in runs:
    if r.get('k6_conn') is not None: d[r['arm']].append(r['k6_conn'])
for a in sorted(d): print(f'  {a:11s} p95 connect duration, mean across runs: {st.mean(d[a]):8.2f} ms')

print('\n=== delivery ratio: delivered vs published (server-originated workloads) ===')
for r in sorted(runs, key=lambda x:-(x.get('published') or 0)):
    if r.get('published') and r['wl'] != 'chat' and r['tier']==1000:
        exp = r['published'] * r['tier']
        print(f"  {r['arm']:11s} {r['wl']:13s} published={r['published']:6d} "
              f"delivered={r['sent']:9d} expected={exp:9d} ratio={r['sent']/exp:5.3f}")
