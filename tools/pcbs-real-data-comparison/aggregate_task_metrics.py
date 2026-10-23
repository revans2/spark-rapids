#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Post-run research reporter. Never launch GPU work or modify benchmark evidence.
All project operations must use MCP docker_exec. Existing Spark behavior unchanged.
"""
import argparse
from collections import defaultdict, Counter
import json
from pathlib import Path
import re

TIME = {'gpuTime','gpuSemaphoreWait','gpuRetryBlockTime','gpuRetryComputationTime',
        'gpuSpillToHostTime','gpuSpillToDiskTime','gpuReadSpillFromHostTime','gpuReadSpillFromDiskTime'}
COUNT = {'gpuRetryCount','gpuSplitAndRetryCount'}
BYTES = {'gpuSpillToHostBytes','gpuSpillToDiskBytes','gpuOutputBatchBytes'}
HIGH = {'gpuMaxDeviceMemoryBytes','gpuMaxHostMemoryBytes','gpuMaxDiskMemoryBytes',
        'gpuMaxPageableMemoryBytes','gpuMaxPinnedMemoryBytes','gpuMaxTaskFootprint',
        'gpuMaxConcurrentGpuTasks','gpuOnGpuTasksWaitingGPUMaxCount'}
METRICS = sorted(TIME | COUNT | BYTES | HIGH)

def parsed(name, raw):
    if raw is None:return None
    if isinstance(raw,(int,float)):
        return {'value':raw,'unit':'ns' if name in TIME else 'count' if name in COUNT or 'Count' in name or name=='gpuMaxConcurrentGpuTasks' else 'bytes','precision':'numeric event update'}
    value=str(raw)
    if name in TIME:
        m=re.fullmatch(r'(\d+):(\d+):(\d+(?:\.\d+)?)',value)
        if m:return {'value':(int(m[1])*3600+int(m[2])*60+float(m[3]))*1e9,'unit':'ns','precision':'rounded to milliseconds in event log'}
    if name in BYTES or name in HIGH:
        m=re.search(r'\((-?\d+) bytes\)',value)
        if m:return {'value':int(m[1]),'unit':'bytes','precision':'exact embedded byte count'}
    if re.fullmatch(r'-?\d+',value):
        return {'value':int(value),'unit':'ns' if name in TIME else 'count' if name in COUNT or 'Count' in name or name=='gpuMaxConcurrentGpuTasks' else 'bytes','precision':'integer event update'}
    return None

def phase_for(start,end,phases):
    if start is None:return {'state':'unknown launch time','phase':None}
    matches=[p for p in phases if p['start_epoch_ms']<=start and
             (p.get('end_epoch_ms') is None or start<=p['end_epoch_ms'])]
    if len(matches)!=1:return {'state':'outside or ambiguous phase','phase':None}
    p=matches[0]
    label={k:p.get(k) for k in ('phase','iteration','consumer_target_mib')}
    if end is None:return {'state':'unfinished attempt; launch attribution only','phase':label}
    if p.get('end_epoch_ms') is None:return {'state':'open failed phase; launch attribution only','phase':label}
    if end>p['end_epoch_ms']:return {'state':'task crosses phase boundary','phase':label}
    return {'state':'fully contained','phase':label}

def summarize_records(records):
    buckets=defaultdict(list)
    for r in records:
        a=r['attribution'];p=a['phase'] or {}
        buckets[(p.get('phase'),p.get('iteration'),p.get('consumer_target_mib'),a['state'],r['attempt_status'])].append(r)
    out=[]
    for key,tasks in buckets.items():
        stats={}
        for name in METRICS:
            vals=[t['metrics'][name]['parsed']['value'] for t in tasks
                  if name in t['metrics'] and t['metrics'][name]['parsed'] is not None]
            stats[name]={'observed_tasks':len(vals),'unknown_tasks':len(tasks)-len(vals),
                         'observed_maximum':max(vals) if vals else None,
                         'observed_sum':sum(vals) if vals and name not in HIGH else None,
                         'aggregation':'maximum across observed task reports; never simultaneous residency' if name in HIGH else 'sum of observed attempt updates only; missing stays unknown',
                         'units':sorted({t['metrics'][name]['parsed']['unit'] for t in tasks if name in t['metrics'] and t['metrics'][name]['parsed'] is not None})}
        logical=Counter((t['stage_id'],t['stage_attempt_id'],t['partition_index']) for t in tasks)
        out.append({'phase':key[0],'iteration':key[1],'consumer_target_mib':key[2],
                    'attribution_state':key[3],'attempt_status':key[4],'attempt_count':len(tasks),
                    'multiple_attempt_partitions':[{'stage_id':k[0],'stage_attempt_id':k[1],'partition_index':k[2],'attempts':n} for k,n in logical.items() if n>1],
                    'metrics':stats})
    return out

def collect(events,phases):
    jobs=[];starts={};ends={};duplicates=0
    for e in events:
        kind=e.get('Event')
        if kind=='SparkListenerJobStart':
            jobs.append({'job_id':e.get('Job ID'),'submission_time':e.get('Submission Time'),
                         'stage_ids':e.get('Stage IDs',[]),'sql_execution_id':(e.get('Properties') or {}).get('spark.sql.execution.id')})
        elif kind in ('SparkListenerTaskStart','SparkListenerTaskEnd'):
            t=e['Task Info'];key=(e['Stage ID'],e.get('Stage Attempt ID',0),t['Task ID'],t.get('Attempt',0))
            if kind=='SparkListenerTaskStart':starts[key]=e;continue
            if key in ends:
                if ends[key]!=e:raise ValueError('Conflicting duplicate TaskEnd: '+str(key))
                duplicates+=1;continue
            ends[key]=e
    records=[]
    for key in sorted(set(starts)|set(ends)):
        e=ends.get(key) or starts[key];t=e['Task Info'];finished=key in ends
        reason=e.get('Task End Reason',{});reason_name=reason.get('Reason') if isinstance(reason,dict) else str(reason)
        status=('SUCCESS' if reason_name=='Success' else 'KILLED' if reason_name=='TaskKilled' else 'FAILED') if finished else 'UNFINISHED'
        metrics={};unknown_names=[]
        for a in t.get('Accumulables',[]):
            name=a.get('Name')
            if name not in METRICS:continue
            if name in metrics:raise ValueError('Duplicate metric name within task: '+name)
            metrics[name]={'raw_update':a.get('Update'),'parsed':parsed(name,a.get('Update')),
                           'accumulator_id':a.get('ID'),'count_failed_values':a.get('Count Failed Values')}
            if metrics[name]['parsed'] is None:unknown_names.append(name)
        launch=t.get('Launch Time');finish=t.get('Finish Time') if finished else None
        attribution=phase_for(launch,finish,phases)
        candidates=[j for j in jobs if key[0] in j['stage_ids']]
        records.append({'stage_id':key[0],'stage_attempt_id':key[1],'task_id':key[2],'attempt_number':key[3],
                        'partition_index':t.get('Index'),'speculative':t.get('Speculative'),
                        'launch_time':launch,'finish_time':finish,'attempt_status':status,
                        'task_end_reason':reason,'attribution':attribution,'job_candidates':candidates,
                        'sql_execution_ids':sorted({str(j['sql_execution_id']) for j in candidates if j['sql_execution_id'] is not None}),
                        'metrics':metrics,'unparsed_metric_names':unknown_names})
    return {'duplicate_identical_taskend_events':duplicates,'tasks':records,'phase_attempt_groups':summarize_records(records),'jobs':jobs}

def phases_from(case,result):
    complete={(p['phase'],p['iteration']):p for p in result.get('phases',[])}
    out=[]
    for line in (case/'run.log').read_text().splitlines():
        if not line.startswith('REAL_CACHE '):continue
        e=json.loads(line[len('REAL_CACHE '):])
        if e.get('event')!='phase_start':continue
        p=complete.get((e['phase'],e['iteration']),{})
        out.append({'phase':e['phase'],'iteration':e['iteration'],
                    'consumer_target_mib':e.get('consumer_target_mib',p.get('consumer_target_mib')),
                    'start_epoch_ms':e.get('epoch_ms',e.get('start_epoch_ms')),
                    'end_epoch_ms':p.get('end_epoch_ms')})
    if any(p['start_epoch_ms'] is None for p in out):raise ValueError('Unknown phase_start clock field; inspect evidence')
    return out

def event_stream(case):
    event_dir = case/'events'
    paths = [p for p in sorted(event_dir.rglob('*'))
             if p.is_file() and not p.name.startswith('.')]
    if not event_dir.is_dir() or not paths:
        raise ValueError('Missing event-log files: '+str(event_dir))
    for path in paths:
        if path.suffix in ('.lz4','.zstd','.snappy'):raise ValueError('Compressed log needs explicit supported reader: '+str(path))
        with path.open() as stream:
            for number,line in enumerate(stream,1):
                try:yield json.loads(line)
                except ValueError:raise ValueError('Incomplete/invalid event line: '+str(path)+':'+str(number))

NOTES=[
 'Post-run reporter, not an independent verifier of benchmark correctness or runtime controls.',
 'TaskEnd Update is the task-local contribution; driver cumulative Value is deliberately ignored.',
 'Successful, failed, killed and unfinished attempts are separate. Successful speculative duplicates remain attempt work, not logical winners.',
 'Phase attribution requires whole-task containment. Crossing and unfinished/open intervals are explicit; out-of-phase setup is retained.',
 'Jobs/SQL execution IDs are stage candidates, not proof each reused stage ran in every job. Use task phase interval as the primary temporal attribution.',
 'Time strings round to milliseconds; summed parsed nanoseconds do not restore lost precision.',
 'Absent or unparseable metric is unknown, never an inferred zero. Observed sums are partial observations, not full-workload bounds.',
 'High-watermarks use maximum across observed reports, never a sum or simultaneous-residency claim; some host reports reflect process-wide pools.',
 'RMM retry counters count registered retry/split throws, not every internal allocation attempt. Successful decoded records do not prove no retries.',
 'Task metrics include all operators in that task, including downstream aggregation. gpuTime is semaphore holding time, not kernel busy time.',
 'RAPIDS spill counters exclude generic Spark cache disk IO and JDK direct-buffer memory.']

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--matrix',type=Path,required=True)
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args();matrix=args.matrix.resolve()
    status=json.loads((matrix/'status.json').read_text())
    if status.get('state')!='RUNS_COMPLETE' or status.get('completed')!=84 or status.get('total')!=84:
        raise SystemExit('Refusing event scan before all 84 primary timing runs complete')
    entries=json.loads((matrix/'results.json').read_text())
    if len(entries)!=84 or len({e['case']['id'] for e in entries})!=84:
        raise SystemExit('Expected 84 distinct result entries')
    args.out.mkdir(parents=True,exist_ok=False)
    index=[]
    for entry in entries:
        name=entry['case']['id'];case=(matrix/name).resolve()
        if case.parent!=matrix:raise ValueError('Case escapes matrix')
        result=entry['result'];phases=phases_from(case,result)
        report=collect(event_stream(case),phases)
        if not report['tasks']:
            raise ValueError('Missing task records in full-data event evidence: '+str(case))
        report.update(case=name,outcome=result.get('outcome'),issues=result.get('issues'),phases=phases,notes=NOTES)
        (args.out/(name+'.json')).write_text(json.dumps(report,indent=2)+'\n')
        index.append({'case':name,'outcome':result.get('outcome'),'tasks':len(report['tasks']),
                      'attribution_counts':dict(Counter(t['attribution']['state'] for t in report['tasks']))})
    (args.out/'index.json').write_text(json.dumps({'notes':NOTES,'cases':index},indent=2)+'\n')
    print(str(args.out/'index.json'))

if __name__=='__main__':main()
