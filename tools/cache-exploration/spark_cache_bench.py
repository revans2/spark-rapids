#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Spark cache population/reuse and storage accounting; launch with spark-submit."""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import re
import time
from urllib.parse import unquote, urlparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F


def fixture(spark, name, rows, partitions):
    frame = spark.range(rows, numPartitions=partitions).selectExpr(
        'id * 17L - 991L as id',
        'case when id % 13 = 0 then cast(null as int) else cast((id * 37) % 1009 as int) end as key',
        'id as row_index')
    if name == 'strings-low':
        frame = frame.withColumn('payload', F.expr(
            "case when row_index % 17 = 0 then cast(null as string) "
            "else concat('group-', cast(row_index % 32 as string)) end"))
    elif name == 'strings-wide':
        # Deterministic varying strings, generated identically for every cache format.
        frame = frame.withColumn('payload', F.expr(
            "case when row_index % 17 = 0 then cast(null as string) "
            "else concat(sha2(cast(row_index as string), 256), '-value-', cast(row_index as string)) end"))
    elif name == 'fixed-wide':
        # Thirty additional longs: predictable, dictionary-friendly and high-entropy values.
        # Keep every expression in Spark SQL so generation remains streamed on CPU or GPU.
        extras = []
        for index in range(10):
            extras.extend([
                "case when (row_index + {i}) % 19 = 0 then cast(null as bigint) "
                "else row_index * {s}L + {b}L end as wide_monotonic_{i:02d}"
                .format(i=index, s=index + 3, b=index * 101),
                "case when (row_index + {i}) % 23 = 0 then cast(null as bigint) "
                "else (row_index + {b}L) % {c}L end as wide_low_{i:02d}"
                .format(i=index, b=index * 7, c=16 + index * 8),
                "case when (row_index + {i}) % 29 = 0 then cast(null as bigint) "
                "else xxhash64(row_index, {s}L) end as wide_hash_{i:02d}"
                .format(i=index, s=1000 + index)])
        frame = frame.selectExpr('*', *extras)
    elif name == 'interval':
        frame = frame.withColumn('interval_value', F.expr(
            'make_interval(cast(row_index % 3 as int), 0, 0, cast(row_index % 11 as int))'))
    return frame.drop('row_index')


def aggregate(frame, narrow):
    columns = [F.count('*').alias('rows'), F.sum('id').alias('sum_id')]
    if not narrow:
        columns.extend([F.sum('key').alias('sum_key'),
                        F.sum(F.col('key').isNull().cast('long')).alias('key_nulls')])
        if 'payload' in list(frame._jdf.columns()):
            columns.extend([F.sum(F.length('payload')).alias('text_characters'),
                            F.sum(F.xxhash64('payload')).alias('text_xxhash_sum'),
                            F.sum(F.col('payload').isNull().cast('long')).alias('text_nulls')])
        wide = [name for name in frame._jdf.columns() if name.startswith('wide_')]
        if wide:
            # One row hash references every extra field, preventing full-read column pruning.
            nulls = sum((F.col(name).isNull().cast('long') for name in wide), F.lit(0))
            columns.extend([F.sum(F.xxhash64(*wide)).alias('wide_xxhash_sum'),
                            F.sum(nulls).alias('wide_nulls')])
        if 'interval_value' in list(frame._jdf.columns()):
            columns.extend([F.sum(F.length(F.col('interval_value').cast('string')))
                            .alias('interval_text_length'),
                            F.sum(F.xxhash64(F.col('interval_value').cast('string')))
                            .alias('interval_text_xxhash_sum')])
    return frame.agg(*columns)


def logical_size(frame):
    rows = frame.count()
    # Canonical Arrow-layout estimate: fixed slots, per-column validity, UTF8 offsets/payload.
    names = list(frame._jdf.columns())
    fixed_slots = 12 + 8 * sum(name.startswith('wide_') for name in names)
    result = fixed_slots * rows + ((rows + 7) // 8) * len(names)
    if 'payload' in list(frame._jdf.columns()):
        payload = frame.agg(F.sum(F.length(F.encode('payload', 'UTF-8')))).first()[0] or 0
        result += 4 * (rows + 1) + payload
    if 'interval_value' in list(frame._jdf.columns()):
        result += 16 * rows
    return result


def validation_frame(frame):
    if 'interval_value' in list(frame._jdf.columns()):
        return frame.withColumn('interval_value', F.col('interval_value').cast('string'))
    return frame


def partition_logical_sizes(frame, total_partitions):
    names = list(frame._jdf.columns())
    payload = F.length(F.encode('payload', 'UTF-8')) if 'payload' in names else F.lit(0)
    grouped = (frame.select(F.spark_partition_id().alias('partition'), payload.alias('payload_bytes'))
               .groupBy('partition').agg(F.count('*').alias('rows'),
                                        F.sum('payload_bytes').alias('payload_bytes')).collect())
    result = {partition: {'rows': 0, 'canonical_partition_bytes_estimate': 0}
              for partition in range(total_partitions)}
    for row in grouped:
        rows = row['rows']
        fixed_slots = 12 + 8 * sum(name.startswith('wide_') for name in names)
        size = fixed_slots * rows + ((rows + 7) // 8) * len(names)
        if 'payload' in names:
            size += 4 * (rows + 1) + (row['payload_bytes'] or 0)
        if 'interval_value' in names:
            size += 16 * rows
        result[row['partition']] = {'rows': rows, 'canonical_partition_bytes_estimate': size}
    return result


def local_blocks(spark, rdd_id, partition_sizes):
    manager = spark.sparkContext._jsc.sc().env().blockManager()
    block_class = spark.sparkContext._jvm.org.apache.spark.storage.RDDBlockId
    blocks = []
    resident_rows = resident_logical = disk_rows = 0
    for partition, logical in sorted(partition_sizes.items()):
        status = manager.getStatus(block_class.apply(rdd_id, partition))
        memory = status.get().memSize() if status.isDefined() else 0
        disk = status.get().diskSize() if status.isDefined() else 0
        blocks.append(dict(partition=partition, resident_bytes=memory, disk_bytes=disk, **logical))
        if memory > 0:
            resident_rows += logical['rows']
            resident_logical += logical['canonical_partition_bytes_estimate']
        elif disk > 0:
            disk_rows += logical['rows']
    return {'blocks': blocks, 'resident_logical_rows': resident_rows,
            'resident_partition_logical_bytes_estimate': resident_logical,
            'disk_only_logical_rows': disk_rows,
            'scope': 'Local BlockManager metadata without a cache-read action. Logical subsets use '
                     'the deterministic source partition mapping, checked by optional post-run audit.'}


def storage_info(spark, rdd_id, partition_sizes):
    """Keep local block accounting consistent; retain the separate RDD report as evidence."""
    blocks = local_blocks(spark, rdd_id, partition_sizes)
    result = {
        'rdd_id': rdd_id,
        'resident_storage_bytes': sum(block['resident_bytes'] for block in blocks['blocks']),
        'disk_storage_bytes': sum(block['disk_bytes'] for block in blocks['blocks']),
        'cached_partitions': sum(block['resident_bytes'] > 0 or block['disk_bytes'] > 0
                                 for block in blocks['blocks']),
        'total_partitions': len(partition_sizes),
        'rdd_reported_storage_info': None,
        'rdd_reported_storage_info_caveat':
            'Separate getRDDStorageInfo observation; its counters may lag local block status. '
            'Primary bytes, cached partitions and logical subsets all use the same sequential '
            'local block observations, not these separately reported counters.',
        **blocks}
    infos = spark.sparkContext._jsc.sc().getRDDStorageInfo()
    for info in infos:
        if info.id() == rdd_id:
            result['rdd_reported_storage_info'] = {
                'resident_storage_bytes': info.memSize(), 'disk_storage_bytes': info.diskSize(),
                'cached_partitions': info.numCachedPartitions(), 'total_partitions': info.numPartitions(),
                'storage_level': info.storageLevel().description()}
            break
    return result


def heap(spark):
    usage = spark.sparkContext._jvm.java.lang.management.ManagementFactory.getMemoryMXBean() \
        .getHeapMemoryUsage()
    return {'used_bytes': usage.getUsed(), 'committed_bytes': usage.getCommitted(),
            'max_bytes': usage.getMax()}


def cached_plan_metrics(plan):
    """Read existing producer SQL counters; this does not execute or traverse cached rows."""
    def node(current):
        metrics = {}
        entries = current.metrics().iterator()
        while entries.hasNext():
            entry = entries.next()
            metric = entry._2()
            name = metric.name()
            metrics[entry._1()] = {
                'name': name.get() if name.isDefined() else None,
                'metric_type': metric.metricType(), 'value': metric.value()}
        children = []
        inputs = current.children().iterator()
        while inputs.hasNext():
            children.append(node(inputs.next()))
        return {'node_name': current.nodeName(), 'node_id': current.id(),
                'class': current.getClass().getName(), 'metrics': metrics, 'children': children}
    try:
        return {'tree': node(plan),
                'scope': 'Producer SQL metric snapshot after population, aggregated across tasks. '
                         'Only counters exposed by this executed plan are reported. Missing/zero '
                         'batch counters do not establish batch counts or per-task distribution.'}
    except Exception as error:
        return {'unavailable': repr(error)}


def plugin_memory(spark):
    if 'com.nvidia.spark.SQLPlugin' not in spark.sparkContext.getConf().get('spark.plugins', ''):
        return {'scope': 'Plugin not loaded; native GPU APIs not queried'}
    try:
        rmm = spark.sparkContext._jvm.ai.rapids.cudf.Rmm
        if not rmm.isInitialized():
            return {'rmm_initialized': False}
        tracked = rmm.getTracker() is not None
        return {'rmm_initialized': True, 'rmm_tracking_available': tracked,
                'rmm_tracked_live_bytes': rmm.getTotalBytesAllocated() if tracked else None,
                'rmm_tracked_high_water_bytes': rmm.getMaximumTotalBytesAllocated() if tracked else None,
                'managed_host_usage_ratio': spark.sparkContext._jvm.com.nvidia.spark.rapids
                .HostAlloc.getUsageRatio(),
                'managed_host_data_limit_bytes': spark.sparkContext._jvm.ai.rapids.cudf
                .DefaultHostMemoryAllocator.get().getTotalLimit(),
                'pinned_pool_bytes': spark.sparkContext._jvm.ai.rapids.cudf
                .PinnedMemoryPool.getTotalPoolSizeBytes(),
                'gpu_pool_bytes': rmm.getPoolSize(),
                'scope': 'RMM high water is cumulative for this JVM, not a per-action peak. '
                         'Unused pool reservation and allocations outside these allocators are excluded.'}
    except Exception as error:
        return {'unavailable': repr(error)}


def prototype_counters(spark, cache_format):
    if cache_format != 'nvcomp':
        return None
    values = spark.sparkContext._jvm.org.apache.spark.sql.cacheexploration \
        .SparkNvcompSerializer.snapshot()
    return dict(zip(['encoded_groups', 'decoded_groups', 'successful_group_encode_calls',
                     'successful_group_decode_calls', 'concatenations', 'output_batches',
                     'output_rows', 'output_device_bytes', 'max_output_device_bytes'], list(values)))


@contextmanager
def profile_phase(spark, enabled, name, cpu_directory=None, cpu_phases=None):
    """Action-scoped diagnostics; setup/teardown are outside the collect timer."""
    marker = None
    sampler = None
    record = None
    try:
        if cpu_directory is not None:
            sampler = spark.sparkContext._jvm.org.apache.spark.sql.cacheexploration.CpuPhaseProfiler
            path = cpu_directory / (name.replace('/', '-') + '.jfr')
            record = {'name': name, 'file': str(path),
                      'profiler': json.loads(sampler.start(str(path)))}
        if enabled:
            marker = spark.sparkContext._jvm.ai.rapids.cudf.NvtxUniqueRange(
                'cache-exploration/' + name, 0xFF00AAFF - 2**32)  # signed Java ARGB int
        if sampler is not None:
            sampler.begin()
        yield
    finally:
        try:
            if marker is not None:
                marker.close()
            if record is not None:
                record.update(json.loads(sampler.finish()))
                cpu_phases.append(record)
        finally:
            if record is not None:
                sampler.stop()


def check_runtime_origins(spark, plugin_jar, jni_jar):
    """Verify class loading, rather than infer it from submitted jar arguments."""
    loader = spark.sparkContext._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    expected = {
        'com.nvidia.spark.ParquetCachedBatchSerializer': plugin_jar,
        'com.nvidia.spark.rapids.parquet.ParquetCachedBatchSerializer': plugin_jar,
        'com.nvidia.spark.rapids.parquet.ParquetCachedBatchReader': plugin_jar,
        'com.nvidia.spark.GpuCachedBatchSerializer': plugin_jar,
        'ai.rapids.cudf.Table': jni_jar,
        'ai.rapids.cudf.ParquetChunkedReader': jni_jar}
    observed = {}
    for name, path in expected.items():
        source = loader.loadClass(name).getProtectionDomain().getCodeSource()
        if source is None:
            raise AssertionError('Class has no verifiable origin: ' + name)
        location = str(source.getLocation().toExternalForm())
        url = urlparse(location)
        if url.scheme != 'file' or Path(unquote(url.path)).resolve() != path.resolve():
            raise AssertionError('Unexpected runtime origin for ' + name + ': ' + location)
        observed[name] = location
    return observed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--format', choices=['default', 'pcbs', 'vortex', 'nvcomp'], required=True)
    parser.add_argument('--serializer-class',
                        default='org.apache.spark.sql.cacheexploration.SparkNvcompCascadedSerializer')
    parser.add_argument('--vortex-class', default='org.apache.spark.sql.cacheexploration.SparkVortexSerializer')
    parser.add_argument('--write-mode', choices=['cpu', 'gpu'], default='cpu')
    parser.add_argument('--read-modes', default='cpu,gpu')
    parser.add_argument('--fixture', choices=['fixed', 'fixed-wide', 'strings-low', 'strings-wide', 'interval'],
                        default='fixed')
    parser.add_argument('--rows', type=int, default=65536)
    parser.add_argument('--partitions', type=int, default=4)
    parser.add_argument('--task-size-mib', type=int, choices=[256, 512, 1024, 2048],
                        help='Approved fixed-wide logical bytes per task; requires exactly two waves')
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--storage', choices=['dataframe-default', 'serialized-memory-and-disk',
                                             'serialized-memory-only'], default='dataframe-default')
    parser.add_argument('--warmups', type=int, default=2)
    parser.add_argument('--payload-audit-class',
                        help='Optional tools helper with summarize(DataFrame); runs after all measurements')
    parser.add_argument('--expected-plugin-jar', type=Path)
    parser.add_argument('--expected-jni-jar', type=Path)
    parser.add_argument('--cpu-profile-dir', type=Path,
                        help='Separate action-scoped async-profiler CPU/wall recordings')
    parser.add_argument('--nvtx-phases', action='store_true',
                        help='Mark timed actions for a separate Nsight profiling run')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    read_modes = args.read_modes.split(',')
    if args.rows < 1 or args.partitions < 1 or args.repeats < 1 or args.warmups < 0 \
            or not read_modes or any(mode not in ['cpu', 'gpu'] for mode in read_modes):
        parser.error('positive rows/partitions/repeats and cpu/gpu read modes required')
    if args.output.exists():
        parser.error('output file already exists')
    if args.cpu_profile_dir is not None:
        args.cpu_profile_dir = args.cpu_profile_dir.resolve()
        args.cpu_profile_dir.mkdir(parents=True, exist_ok=False)
    serializers = {'default': 'org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer',
                   'pcbs': 'com.nvidia.spark.ParquetCachedBatchSerializer',
                   'vortex': args.vortex_class, 'nvcomp': args.serializer_class}
    spark = (SparkSession.builder.appName('Cache format exploration')
             .config('spark.sql.cache.serializer', serializers[args.format])
             .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    result = {'arguments': vars(args).copy(), 'spark_version': spark.version,
              'serializer': serializers[args.format], 'reads': [], 'cpu_phases': [],
              'scope': 'Local Spark cache job; resident bytes are BlockManager storage accounting. '
                       'Heap snapshots are not temporary-memory peaks. Serializer size is a '
                       'separate reported statistic, not an estimate of total process memory. '
                       'Large-data correctness uses deterministic aggregates; small data checks '
                       'also compare every row. No automatic execution-mode claim from config.'}
    frame = None
    try:
        if spark.sparkContext.master != 'local' and not spark.sparkContext.master.startswith('local['):
            raise ValueError('This initial storage/heap runner supports local Spark execution only')
        result['master'] = spark.sparkContext.master
        if (args.expected_plugin_jar is None) != (args.expected_jni_jar is None):
            raise ValueError('Runtime origin checks require both plugin and JNI jars')
        if args.expected_plugin_jar is not None:
            result['runtime_class_origins'] = check_runtime_origins(
                spark, args.expected_plugin_jar, args.expected_jni_jar)
        result['effective_cache_compression_enabled'] = spark.conf.get(
            'spark.sql.inMemoryColumnarStorage.compressed')
        if args.nvtx_phases and spark.sparkContext._jvm.java.lang.System.getProperty(
                'ai.rapids.cudf.nvtx.enabled') != 'true':
            raise ValueError('NVTX phase tracing requires JVM ai.rapids.cudf.nvtx.enabled=true')
        if args.task_size_mib is not None:
            match = re.fullmatch(r'local\[(\d+)(?:,\d+)?\]', spark.sparkContext.master)
            if match is None:
                raise ValueError('Task-size measurements require an explicit local task count')
            task_slots = int(match.group(1))
            target_bytes = args.task_size_mib * 1024 * 1024
            rows_per_task = target_bytes // 256
            if args.fixture != 'fixed-wide' or args.rows != rows_per_task * args.partitions:
                raise ValueError('Task-size rows must exactly match fixed-wide 256-byte logical rows')
            if args.partitions != 2 * task_slots:
                raise ValueError('Task-size measurements require partitions == 2 * local task count')
            result['task_size_model'] = {
                'target_logical_bytes_per_task': target_bytes,
                'rows_per_task': rows_per_task, 'logical_bytes_per_row': 256,
                'concurrent_local_tasks': task_slots, 'task_waves': 2,
                'scope': 'Canonical logical size, not GPU batch size or resident memory'}

        gpu_requested = args.write_mode == 'gpu' or 'gpu' in read_modes
        if gpu_requested and 'com.nvidia.spark.SQLPlugin' not in spark.sparkContext.getConf().get('spark.plugins', ''):
            raise ValueError('GPU modes require the RAPIDS Spark plugin to be loaded')
        spark.sparkContext.setLogLevel('WARN')
        result['spark_conf'] = dict(spark.sparkContext.getConf().getAll())
        # Reference actions must not pre-plan the DataFrame later used to populate GPU cache.
        spark.conf.set('spark.rapids.sql.enabled', 'false')
        reference = fixture(spark, args.fixture, args.rows, args.partitions)
        expected = {str(narrow): aggregate(reference, narrow).first().asDict()
                    for narrow in [False, True]}
        expected_rows = validation_frame(reference).orderBy('id').collect() if args.rows <= 4096 else None
        result['logical_bytes_canonical_estimate'] = logical_size(reference)
        partition_sizes = partition_logical_sizes(reference, args.partitions)
        result['source_partition_logical_sizes'] = partition_sizes
        if args.task_size_mib is not None:
            target_bytes = args.task_size_mib * 1024 * 1024
            if any(value['canonical_partition_bytes_estimate'] != target_bytes
                   for value in partition_sizes.values()):
                raise AssertionError('Source partition logical bytes differ from requested task size')
        result['expected_aggregates'] = expected
        spark.conf.set('spark.rapids.sql.enabled', str(args.write_mode == 'gpu').lower())
        frame = fixture(spark, args.fixture, args.rows, args.partitions)
        storage = {'dataframe-default': StorageLevel.MEMORY_AND_DISK_DESER,
                   'serialized-memory-and-disk': StorageLevel.MEMORY_AND_DISK,
                   'serialized-memory-only': StorageLevel.MEMORY_ONLY}[args.storage]
        frame.persist(storage)
        cached = spark._jsparkSession.sharedState().cacheManager().lookupCachedData(frame._jdf)
        if cached.isEmpty():
            raise AssertionError('cache entry not registered')
        cache = cached.get().cachedRepresentation().cacheBuilder()
        result['cached_plan'] = cache.cachedPlan().toString()
        result['driver_heap_before_population'] = heap(spark)
        result['plugin_memory_before_population'] = plugin_memory(spark)
        population = frame.agg(F.count('*').alias('rows'))
        with profile_phase(spark, args.nvtx_phases, 'population',
                           args.cpu_profile_dir, result['cpu_phases']):
            started = time.perf_counter_ns()
            observed_rows = population.collect()[0]['rows']
            result['population_ns'] = time.perf_counter_ns() - started
        result['population_action_final_plan'] = population._jdf.queryExecution().executedPlan().toString()
        result['cached_plan_metrics_after_population'] = cached_plan_metrics(cache.cachedPlan())
        if observed_rows != args.rows:
            raise AssertionError('population row count')
        rdd_id = cache.cachedColumnBuffers().id()
        result['serializer_reported_size_bytes_after_population'] = cache.sizeInBytesStats().value()
        result['serializer_reported_rows_after_population'] = cache.rowCountStats().value()
        if result['serializer_reported_rows_after_population'] != args.rows:
            raise AssertionError('cache population accumulator does not match input rows')
        if cache.cachedColumnBuffers().getNumPartitions() != args.partitions:
            raise AssertionError('cache partitioning differs from source fixture')
        result['storage_after_population'] = storage_info(spark, rdd_id, partition_sizes)
        result['driver_heap_after_population'] = heap(spark)
        result['plugin_memory_after_population'] = plugin_memory(spark)
        result['prototype_after_population'] = prototype_counters(spark, args.format)
        print(json.dumps({'phase': 'population', 'elapsed_ns': result['population_ns'],
                          'rows': observed_rows}), flush=True)
        for mode in read_modes:
            spark.conf.set('spark.rapids.sql.enabled', str(mode == 'gpu').lower())
            for narrow in [False, True]:
                for iteration in range(-args.warmups, args.repeats):
                    query = aggregate(frame, narrow)
                    phase = 'read/{}/{}/{}'.format(
                        mode, 'id-only' if narrow else 'full', iteration)
                    with profile_phase(spark, args.nvtx_phases, phase,
                                       args.cpu_profile_dir, result['cpu_phases']):
                        started = time.perf_counter_ns()
                        observed = query.collect()[0].asDict()
                        elapsed = time.perf_counter_ns() - started
                    if observed != expected[str(narrow)]:
                        raise AssertionError('cache aggregate mismatch: ' + repr(observed))
                    plan = query._jdf.queryExecution().executedPlan().toString()
                    read_plan = plan.split('InMemoryRelation', 1)[0]
                    reader_backend = ('gpu' if 'GpuInMemoryTableScan' in read_plan
                                      else 'cpu' if 'InMemoryTableScan' in read_plan else 'unknown')
                    result['reads'].append({'requested_mode': mode, 'narrow': narrow,
                                            'observed_cache_reader': reader_backend,
                                            'has_cpu_expression_bridge': 'GpuCpuBridge' in read_plan,
                                            'warmup': iteration < 0,
                                            'iteration': iteration, 'elapsed_ns': elapsed,
                                            'plan': plan, 'aggregate': observed,
                                            'storage': storage_info(spark, rdd_id, partition_sizes),
                                            'driver_heap_after_read': heap(spark),
                                            'plugin_memory_after_read': plugin_memory(spark),
                                            'prototype_counters': prototype_counters(spark, args.format)})
                    print(json.dumps({'phase': 'read', 'requested_mode': mode,
                                      'narrow': narrow, 'warmup': iteration < 0,
                                      'iteration': iteration, 'elapsed_ns': elapsed,
                                      'observed_cache_reader': reader_backend,
                                      'aggregate_checked': True}), flush=True)
            if expected_rows is not None:
                if validation_frame(frame).orderBy('id').collect() != expected_rows:
                    raise AssertionError('full row comparison failed for ' + mode)
        result['storage_after_reads'] = storage_info(spark, rdd_id, partition_sizes)
        result['prototype_after_reads'] = prototype_counters(spark, args.format)
        result['serializer_reported_size_bytes_after_reads'] = cache.sizeInBytesStats().value()
        result['serializer_reported_rows_after_reads'] = cache.rowCountStats().value()
        result['size_statistic_caveat'] = ('Serializer-defined, not comparable encoded bytes: default '
            'Spark reports uncompressed column stats; PCBS reports encoded arrays. '
            'Accumulators can increase when partitions are recomputed, so are not retained-data counts.')
        if args.payload_audit_class:
            helper = spark.sparkContext._jvm
            for component in args.payload_audit_class.split('.'):
                helper = getattr(helper, component)
            result['post_run_payload_audit'] = json.loads(helper.summarize(frame._jdf))
            for partition in result['post_run_payload_audit']['partitions']:
                if partition['rows'] != partition_sizes.get(partition['partition'], {}).get('rows', 0):
                    raise AssertionError('cache/source logical partition mapping differs')
            result['post_run_audit_caveat'] = ('Separate metadata action after timing/storage snapshots; '
                'may reread/recompute blocks and change residency. Its resource use is not a capacity result.')
        result['status'] = 'PASS'
    except Exception as error:
        result['status'] = 'FAIL'
        result['error'] = repr(error)
        raise
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, default=str) + '\n')
        try:
            if frame is not None:
                frame.unpersist(blocking=True)
        finally:
            spark.stop()


if __name__ == '__main__':
    main()
