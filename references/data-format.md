# Spark payload format and analysis semantics

## Where the data lives

- Viewer page 'https://spark.lucko.me/<code>' is a Next.js shell; it fetches the payload from
  'https://bytebin.lucko.me/<code>'.
- 'https://spark.lucko.me/<code>?raw=1' is not the payload: it returns a small JSON metadata
  document ('{"type":"sampler","metadata":{...}}') with no call tree. The analyzer detects that
  shape, prints its statistics for 'probe', and refuses the tree commands with a hint to use the
  bytebin URL.
- The payload is a protobuf message, optionally gzip-wrapped ('1f 8b' magic). Local '.spark'
  files from '/spark sampler --save-to-file' use the same encoding.
- Top-level message depends on how the report was produced:
  - 'SamplerData' (field 1 = SamplerMetadata, field 2 = threads) - has a call tree;
  - 'HealthData' (field 1 = HealthMetadata, field 2 = map<int32, WindowStatistics>);
  - 'HeapData' (field 1 = HeapMetadata, field 2 = repeated HeapEntry).
  The analyzer distinguishes them by decoding field 1 and inspecting field 2.

## SamplerData layout

| field | name | notes |
|---|---|---|
| 1 | metadata | see below |
| 2 | threads | repeated ThreadNode; one per dumped thread (or per pool when grouped) |
| 3 | class_sources | map<class name, mod id> - the basis of mod attribution |
| 4, 5 | method_sources, line_sources | optional, usually absent |
| 6 | time_windows | repeated int32, window identifiers |
| 7 | time_window_statistics | map<int32, WindowStatistics> - ticks, tps, mspt, players, entities, chunks |
| 8 | channel_info | viewer socket channel |

SamplerMetadata carries creator, start/end time, sampling interval in microseconds, thread
dumper, data aggregator, platform metadata/statistics, system statistics, server
configurations, sources (mod list), and - in newer versions - a Metrics series (TPS, tick
duration, CPU, heap, world info over time) and sampler mode/engine.

## The call tree

'ThreadNode' holds a flat array 'children' of 'StackTraceNode' plus 'times' (per window) and
'children_refs'. Every 'StackTraceNode.children_refs' indexes that same flat array, so the tree
is a trie over stack frames with the node objects pooled:

- 'times[i]' = time attributed to that frame in window i, in milliseconds;
- root 'times' sum = total sampled time for that thread (250 Hz x 4 ms = wall time);
- identical stack prefixes merge into one node, so a node's inclusive time is the sum over all
  occurrences of that stack path.
- 'analyze' walks every thread in the payload by default. Targeted commands accept `--thread NAME|INDEX`;
  without it they use the first thread only for backward compatibility, and the report must state that
  choice. Never treat a single-thread result as global when other threads exist.

Self time is not stored. Compute it per window as inclusive(node) minus the sum of the
inclusive times of its children. It describes sampled wall-clock time on the sampled thread,
not CPU cycles. A tiny negative value can appear from float rounding; the analyzer clamps it to
zero and counts every clamp in the summary's validation block (and in 'analysis.json' under 'issues').

## Aggregator and window semantics

- 'DataAggregator.type' is SIMPLE or TICKED. TICKED also carries 'tick_length_threshold'
  (microseconds) and 'number_of_included_ticks'.
- With TICKED, only ticks longer than the threshold contribute samples. The analyzer prints
  'included ticks N, of M total ticks (P%)', so the comparison is read off the probe output
  directly: P below 100% means the profile covers slow ticks, not every tick.
- 'thread_grouper' is BY_NAME, BY_POOL, or AS_ONE; it decides whether thread nodes represent
  individual threads or merged groups.
- Windows are typically one minute in long reports; each node's 'times' array is indexed by
  window, which is what makes per-window trend tables possible.

## Attribution model

'class_sources' maps sampled class names to a mod id (2454 distinct classes over 176 mods in a
354-mod pack, as measured on one real payload - the counts vary per report). Classes absent from
the map are vanilla ('net.minecraft.*'), loader, JDK, or library code. Three derived views are
used:

1. **Owner**: walk from a node upward until a class with a mod id is found (including the node
   itself); if none, classify by package as vanilla or JDK/library. Answers "whose code ran".
2. **Folded owner**: same walk but skipping a configurable list of library mods, so their time
   is credited to the mod that requested it. Answers "who caused it".
3. **Unmapped**: classes with no mod id that are neither vanilla, a JVM/runtime namespace, nor a
  known public/platform namespace
  ('java|javax|jdk|sun|com.sun|kotlin|scala|groovy|org.graalvm|org.slf4j|ch.qos|org.apache.logging'),
   i.e. code whose mod id spark failed to record. The summary prints the self time involved plus
   the package roots, and flags any root that fuzzy-matches a mod name declared in the payload's
   own 'sources' list as that mod's unmapped code (a name matches when it is equal, or a common
   prefix of at least 6 characters covering >=75% of the shorter name). Such a mod's cost is
   otherwise billed to whatever mapped mod sits above it, so read the owner tables as "where the
   code ran", not "who wrote it". Classes mapped to a JVM namespace id (proxy classes such as
  'jdk.proxy3' or 'com.sun.proxy.$Proxy12') are skipped, so they never show up as a mod.

Known public/platform namespaces such as fastutil, Guava, NeoForge bus, and Netty are reported
separately as neutral context; they are not evidence of a missing mod. Truly unmapped classes still
need their nearest mapped caller and a confidence label.

Neither view is additive with the others, and inclusive family totals can exceed 100% because a
class appears in many stack paths; only self-time aggregation is comparable across rows. Rows in
the unmapped table can nest inside each other as well, so its inclusive column overlaps.

## Sanity checks

The analyzer runs these itself and prints them in the summary's validation block (the same
numbers are in 'analysis.json' under 'issues'); a failing thread is flagged with
'** DECODING MAY BE WRONG ... **':

- sum(self over all nodes) == root inclusive total, printed as 'sum(self) - root = <delta ms>'
  (a mismatch means a decoding problem);
- every 'children_refs' index must be < the flat array length ('refs out of range');
- no edge should be referenced twice and no cycle should be reachable from the roots
  ('repeated refs' / 'cycle refs');
- every node must be reachable from the root ('unreachable');
- self time is clamped at zero, and the number of clamps is reported ('negative self').

Any non-zero structural issue is a hard stop for attribution and repair ranking. Re-fetch or
request the raw payload; do not publish a guessed root cause.

## Versioning

The schema implemented in 'scripts/spark.mjs' matches spark's 'spark.proto',
'spark_sampler.proto', 'spark_heap.proto' and 'spark_ws.proto'. Newer spark builds add fields
(metrics, sampler mode/engine); unknown fields are skipped safely, and the probe reports
'spark_data_version' when present. If a future payload fails to decode, fetch the current
protos from 'lucko/spark' ('spark-common/src/main/proto/spark/') and extend the schema table in
the script - network access to raw.githubusercontent.com may require a shell tool rather than a
web fetch tool.
