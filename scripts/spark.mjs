#!/usr/bin/env node
/**
 * spark report analyzer - decodes raw spark profiler payloads (protobuf) and
 * produces attribution artifacts. Standard library only; no npm install.
 *
 *   node spark.mjs fetch   <url|code|file> [--out DIR]
 *   node spark.mjs probe   <file> [--json]
 *   node spark.mjs analyze <file> [--out DIR] [--thread NAME|INDEX] [--top N] [--tree-min PCT] [--dive-min PCT] [--dive-depth N] [--max-dives N]
 *   node spark.mjs owners  <file> [--thread NAME|INDEX] [--top N]
 *   node spark.mjs callers <file> <method-key> [--thread NAME|INDEX] [--top N] [--like]
 *   node spark.mjs dive    <file> <method-key> [--thread NAME|INDEX] [--like] [--depth N] [--min PCT]
 *
 * method-key is 'fully.qualified.ClassName.methodName' (as printed by analyze).
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import https from 'node:https';
import crypto from 'node:crypto';

/* ------------------------------------------------------------------ schema */
const M = {
  SamplerData: {
    1: ['metadata', 'SamplerMetadata'],
    2: ['threads', 'ThreadNode', { rep: true }],
    3: ['class_sources', 'map<string,string>'],
    4: ['method_sources', 'map<string,string>'],
    5: ['line_sources', 'map<string,string>'],
    6: ['time_windows', 'int32', { rep: true }],
    7: ['time_window_statistics', 'map<int32,WindowStatistics>'],
    8: ['channel_info', 'SocketChannelInfo'],
  },
  SocketChannelInfo: { 1: ['channel_id', 'string'], 2: ['public_key', 'bytes'] },
  ThreadNode: {
    1: ['name', 'string'],
    3: ['children', 'StackTraceNode', { rep: true }],
    4: ['times', 'double', { rep: true }],
    5: ['children_refs', 'int32', { rep: true }],
  },
  StackTraceNode: {
    3: ['class_name', 'string'],
    4: ['method_name', 'string'],
    5: ['parent_line_number', 'int32'],
    6: ['line_number', 'int32'],
    7: ['method_desc', 'string'],
    8: ['times', 'double', { rep: true }],
    9: ['children_refs', 'int32', { rep: true }],
  },
  CommandSenderMetadata: { 1: ['type', 'enum'], 2: ['name', 'string'], 3: ['unique_id', 'string'] },
  ThreadDumper: { 1: ['type', 'enum'], 2: ['ids', 'int64', { rep: true }], 3: ['patterns', 'string', { rep: true }] },
  DataAggregator: { 1: ['type', 'enum'], 2: ['thread_grouper', 'enum'], 3: ['tick_length_threshold', 'int64'], 4: ['number_of_included_ticks', 'int32'] },
  SamplerMetadata: {
    1: ['creator', 'CommandSenderMetadata'],
    2: ['start_time', 'int64'],
    3: ['interval', 'int32'],
    4: ['thread_dumper', 'ThreadDumper'],
    5: ['data_aggregator', 'DataAggregator'],
    6: ['comment', 'string'],
    7: ['platform_metadata', 'PlatformMetadata'],
    8: ['platform_statistics', 'PlatformStatistics'],
    9: ['system_statistics', 'SystemStatistics'],
    10: ['server_configurations', 'map<string,string>'],
    11: ['end_time', 'int64'],
    12: ['number_of_ticks', 'int32'],
    13: ['sources', 'map<string,PluginOrModMetadata>'],
    14: ['extra_platform_metadata', 'map<string,string>'],
    15: ['sampler_mode', 'enum'],
    16: ['sampler_engine', 'enum'],
    17: ['sampler_engine_version', 'string'],
    18: ['metrics', 'Metrics'],
  },
  PlatformMetadata: { 1: ['type', 'enum'], 2: ['name', 'string'], 3: ['version', 'string'], 4: ['minecraft_version', 'string'], 7: ['spark_data_version', 'int32'], 8: ['brand', 'string'], 9: ['spark_version', 'string'] },
  PluginOrModMetadata: { 1: ['name', 'string'], 2: ['version', 'string'], 3: ['author', 'string'], 4: ['description', 'string'], 5: ['builtin', 'bool'] },
  SystemStatistics: {
    1: ['cpu', 'SysCpu'], 2: ['memory', 'SysMemory'], 3: ['gc', 'map<string,SysGc>'],
    4: ['disk', 'SysDisk'], 5: ['os', 'SysOs'], 6: ['java', 'SysJava'], 7: ['uptime', 'int64'],
    8: ['net', 'map<string,NetInterface>'], 9: ['jvm', 'SysJvm'],
  },
  SysCpu: { 1: ['threads', 'int32'], 2: ['process_usage', 'Usage1m15m'], 3: ['system_usage', 'Usage1m15m'], 4: ['model_name', 'string'] },
  Usage1m15m: { 1: ['last1m', 'double'], 2: ['last15m', 'double'] },
  SysMemory: { 1: ['physical', 'MemPool'], 2: ['swap', 'MemPool'] },
  MemPool: { 1: ['used', 'int64'], 2: ['total', 'int64'] },
  SysGc: { 1: ['total', 'int64'], 2: ['avg_time', 'double'], 3: ['avg_frequency', 'double'] },
  SysDisk: { 1: ['used', 'int64'], 2: ['total', 'int64'] },
  SysOs: { 1: ['arch', 'string'], 2: ['name', 'string'], 3: ['version', 'string'] },
  SysJava: { 1: ['vendor', 'string'], 2: ['version', 'string'], 3: ['vendor_version', 'string'], 4: ['vm_args', 'string'] },
  SysJvm: { 1: ['name', 'string'], 2: ['vendor', 'string'], 3: ['version', 'string'] },
  NetInterface: { 1: ['rx_bytes', 'RollingAverageValues'], 2: ['tx_bytes', 'RollingAverageValues'], 3: ['rx_packets', 'RollingAverageValues'], 4: ['tx_packets', 'RollingAverageValues'] },
  RollingAverageValues: { 1: ['mean', 'double'], 2: ['max', 'double'], 3: ['min', 'double'], 4: ['median', 'double'], 5: ['percentile95', 'double'] },
  MemoryUsage: { 1: ['used', 'int64'], 2: ['committed', 'int64'], 3: ['init', 'int64'], 4: ['max', 'int64'] },
  PlatformStatistics: {
    1: ['memory', 'PsMemory'], 2: ['gc', 'map<string,PsGc>'], 3: ['uptime', 'int64'], 4: ['tps', 'PsTps'],
    5: ['mspt', 'PsMspt'], 6: ['ping', 'PsPing'], 7: ['player_count', 'int64'], 8: ['world', 'WorldStatistics'], 9: ['online_mode', 'enum'],
  },
  PsMemory: {
    1: ['heap', 'MemoryUsage'], 2: ['non_heap', 'MemoryUsage'], 3: ['pools', 'PsMemPool', { rep: true }],
    4: ['alloc_bps_last1m', 'RollingAverageValues'], 5: ['alloc_bps_last5m', 'RollingAverageValues'], 6: ['alloc_bps_last15m', 'RollingAverageValues'],
  },
  PsMemPool: { 1: ['name', 'string'], 2: ['usage', 'MemoryUsage'], 3: ['collection_usage', 'MemoryUsage'] },
  PsGc: { 1: ['total', 'int64'], 2: ['avg_time', 'double'], 3: ['avg_frequency', 'double'] },
  PsTps: { 1: ['last1m', 'double'], 2: ['last5m', 'double'], 3: ['last15m', 'double'], 4: ['game_target_tps', 'int32'] },
  PsMspt: { 1: ['last1m', 'RollingAverageValues'], 2: ['last5m', 'RollingAverageValues'], 3: ['game_max_ideal_mspt', 'int32'] },
  PsPing: { 1: ['last15m', 'RollingAverageValues'] },
  WorldStatistics: {
    1: ['total_entities', 'int32'], 2: ['entity_counts', 'map<string,int32>'], 3: ['worlds', 'World', { rep: true }],
    4: ['game_rules', 'GameRule', { rep: true }], 5: ['data_packs', 'DataPack', { rep: true }],
  },
  World: { 1: ['name', 'string'], 2: ['total_entities', 'int32'], 3: ['regions', 'Region', { rep: true }] },
  Region: { 1: ['total_entities', 'int32'], 2: ['chunks', 'Chunk', { rep: true }] },
  Chunk: { 1: ['x', 'int32'], 2: ['z', 'int32'], 3: ['total_entities', 'int32'], 4: ['entity_counts', 'map<string,int32>'] },
  GameRule: { 1: ['name', 'string'], 2: ['default_value', 'string'], 3: ['world_values', 'map<string,string>'] },
  DataPack: { 1: ['name', 'string'], 2: ['description', 'string'], 3: ['source', 'string'], 4: ['builtin', 'bool'] },
  WindowStatistics: {
    1: ['ticks', 'int32'], 2: ['cpu_process', 'double'], 3: ['cpu_system', 'double'], 4: ['tps', 'double'],
    5: ['mspt_median', 'double'], 6: ['mspt_max', 'double'], 7: ['players', 'int32'], 8: ['entities', 'int32'],
    9: ['tile_entities', 'int32'], 10: ['chunks', 'int32'], 11: ['start_time', 'int64'], 12: ['end_time', 'int64'], 13: ['duration', 'int32'],
  },
  Metrics: {
    1: ['tps', 'DoubleMetricSeries'], 2: ['tick_duration', 'AveragesMetricSeries'], 3: ['cpu_usage_process', 'DoubleMetricSeries'],
    4: ['cpu_usage_system', 'DoubleMetricSeries'], 5: ['memory_usage_heap', 'MemoryUsageMetricSeries'],
    6: ['memory_usage_non_heap', 'MemoryUsageMetricSeries'], 7: ['memory_allocation', 'DoubleMetricSeries'],
    8: ['world_info', 'WorldInfoMetricSeries'], 9: ['player_ping', 'AveragesMetricSeries'],
  },
  DoubleMetricSeries: { 1: ['start_timestamp_ms', 'int64'], 2: ['timestamp_deltas_ms', 'uint32', { rep: true }], 3: ['values', 'double', { rep: true }] },
  AveragesMetricSeries: { 1: ['start_timestamp_ms', 'int64'], 2: ['timestamp_deltas_ms', 'uint32', { rep: true }], 3: ['values', 'RollingAverageValues', { rep: true }] },
  MemoryUsageMetricSeries: { 1: ['start_timestamp_ms', 'int64'], 2: ['timestamp_deltas_ms', 'uint32', { rep: true }], 3: ['values', 'MemoryUsage', { rep: true }] },
  WorldInfoMetricSeries: { 1: ['start_timestamp_ms', 'int64'], 2: ['timestamp_deltas_ms', 'uint32', { rep: true }], 3: ['values', 'WorldInfoValues', { rep: true }] },
  WorldInfoValues: { 1: ['players', 'int32'], 2: ['entities', 'int32'], 3: ['tile_entities', 'int32'], 4: ['chunks', 'int32'] },
  HealthData: { 1: ['metadata', 'HealthMetadata'], 2: ['time_window_statistics', 'map<int32,WindowStatistics>'], 3: ['channel_info', 'SocketChannelInfo'] },
  HealthMetadata: {
    1: ['creator', 'CommandSenderMetadata'], 2: ['platform_metadata', 'PlatformMetadata'], 3: ['platform_statistics', 'PlatformStatistics'],
    4: ['system_statistics', 'SystemStatistics'], 5: ['generated_time', 'int64'], 6: ['server_configurations', 'map<string,string>'],
    7: ['sources', 'map<string,PluginOrModMetadata>'], 8: ['extra_platform_metadata', 'map<string,string>'], 9: ['metrics', 'Metrics'],
  },
  HeapData: { 1: ['metadata', 'HeapMetadata'], 2: ['entries', 'HeapEntry', { rep: true }] },
  HeapMetadata: {
    1: ['creator', 'CommandSenderMetadata'], 2: ['platform_metadata', 'PlatformMetadata'], 3: ['platform_statistics', 'PlatformStatistics'],
    4: ['system_statistics', 'SystemStatistics'], 5: ['generated_time', 'int64'], 6: ['server_configurations', 'map<string,string>'],
    7: ['sources', 'map<string,PluginOrModMetadata>'], 8: ['extra_platform_metadata', 'map<string,string>'], 9: ['metrics', 'Metrics'],
  },
  HeapEntry: { 1: ['order', 'int32'], 2: ['instances', 'int32'], 3: ['size', 'int64'], 4: ['type', 'string'] },
};

/* ------------------------------------------------------------- wire reader */
function rv(buf, pos) {
  let result = 0n, shift = 0n;
  for (let i = 0; i < 10; i++) {
    if (pos >= buf.length) throw new Error('truncated varint at ' + pos);
    const b = buf[pos++];
    result |= BigInt(b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7n;
    if (i === 9) throw new Error('varint exceeds 10 bytes at ' + (pos - 1));
  }
  return [result, pos];
}
function skipField(buf, pos, wt) {
  if (wt === 0) { const r = rv(buf, pos); return r[1]; }
  if (wt === 1) { const end = pos + 8; if (end > buf.length) throw new Error('truncated fixed64 at ' + pos); return end; }
  if (wt === 2) {
    const r = rv(buf, pos); const end = r[1] + Number(r[0]);
    if (!Number.isSafeInteger(end) || end > buf.length) throw new Error('truncated length-delimited field at ' + pos);
    return end;
  }
  if (wt === 5) { const end = pos + 4; if (end > buf.length) throw new Error('truncated fixed32 at ' + pos); return end; }
  throw new Error('bad wire type ' + wt + ' at ' + pos);
}
const int64toNum = (v) => { const n = Number(v); return Number.isSafeInteger(n) ? n : v.toString(); };
const isMsg = (t) => /^[A-Z]/.test(t);
/** Wire type a schema type must carry when encoded singular. */
function expectedWt(type) {
  if (type === 'string' || type === 'bytes' || type.startsWith('map<') || isMsg(type)) return 2;
  if (type === 'double' || type === 'fixed64') return 1;
  if (type === 'float' || type === 'fixed32') return 5;
  return 0;
}
function readScalar(buf, pos, wt, type) {
  switch (type) {
    case 'string': { const r = rv(buf, pos); const end = r[1] + Number(r[0]); if (end > buf.length) throw new Error('truncated string at ' + pos); return [buf.toString('utf8', r[1], end), end]; }
    case 'bytes': { const r = rv(buf, pos); const end = r[1] + Number(r[0]); if (end > buf.length) throw new Error('truncated bytes at ' + pos); return [buf.subarray(r[1], end), end]; }
    case 'double': if (pos + 8 > buf.length) throw new Error('truncated double at ' + pos); return [buf.readDoubleLE(pos), pos + 8];
    case 'float': if (pos + 4 > buf.length) throw new Error('truncated float at ' + pos); return [buf.readFloatLE(pos), pos + 4];
    case 'int64': case 'uint64': { const r = rv(buf, pos); return [int64toNum(r[0]), r[1]]; }
    case 'bool': { const r = rv(buf, pos); return [r[0] !== 0n, r[1]]; }
    case 'int32': case 'enum': { const r = rv(buf, pos); return [Number(BigInt.asIntN(32, r[0])), r[1]]; }
    case 'uint32': { const r = rv(buf, pos); return [Number(r[0]), r[1]]; }
    default:
      if (isMsg(type)) { const r = rv(buf, pos); return [decodeMsg(buf, r[1], r[1] + Number(r[0]), type), r[1] + Number(r[0])]; }
      throw new Error('unknown scalar type ' + type);
  }
}
function decodeMsg(buf, pos, end, msgName) {
  const schema = M[msgName];
  if (!schema) throw new Error('no schema for ' + msgName);
  if (pos < 0 || end < pos || end > buf.length) throw new Error('invalid message bounds for ' + msgName);
  const out = {};
  while (pos < end) {
    const t = rv(buf, pos); pos = t[1];
    const tag = Number(t[0]), field = tag >>> 3, wt = tag & 7;
    const desc = schema[field];
    if (!desc) { pos = skipField(buf, pos, wt); continue; }
    const name = desc[0], type = desc[1], opts = desc[2];
    const rep = !!(opts && opts.rep);
    // A field decoded with the wrong message schema (report-kind probing, or a newer
    // spark build) can carry a different wire type. Trust the wire, not the schema:
    // skip it instead of desynchronizing the stream.
    const expWt = expectedWt(type);
    const compatible = (rep && !type.startsWith('map<')) ? (wt === expWt || (wt === 2 && expWt !== 2)) : wt === expWt;
    if (!compatible) { pos = skipField(buf, pos, wt); continue; }
    if (type.startsWith('map<')) {
      const inner = type.slice(4, -1).split(',');
      const kt = inner[0], vt = inner[1];
      const lr = rv(buf, pos); const subEnd = lr[1] + Number(lr[0]);
      let key = kt === 'string' ? '' : 0, val = null, sp = lr[1];
      while (sp < subEnd) {
        const t2 = rv(buf, sp); sp = t2[1];
        const f2 = Number(t2[0]) >>> 3, w2 = Number(t2[0]) & 7;
        if (f2 === 1) { const r = readScalar(buf, sp, w2, kt); key = r[0]; sp = r[1]; }
        else if (f2 === 2) { const r = readScalar(buf, sp, w2, vt); val = r[0]; sp = r[1]; }
        else sp = skipField(buf, sp, w2);
      }
      if (!out[name]) out[name] = {};
      out[name][key] = val;
      pos = subEnd;
    } else if (rep && wt === 2 && !isMsg(type)) {
      const lr = rv(buf, pos); const subEnd = lr[1] + Number(lr[0]);
      let sp = lr[1];
      const arr = out[name] || (out[name] = []);
      while (sp < subEnd) { const r = readScalar(buf, sp, 0, type); arr.push(r[0]); sp = r[1]; }
      pos = subEnd;
    } else {
      const r = readScalar(buf, pos, wt, type);
      if (rep) (out[name] || (out[name] = [])).push(r[0]); else out[name] = r[0];
      pos = r[1];
    }
    if (pos > end) throw new Error('field exceeds message bounds for ' + msgName);
  }
  return out;
}

/* ------------------------------------------------------------------- input */
export function loadBuffer(ref) {
  let buf = fs.readFileSync(ref);
  if (buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b) buf = zlib.gunzipSync(buf);
  return buf;
}
function httpGet(url, redirects = 0, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('too many redirects'));
    const req = https.get(url, { headers: { 'User-Agent': 'spark-report-analyzer', 'Accept-Encoding': 'gzip' } }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return resolve(httpGet(new URL(res.headers.location, url).href, redirects + 1, timeoutMs));
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode + ' for ' + url)); }
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        let body = Buffer.concat(chunks);
        const enc = String(res.headers['content-encoding'] || '');
        if (enc.includes('gzip') || (body[0] === 0x1f && body[1] === 0x8b)) {
          try { body = zlib.gunzipSync(body); } catch { /* leave as-is */ }
        }
        resolve({ body, headers: res.headers });
      });
    });
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error('request timed out after ' + timeoutMs + ' ms'));
    });
    req.on('error', reject);
  });
}
const CODE_RE = /^[A-Za-z0-9_-]{5,}$/;
function extractCode(target) {
  const m = /spark\.lucko\.me\/(?:viewer\/)?([A-Za-z0-9_-]+)/.exec(target);
  if (m) return m[1];
  const b = /bytebin\.lucko\.me\/([A-Za-z0-9_-]+)/.exec(target);
  if (b) return b[1];
  if (CODE_RE.test(target)) return target;
  return undefined;
}
export async function fetchReport(target, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const dest = path.join(outDir, 'raw.bin');
  if (fs.existsSync(target) && fs.statSync(target).isFile()) {
    fs.copyFileSync(target, dest);
    const body = fs.readFileSync(dest);
    fs.writeFileSync(path.join(outDir, 'source.json'), JSON.stringify({
      kind: 'file',
      source: path.resolve(target),
      bytes: body.length,
      sha256: crypto.createHash('sha256').update(body).digest('hex'),
      fetchedAt: new Date().toISOString(),
      fetchedTimezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    }, null, 2));
    return dest;
  }
  const code = extractCode(target);
  if (!code) throw new Error('cannot parse a spark code from: ' + target);
  const { body } = await httpGet('https://bytebin.lucko.me/' + code);
  if (!body.length) throw new Error('empty payload for code ' + code);
  fs.writeFileSync(dest, body);
  fs.writeFileSync(path.join(outDir, 'source.json'), JSON.stringify({
    kind: 'bytebin',
    code,
    url: 'https://spark.lucko.me/' + code,
    bytebinUrl: 'https://bytebin.lucko.me/' + code,
    bytes: body.length,
    sha256: crypto.createHash('sha256').update(body).digest('hex'),
    fetchedAt: new Date().toISOString(),
    fetchedTimezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
  }, null, 2));
  return dest;
}

/* ------------------------------------------------------------------- kinds */
function topLevelScan(buf) {
  const seen = {};
  let pos = 0;
  while (pos < buf.length) {
    const t = rv(buf, pos);
    const tag = Number(t[0]), field = tag >>> 3, wt = tag & 7;
    if (!(wt === 0 || wt === 1 || wt === 2 || wt === 5) || field === 0) break;
    pos = t[1];
    const start = pos;
    pos = skipField(buf, pos, wt);
    if (!seen[field]) seen[field] = { field, wireType: wt, count: 1, length: wt === 2 ? pos - start - 1 : undefined, offset: start };
    else seen[field].count++;
  }
  return Object.values(seen).sort((a, b) => a.field - b.field);
}
export function detectAndDecode(buf) {
  const metaLen = (() => {
    const t = rv(buf, 0);
    if ((Number(t[0]) >>> 3) !== 1 || (Number(t[0]) & 7) !== 2) return undefined;
    const l = rv(buf, t[1]);
    return { start: l[1], end: l[1] + Number(l[0]) };
  })();
  if (!metaLen) return { kind: 'unknown', scan: topLevelScan(buf) };
  const tryDecode = (a, b, name) => { try { return decodeMsg(buf, a, b, name); } catch { return undefined; } };
  const sampler = tryDecode(metaLen.start, metaLen.end, 'SamplerMetadata');
  if (sampler && (sampler.thread_dumper || sampler.interval || sampler.number_of_ticks || sampler.data_aggregator)) {
    return { kind: 'sampler', data: tryDecode(0, buf.length, 'SamplerData') };
  }
  const healthMeta = tryDecode(metaLen.start, metaLen.end, 'HealthMetadata');
  const heapMeta = tryDecode(metaLen.start, metaLen.end, 'HeapMetadata');
  // field 2 shape decides health (map<int32,WindowStatistics>) vs heap (repeated HeapEntry)
  const scan = topLevelScan(buf);
  const f2 = scan.find((s) => s.field === 2);
  let kind = 'unknown';
  if (f2 && f2.wireType === 2 && f2.length) {
    const t = rv(buf, f2.offset);
    const entryStart = t[1];
    const probe = tryDecode(entryStart, entryStart + Number(t[0]), 'HeapEntry');
    if (probe && typeof probe.type === 'string' && typeof probe.size !== 'undefined') kind = 'heap';
    else {
      // map entry: key/value message with WindowStatistics
      let sp = entryStart, isWindow = false;
      const subEnd = entryStart + Number(t[0]);
      while (sp < subEnd) {
        const t2 = rv(buf, sp); sp = t2[1];
        const f2n = Number(t2[0]) >>> 3, w2 = Number(t2[0]) & 7;
        if (f2n === 2 && w2 === 2) {
          const lr = rv(buf, sp);
          const win = tryDecode(lr[1], lr[1] + Number(lr[0]), 'WindowStatistics');
          if (win && (typeof win.ticks !== 'undefined' || typeof win.tps !== 'undefined')) isWindow = true;
          sp = lr[1] + Number(lr[0]);
        } else sp = skipField(buf, sp, w2);
      }
      kind = isWindow ? 'health' : 'unknown';
    }
  }
  if (kind === 'health') return { kind, data: tryDecode(0, buf.length, 'HealthData'), meta: healthMeta };
  if (kind === 'heap') return { kind, data: tryDecode(0, buf.length, 'HeapData'), meta: heapMeta };
  return { kind: 'unknown', meta: healthMeta, metaAlt: heapMeta, scan };
}

/* -------------------------------------------------------------- tree utils */
export function buildTree(thread) {
  const nodes = thread.children || [];
  const kids = new Array(nodes.length);
  const inclTot = new Float64Array(nodes.length);
  const selfTot = new Float64Array(nodes.length);
  const parent = new Int32Array(nodes.length).fill(-1);
  for (let i = 0; i < nodes.length; i++) {
    const refs = nodes[i].children_refs || [];
    kids[i] = refs;
    const tv = nodes[i].times || [];
    let s = 0; for (let k = 0; k < tv.length; k++) s += tv[k];
    inclTot[i] = s;
  }
  const reachable = new Uint8Array(nodes.length);
  const edgeSeen = new Set();
  const roots = thread.children_refs || [];
  const stack = [];
  let badRefs = 0, dupRefs = 0, cycleRefs = 0;
  for (const r of roots) {
    if (r >= 0 && r < nodes.length) {
      if (reachable[r]) dupRefs++;
      reachable[r] = 1;
      parent[r] = -1;
      stack.push(r);
    } else badRefs++;
  }
  while (stack.length) {
    const i = stack.pop();
    for (const c of kids[i]) {
      if (c < 0 || c >= nodes.length) { badRefs++; continue; }
      const edge = i + ':' + c;
      if (edgeSeen.has(edge)) dupRefs++;
      edgeSeen.add(edge);
      if (reachable[c]) {
        let isCycle = false;
        let ancestor = i;
        while (ancestor >= 0) {
          if (ancestor === c) { cycleRefs++; isCycle = true; break; }
          ancestor = parent[ancestor];
        }
        if (parent[c] !== i && !isCycle) dupRefs++;
        continue;
      }
      reachable[c] = 1;
      parent[c] = i;
      stack.push(c);
    }
  }
  let negSelf = 0, negSelfMs = 0;
  for (let i = 0; i < nodes.length; i++) {
    let s = 0; for (const c of kids[i]) s += inclTot[c];
    let v = inclTot[i] - s;
    // float rounding produces tiny negatives; larger ones mean the wiring is broken, so they are
    // clamped (never reported as negative cost) and surfaced as a validation issue instead.
    if (v < 0) { negSelf++; negSelfMs += v; v = 0; }
    selfTot[i] = v;
  }
  let ROOT = 0; for (const r of roots) ROOT += inclTot[r];
  let selfSum = 0; for (let i = 0; i < nodes.length; i++) selfSum += selfTot[i];
  const unreachable = reachable.reduce((a, v) => a + (v ? 0 : 1), 0);
  const nW = (thread.times || []).length;
  const windowIssues = [];
  for (let w = 0; w < nW; w++) {
    let root = 0;
    for (const r of roots) root += (nodes[r].times || [])[w] || 0;
    let windowSelfSum = 0;
    let windowNegativeSelf = 0;
    let windowNegativeSelfMs = 0;
    for (let i = 0; i < nodes.length; i++) {
      const own = (nodes[i].times || [])[w] || 0;
      let child = 0;
      for (const c of kids[i]) child += (nodes[c].times || [])[w] || 0;
      const value = own - child;
      if (value < 0) {
        windowNegativeSelf++;
        windowNegativeSelfMs += value;
      } else {
        windowSelfSum += value;
      }
    }
    const selfVsRoot = windowSelfSum - root;
    windowIssues.push({
      window: w,
      root,
      selfSum: windowSelfSum,
      selfVsRoot,
      negativeSelf: windowNegativeSelf,
      negativeSelfMs: windowNegativeSelfMs,
      suspicious: windowNegativeSelf > 0 || Math.abs(selfVsRoot) > Math.max(0.5, 1e-6 * root),
    });
  }
  return {
    nodes, kids, inclTot, selfTot, parent, roots, ROOT, nW, name: thread.name,
    issues: { badRefs, dupRefs, cycleRefs, negSelf, negSelfMs, selfSum, selfVsRoot: selfSum - ROOT, unreachable, nW, windowIssues },
  };
}
function hasWindowValidationFailure(issues) {
  return (issues.windowIssues || []).some((w) => w.suspicious);
}
function selfVec(t, i, w) {
  const nodes = t.nodes;
  const own = (nodes[i].times || [])[w] || 0;
  let s = 0;
  for (const c of t.kids[i]) s += (nodes[c].times || [])[w] || 0;
  const v = own - s;
  return v > 0 ? v : 0; // clamped to zero, as documented in references/data-format.md
}
const key = (t, i) => (t.nodes[i].class_name || '?') + '.' + (t.nodes[i].method_name || '?');
const label = (t, i) => key(t, i) + (t.nodes[i].line_number ? ':' + t.nodes[i].line_number : '');

/* ------------------------------------------------------------- attribution */
const DEFAULT_LIBS = ['owo', 'ldlib2', 'titanium', 'thunderbolt', 'architectury', 'placebo', 'curios', 'rhino', 'geckolib',
  'cyclopscore', 'collective', 'balm', 'glodium', 'sauce', 'cupboard', 'prickle', 'kubejs', 'puzzleslib', 'resourceful',
  'supermartijn642corelib', 'mcjtylib', 'ferritecore', 'modernfix', 'observable', 'mixinextras', 'fabric-api', 'architectury-api'];
const DEFAULT_FAMILIES = [
  ['AE2 ecosystem', '(ae2|megacells|ae2lt|ae2omnicells|extendedae_plus|appflux|ifeu|thunderbolt|applied|appex|merequester|neoecoae|ae2cs|ae2ct|ae2wtlib|expatternprovider)'],
  ['Mekanism family', '(mekanism|mekmm|mekanism_extras|mekanismgenerators|mek_x_star|mekanism_unleashed|meklg|mekaf|meklm|evolvedmekanism)'],
  ['Industrial Foregoing / Titanium', '(industrialforegoing|industrialforegoingsouls|titanium|dysoncubeproject)'],
  ['Create family', '(create|createaddition|create_enchantment_industry|ponder|flywheel)'],
  ['Thermal / CoFH', '(thermal|cofh|redstone_arsenal)'],
  ['Botania / Mythic', '(botania|mythicbotany)'],
];
const VANILLA = /^(net\.minecraft|com\.mojang)/;
/** JVM/runtime namespaces: unmapped too, but never a mod - keep them out of the unmapped-mod view. */
const RUNTIME_ROOT = /^(java|javax|jdk|sun|com\.sun|kotlin|scala|groovy|org\.graalvm|org\.slf4j|ch\.qos|org\.apache\.logging)(\.|$)/;
/**
 * The server thread parked here: it is waiting for work (a parallel stream, a lock, a worker
 * pool), not doing CPU work. Such frames are latency, not cost - and they are the one place
 * where "just add cores" looks attractive while being useless. Reported separately so the
 * caller can be traced back to the mod frame that started the parallel work.
 */
const BLOCKING_RE = /(?:^|\.)(?:Unsafe\.park|LockSupport\.(?:park|parkNanos|parkUntil)|ForkJoinTask\.awaitDone|Object\.wait|Thread\.sleep|CountDownLatch\.await|CompletableFuture\.(?:join|get))$/;
const SCHEDULER_RE = /(?:^|\.)(?:Unsafe\.unpark|ForkJoinTask\.(?:join|invoke)|ForkJoinPool\.(?:signalWork|awaitWork|scan)|Thread\.yield)$/;
// Minecraft's main loop parks between ticks when it has spare headroom. Keep this
// separate from real lock/worker waits: the sampled thread is idle by design.
const TICK_PACING_RE = /(?:MinecraftServer\.(?:waitUntilNextTick|waitForTasks)|(?:^|\.)(?:waitUntilNextTick|waitForTasks)|managedBlock)$/;
const GC_PAUSE_RE = /(?:pauses?|safepoints?|stop[\s_-]*the[\s_-]*world|young generation|old generation)$/i;
const GC_CYCLE_RE = /(?:cycle|concurrent)/i;
function gcKind(name) {
  const s = String(name || '');
  if (GC_PAUSE_RE.test(s)) return 'pause';
  if (GC_CYCLE_RE.test(s)) return 'cycle';
  return 'unknown';
}
function aggregatorInfo(agg) {
  const hasType = Object.prototype.hasOwnProperty.call(agg || {}, 'type');
  const rawType = hasType ? agg.type : 0;
  const type = rawType === 0 ? 'SIMPLE' : rawType === 1 ? 'TICKED' : `unknown(${rawType})`;
  return { type, rawType, present: hasType, defaulted: !hasType };
}
function hasPacingAncestor(t, index) {
  let i = index;
  while (i >= 0) {
    const node = t.nodes[i];
    const method = (node.class_name || '?') + '.' + (node.method_name || '?');
    if (TICK_PACING_RE.test(method)) return true;
    i = t.parent[i];
  }
  return false;
}
/**
 * Mixin generates frames named '<mixin>$<hash>$<modid>$<target>' (wrapOperation, redirect,
 * modifyReturnValue, ...), which is the only place the *injecting* mod id appears - the class
 * itself still belongs to the target mod. MixinExtras bridges have no injector id at all.
 */
function mixinInfo(methodName) {
  const mn = String(methodName || '');
  if (/\$bridge\$/.test(mn)) return { mod: 'mixinextras-bridge', target: mn.replace(/^.*\$bridge\$/, ''), uncertain: true };
  const m = /^[A-Za-z_][A-Za-z0-9_]*\$([0-9a-z]{5,8})\$([a-z][a-z0-9_]{1,31})\$(.+)$/.exec(mn);
  if (m) return { mod: m[2], target: m[3] };
  return null;
}
/** Package root of a class name: 'net.yxiao233.appliedsoul.x.Y' -> 'net.yxiao233.appliedsoul'. */
function rootOf(cn) {
  const segs = String(cn || '?').split('.').filter(Boolean);
  return segs.slice(0, Math.min(3, Math.max(1, segs.length - 1))).join('.');
}
/** A mapped id that is really a JVM/proxy artefact, not a mod ('jdk.proxy3', 'com.sun.proxy.$Proxy12'). */
const RUNTIME_ID = /^(jdk|java|javax|sun|com\.sun)(\.|$)/;
const PUBLIC_ROOT = /^(it\.unimi\.dsi|com\.google\.common|net\.neoforged\.bus|net\.neoforged\.fml|net\.minecrell|com\.electronwill|io\.netty)(\.|$)/;
const normId = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
/** Same-mod names differ by prefixes/suffixes ('appliedsoul', 'appliedsoulcore'); 'common' must not match 'commoncapabilities'. */
function sameId(a, b) {
  if (a === b) return true;
  if (a.length < 6 || b.length < 6) return false;
  if (!(a.startsWith(b) || b.startsWith(a))) return false;
  return Math.min(a.length, b.length) / Math.max(a.length, b.length) >= 0.75;
}
/** Index the payload's own mod/plugin list, so unmapped package roots can be matched against it. */
function sourceIndex(meta) {
  const out = [];
  for (const [k, v] of Object.entries((meta && meta.sources) || {})) {
    const declared = (v && v.name) || k;
    const norm = normId(String(declared).replace(/^(mod|plugin)[/.]/i, ''));
    if (norm.length >= 4) out.push({ key: k, declared, norm, version: (v && v.version) || '', jar: (v && v.description) || '' });
  }
  return out;
}
/** Fuzzy-match an unmapped package root against the declared mod list ('net.yxiao233.appliedsoul' -> 'appliedsoul'). */
function matchSource(root, idx) {
  const segs = String(root).split('.').filter(Boolean);
  const cands = [];
  for (let take = 1; take <= 3 && take <= segs.length; take++) cands.push(normId(segs.slice(segs.length - take).join('')));
  for (const c of cands) {
    if (c.length < 4) continue;
    for (const s of idx) if (sameId(c, s.norm)) return s;
  }
  return null;
}

function makeAttributor(t, classSources, libs) {
  const libSet = new Set(libs);
  const parent = t.parent;
  const clsOf = (i) => t.nodes[i].class_name || '?';
  const chain = (i) => {
    const out = [];
    let c = i;
    while (c >= 0) {
      const m = classSources[clsOf(c)];
      // a proxy class is mapped to its own generated package, which is not a mod - fold it away
      if (m && !RUNTIME_ID.test(m) && !/\$Proxy/.test(m)) out.push(m);
      c = parent[c];
    }
    return out;
  };
  const nearestRealMod = (i) => { const ch = chain(i); return ch.find((m) => !libSet.has(m)) || ch[0] || null; };
  return {
    chain,
    nearestMod: (i) => { const ch = chain(i); return ch.length ? ch[0] : null; },
    nearestRealMod,
    nearestLib: (i) => { const ch = chain(i); return ch.length && libSet.has(ch[0]) ? ch[0] : null; },
    isLib: (m) => libSet.has(m),
    foldedOwner: (i) => { const m = nearestRealMod(i); if (m) return 'MOD:' + m; return VANILLA.test(clsOf(i)) ? 'Vanilla (Minecraft)' : 'JDK / library'; },
    owner: (i) => { const m = chain(i)[0]; if (m) return 'MOD:' + m; return VANILLA.test(clsOf(i)) ? 'Vanilla (Minecraft)' : 'JDK / library'; },
  };
}

/* ---------------------------------------------------------------- printing */
const pctOf = (root) => (v) => (root > 0 ? (v / root) * 100 : 0);
function table(rows, headers) {
  const w = headers.map((h, i) => Math.max(h.length, ...rows.map((r) => String(r[i]).length)));
  const line = (r) => r.map((c, i) => String(c).padEnd(w[i])).join('  ');
  return [line(headers), w.map((x) => '-'.repeat(x)).join('  '), ...rows.map(line)].join('\n');
}

/* ------------------------------------------------------------------ probe */
function describePlatform(meta) {
  const p = meta.platform_metadata || {};
  const ps = meta.platform_statistics || {};
  const s = meta.system_statistics || {};
  const out = [];
  out.push('platform      : ' + [p.name, p.version, p.minecraft_version ? 'MC ' + p.minecraft_version : '', p.brand].filter(Boolean).join(' | '));
  if (p.spark_version || p.spark_data_version !== undefined) out.push('spark version : ' + (p.spark_version || '(unknown)') + ' (data version ' + (p.spark_data_version !== undefined ? p.spark_data_version : '?') + ')');
  if (ps.tps) out.push('tps           : 1m=' + fmt(ps.tps.last1m) + ' 5m=' + fmt(ps.tps.last5m) + ' 15m=' + fmt(ps.tps.last15m) + (ps.tps.game_target_tps ? ' (target ' + ps.tps.game_target_tps + ')' : ''));
  if (ps.mspt && ps.mspt.last1m) {
    const m = ps.mspt.last1m;
    out.push('mspt (1m)     : mean=' + fmt(m.mean) + ' median=' + fmt(m.median) + ' p95=' + fmt(m.percentile95) + ' max=' + fmt(m.max) + ' min=' + fmt(m.min));
    if (ps.mspt.last5m) out.push('mspt (5m)     : median=' + fmt(ps.mspt.last5m.median) + ' max=' + fmt(ps.mspt.last5m.max));
    if (ps.mspt.game_max_ideal_mspt) out.push('tick budget   : ' + ps.mspt.game_max_ideal_mspt + ' ms');
  }
  if (typeof ps.player_count !== 'undefined') out.push('players       : ' + ps.player_count);
  {
    const cfgp = serverProperties(meta);
    const vdp = Number(pick(cfgp, ['view-distance', 'viewDistance', 'view_distance']));
    const simd = Number(pick(cfgp, ['simulation-distance', 'simulationDistance', 'simulation_distance']));
    if (Number.isFinite(vdp)) out.push('server config : view-distance ' + vdp + (Number.isFinite(simd) ? ', simulation-distance ' + simd : '') + ' (server.properties from the payload)');
  }
  if (ps.world) out.push('entities      : ' + (ps.world.total_entities || 0) + ' total across ' + ((ps.world.worlds || []).length) + ' worlds');
  if (ps.memory && ps.memory.heap) out.push('heap          : used ' + gb(ps.memory.heap.used) + ' / max ' + gb(ps.memory.heap.max) + ' committed ' + gb(ps.memory.heap.committed));
  if (ps.ping && ps.ping.last15m) out.push('ping          : mean=' + fmt(ps.ping.last15m.mean) + ' p95=' + fmt(ps.ping.last15m.percentile95));
  if (s.cpu) out.push('cpu           : ' + (s.cpu.model_name || '?') + ' x' + (s.cpu.threads || '?') + ' process=' + fmt(s.cpu.process_usage && s.cpu.process_usage.last1m) + ' system=' + fmt(s.cpu.system_usage && s.cpu.system_usage.last1m) + ' (fraction of all cores)');
  if (s.memory) out.push('system memory : used ' + gb(s.memory.physical && s.memory.physical.used) + ' / ' + gb(s.memory.physical && s.memory.physical.total) + '  swap ' + gb(s.memory.swap && s.memory.swap.used) + ' / ' + gb(s.memory.swap && s.memory.swap.total));
  if (s.java) out.push('java          : ' + [s.java.vendor, s.java.version, s.jvm && s.jvm.name].filter(Boolean).join(' '));
  if (s.uptime) out.push('uptime        : ' + (s.uptime / 1000 / 3600).toFixed(1) + ' h');
  if (s.gc) for (const k of Object.keys(s.gc)) {
    const g = s.gc[k] || {};
    if (!g.total) continue;
    const avg = Number.isFinite(g.avg_time) ? fmt(g.avg_time) + 'ms' : 'N/A';
    const kind = gcKind(k);
    out.push('gc            : ' + k + ' [' + kind + '] count=' + g.total + ' avg=' + avg + ' every ' + (Number.isFinite(g.avg_frequency) ? fmt(g.avg_frequency) + 'ms' : 'N/A') + ' scope=process-uptime cumulative');
  }
  const sources = meta.sources || {};
  out.push('mods/plugins  : ' + Object.keys(sources).length + ' sources');
  return out.join('\n');
}
const fmt = (v) => (typeof v === 'number' && Number.isFinite(v)
  ? (Math.abs(v) >= 100 ? v.toFixed(1) : v.toFixed(2))
  : (v === null || v === undefined || (typeof v === 'number' && !Number.isFinite(v)) ? 'N/A' : String(v)));
const gb = (v) => (typeof v === 'number' && Number.isFinite(v) ? (v / 1024 / 1024 / 1024).toFixed(2) + ' GB' : 'N/A');
const pick = (o, names) => { for (const n of names) if (o && o[n] !== undefined) return o[n]; return undefined; };
/**
 * Real payloads nest the whole file as server_configurations['server.properties'] = '<json string>';
 * other shapes are already flat. Without unwrapping it every server-setting lookup returns
 * undefined, and a NaN view-distance silently kills the chunk-loading verdict.
 */
function serverProperties(meta) {
  const cfg = (meta && meta.server_configurations) || {};
  const blob = cfg['server.properties'] !== undefined ? cfg['server.properties'] : cfg['server.properties.json'];
  if (typeof blob === 'string') {
    try { const o = JSON.parse(blob); if (o && typeof o === 'object') return o; } catch { /* keep the flat shape */ }
  }
  return cfg;
}
const lastWindowChunks = (sd) => {
  const ws = sd.time_window_statistics || {};
  const keys = Object.keys(ws).map(Number).sort((a, b) => a - b);
  return keys.length ? ws[keys[keys.length - 1]].chunks || 0 : 0;
};
function pingEnvironment(meta) {
  const ping = meta && meta.platform_statistics && meta.platform_statistics.ping && meta.platform_statistics.ping.last15m;
  if (!ping || ![ping.mean, ping.percentile95, ping.max].some(Number.isFinite)) return null;
  const mean = Number.isFinite(ping.mean) ? ping.mean : null;
  const p95 = Number.isFinite(ping.percentile95) ? ping.percentile95 : null;
  const max = Number.isFinite(ping.max) ? ping.max : null;
  const severity = (p95 !== null && p95 >= 1000) || (mean !== null && mean >= 1000)
    ? 'severe'
    : (p95 !== null && p95 >= 250) || (mean !== null && mean >= 250) ? 'elevated' : 'normal';
  return {
    source: 'platformStatistics.ping.last15m',
    meanMs: mean,
    p95Ms: p95,
    maxMs: max,
    severity,
    cpuAttribution: 'not_server_thread_cpu',
  };
}

function orderedWindowStats(sd) {
  const ws = (sd && sd.time_window_statistics) || {};
  return Object.keys(ws)
    .map(Number)
    .filter(Number.isFinite)
    .sort((a, b) => a - b)
    .map((key, index) => ({ key, index, stats: ws[key] || {} }));
}

function rootTotalsByWindow(thread, nW) {
  const nodes = (thread && thread.children) || [];
  const roots = (thread && thread.children_refs) || [];
  return Array.from({ length: nW }, (_, w) => {
    let total = 0;
    for (const r of roots) total += (nodes[r] && nodes[r].times ? nodes[r].times[w] : 0) || 0;
    return total;
  });
}

/**
 * Resolve the only denominator that can safely turn sampled self time into a
 * per-tick number. SIMPLE has all ticks; TICKED has only included slow ticks,
 * and only when the payload exposes a complete per-window included-tick table.
 * Never substitute the rolling 1m MSPT here: that is a different statistic.
 */
function resolveWindowDenominators(meta, statsRows, rootMsByWindow, nW) {
  const agg = (meta && meta.data_aggregator) || {};
  const aggInfo = aggregatorInfo(agg);
  const rows = [];
  const ticks = [];
  for (let i = 0; i < nW; i++) {
    const s = statsRows[i] && statsRows[i].stats ? statsRows[i].stats : {};
    const value = Number(s.ticks);
    ticks.push(Number.isFinite(value) && value >= 0 ? value : null);
  }
  const finiteTicks = ticks.filter((v) => v !== null);
  const tickSum = finiteTicks.reduce((a, v) => a + v, 0);
  const included = Number.isFinite(Number(agg.number_of_included_ticks)) ? Number(agg.number_of_included_ticks) : null;
  const complete = ticks.length === nW && finiteTicks.length === nW && nW > 0;
  const tickSumMatchesIncluded = included !== null && Math.abs(tickSum - included) < 0.5;
  const rootAvailable = Array.isArray(rootMsByWindow) && rootMsByWindow.length === nW;
  const tickQuality = aggInfo.type === 'SIMPLE'
    ? (complete ? 'complete' : 'missing per-window all-tick counts')
    : (complete && tickSumMatchesIncluded ? 'complete included-tick counts' : 'missing or inconsistent per-window included-tick counts');
  const reliable = aggInfo.type === 'SIMPLE' ? complete : (aggInfo.type === 'TICKED' && complete && tickSumMatchesIncluded);
  for (let i = 0; i < nW; i++) {
    const rootValue = rootAvailable ? Number(rootMsByWindow[i]) : NaN;
    const rootMs = Number.isFinite(rootValue) ? rootValue : null;
    const windowTicks = ticks[i];
    const usable = rootMs !== null && reliable && windowTicks !== null && windowTicks > 0;
    const source = usable
      ? (aggInfo.type === 'TICKED'
        ? 'root_ms / time_window_statistics.ticks (included ticks)'
        : 'root_ms / time_window_statistics.ticks (all ticks)')
      : (aggInfo.type === 'TICKED'
        ? 'unavailable: no reliable per-window included-tick denominator'
        : 'unavailable: no reliable per-window tick denominator');
    rows.push({
      window: i,
      rootMs,
      ticks: windowTicks,
      wallMsPerTick: usable ? rootMs / windowTicks : null,
      denominatorSource: source,
      denominatorKind: aggInfo.type === 'TICKED' ? 'included_ticks' : 'all_ticks',
      denominatorReliable: usable,
    });
  }
  const aggregateRootMs = rootAvailable
    ? rootMsByWindow.reduce((a, v) => a + (Number(v) || 0), 0)
    : null;
  const aggregateTicks = reliable ? finiteTicks.reduce((a, v) => a + v, 0) : null;
  const aggregateUsable = aggregateRootMs !== null && aggregateTicks !== null && aggregateTicks > 0;
  return {
    rows,
    aggregate: {
      rootMs: aggregateRootMs,
      ticks: aggregateTicks,
      wallMsPerTick: aggregateUsable ? aggregateRootMs / aggregateTicks : null,
      denominatorKind: aggInfo.type === 'TICKED' ? 'included_ticks' : 'all_ticks',
      denominatorReliable: aggregateUsable,
      denominatorSource: aggregateUsable
        ? (aggInfo.type === 'TICKED'
          ? 'sum(root_ms) / sum(time_window_statistics.ticks) (included ticks)'
          : 'sum(root_ms) / sum(time_window_statistics.ticks) (all ticks)')
        : (aggInfo.type === 'TICKED'
          ? 'unavailable: ' + tickQuality
          : 'unavailable: ' + tickQuality),
      tickQuality,
      includedTicks: included,
      observedTickSum: finiteTicks.length === nW ? tickSum : null,
    },
  };
}

/**
 * Turn the raw probe numbers into the verdicts the report needs, so that "is this the hardware
 * or the code" is answered by the tool instead of by guesswork.
 */
function envVerdicts(meta, ctx) {
  const out = [];
  const s = meta.system_statistics || {};
  const ps = meta.platform_statistics || {};
  const cpu = s.cpu || {};
  const threads = cpu.threads || 0;
  const proc = cpu.process_usage && cpu.process_usage.last1m;
  if (typeof proc === 'number' && threads) {
    const cores = proc * threads;
    out.push('cpu         : process occupies ~' + fmt(cores) + ' of ' + threads + ' logical cores (raw ' + fmt(proc) + ' of the machine) -> '
      + (cores / threads < 0.5
        ? 'NOT cpu-starved. More cores or a faster CPU will not fix this; the cost is per-tick work on the sampled thread.'
        : 'the process is genuinely CPU-hungry; moving work off the main thread or adding cores can help.'));
  }
  const swap = (s.memory && s.memory.swap) || {};
  if (swap.total > 0) {
    const r = (swap.used || 0) / swap.total;
    if (r >= 0.9) out.push('swap        : ' + gb(swap.used) + ' / ' + gb(swap.total) + ' used (' + (r * 100).toFixed(0) + '%) -> swap is essentially full. Swapped-out pages inflate the wall-clock cost of every blocking/park frame and GC pause; rule this out before blaming the code.');
    else if (r >= 0.5) out.push('swap        : ' + gb(swap.used) + ' / ' + gb(swap.total) + ' used (' + (r * 100).toFixed(0) + '%) -> half of swap is in use; watch for paging if timings look noisy.');
  }
  const uptimeMs = s.uptime || (meta.end_time && meta.start_time ? meta.end_time - meta.start_time : 0);
  if (s.gc && uptimeMs > 0) {
    for (const k of Object.keys(s.gc)) {
      const g = s.gc[k];
      if (!g || !g.total) continue;
      const kind = gcKind(k);
      if (kind !== 'pause') {
        out.push('gc          : ' + k + ' [' + kind + '] ran ' + g.total + 'x; cycle/activity counter only, not a stop-the-world pause estimate (counters are cumulative since process start).');
        continue;
      }
      if (!Number.isFinite(g.avg_time)) {
        out.push('gc          : ' + k + ' [pause] ran ' + g.total + 'x; average pause duration is N/A, so pause share cannot be estimated (counters are cumulative since process start).');
        continue;
      }
      const paused = g.total * g.avg_time;
      const share = (paused / uptimeMs) * 100;
      out.push('gc          : ' + k + ' ran ' + g.total + 'x, avg ' + fmt(g.avg_time) + ' ms, every ' + fmt(g.avg_frequency) + ' ms -> ~' + (paused / 1000).toFixed(0) + ' s of pause time (' + share.toFixed(1) + '% of uptime) -> '
        + (share >= 5 ? 'GC is a real contributor and belongs in the report.' : 'GC is not the bottleneck.')
        + ' Counters are cumulative since process start, not limited to this profile window.');
    }
  }
  const mspt = ps.mspt || {};
  const med = mspt.last1m && mspt.last1m.median;
  if (typeof med === 'number' && mspt.game_max_ideal_mspt) {
    out.push('tick budget : median MSPT ' + fmt(med) + ' ms against the ' + mspt.game_max_ideal_mspt + ' ms budget -> '
      + (med > mspt.game_max_ideal_mspt
        ? 'the MEDIAN tick already misses the budget: sustained overload, not an occasional spike.'
        : 'the median tick fits the budget; look at p95/max for spikes.'));
  }
  const ping = pingEnvironment(meta);
  if (ping) {
    const mean = ping.meanMs;
    const p95 = ping.p95Ms;
    const max = ping.maxMs;
    const values = [
      mean === null ? null : 'mean ' + fmt(mean) + ' ms',
      p95 === null ? null : 'p95 ' + fmt(p95) + ' ms',
      max === null ? null : 'max ' + fmt(max) + ' ms',
    ].filter(Boolean).join(', ');
    out.push('ping        : platformStatistics.ping.last15m (' + values + ') -> '
      + (ping.severity === 'severe'
        ? 'severe network-latency signal; investigate route, proxy, client or host networking separately. This is not evidence of Server thread CPU cost.'
        : ping.severity === 'elevated'
          ? 'elevated network-latency signal; investigate network, proxy or client conditions separately. Do not attribute it to Server thread CPU.'
          : 'no high-latency environment warning; ping is not a Server thread CPU metric.'));
  }
  const chunks = ctx && ctx.chunks;
  if (chunks) {
    const cfg = serverProperties(meta);
    const vd = Number(pick(cfg, ['view-distance', 'viewDistance', 'view_distance']));
    const players = Number(ps.player_count || 0);
    const expected = vd > 0 ? Math.pow(2 * vd + 1, 2) * Math.max(1, players) : null;
    if (expected && chunks > expected * 2) {
      out.push('chunks      : ' + chunks + ' chunk(s) loaded for ' + players + ' player(s) with view-distance ' + vd + ' (~' + expected + ' expected from players alone) -> far above what the players need: look for chunk loaders, kept-loaded dimensions, or a huge simulation distance.');
    } else if (!players && chunks > 2000) {
      out.push('chunks      : ' + chunks + ' chunk(s) loaded while 0 players are online -> all of it is force-loaded.');
    }
  }
  return out;
}

/* ------------------------------------------------- JSON export (?raw=1) */
const JSON_HINT = [
  'This payload is spark\'s JSON export (the ?raw=1 / viewer URL form), not a raw sampler report.',
  'It carries platform and system statistics but has NO call tree, so no attribution is possible from it.',
  'Fetch the raw report instead - it always comes from bytebin:',
  '  node spark.mjs fetch https://bytebin.lucko.me/<code>',
  '(or pass the viewer URL https://spark.lucko.me/<code>; the analyzer resolves it to bytebin itself.)',
].join('\n');
/** Detect the metadata-only JSON export, which is a plausible "raw" file for a human to hand over. */
function jsonPayload(buf) {
  if (!buf.length || buf[0] !== 0x7b) return null; // '{'
  let o = null;
  try { o = JSON.parse(buf.toString('utf8')); } catch { return null; }
  return o && typeof o === 'object' && (o.type || o.metadata) ? o : null;
}
function jsonProbeText(j) {
  const md = j.metadata || {};
  const pm = md.platformMetadata || md.platform_metadata || {};
  const ps = md.platformStatistics || md.platform_statistics || {};
  const ss = md.systemStatistics || md.system_statistics || {};
  const lines = [];
  lines.push('report kind   : json (' + (j.type || '?') + ') - METADATA ONLY, no call tree, no attribution possible');
  lines.push('top-level keys: ' + Object.keys(j).join(', '));
  lines.push('metadata keys : ' + Object.keys(md).slice(0, 20).join(', '));
  lines.push('platform      : ' + [pm.name, pm.version, pick(pm, ['minecraftVersion', 'minecraft_version']), pm.brand].filter(Boolean).join(' | '));
  const tps = ps.tps || {};
  if (tps.last1m !== undefined) lines.push('tps           : 1m=' + fmt(tps.last1m) + ' 5m=' + fmt(tps.last5m) + ' 15m=' + fmt(tps.last15m));
  const mspt = ps.mspt || {};
  if (mspt.last1m) lines.push('mspt (1m)     : mean=' + fmt(mspt.last1m.mean) + ' median=' + fmt(mspt.last1m.median) + ' p95=' + fmt(mspt.last1m.percentile95) + ' max=' + fmt(mspt.last1m.max));
  const players = pick(ps, ['playerCount', 'player_count']);
  if (players !== undefined) lines.push('players       : ' + players);
  const world = ps.world || {};
  const ents = pick(world, ['totalEntities', 'total_entities']);
  if (ents !== undefined) lines.push('entities      : ' + ents + ' total across ' + ((world.worlds || []).length) + ' worlds');
  const heap = (ps.memory || {}).heap;
  if (heap) lines.push('heap          : used ' + gb(heap.used) + ' / max ' + gb(heap.max) + ' committed ' + gb(heap.committed));
  const cpu = ss.cpu || {};
  if (cpu.threads) lines.push('cpu           : ' + (pick(cpu, ['modelName', 'model_name']) || '?') + ' x' + cpu.threads + ' process=' + fmt(pick(cpu.processUsage || cpu.process_usage || {}, ['last1m'])) + ' (fraction of all cores)');
  const ssJava = ss.java || {};
  if (Object.keys(ssJava).length) lines.push('java          : ' + [ssJava.vendor, ssJava.version].filter(Boolean).join(' '));
  if (ss.uptime) lines.push('uptime        : ' + (ss.uptime / 1000 / 3600).toFixed(1) + ' h');
  lines.push('sources       : ' + Object.keys(md.sources || {}).length + ' entries');
  lines.push('');
  lines.push(JSON_HINT);
  return lines.join('\n');
}

function writeFetchError(outDir, error, buf) {
  fs.writeFileSync(path.join(outDir, 'error.json'), JSON.stringify({
    error: String(error && error.message ? error.message : error),
    bytes: buf.length,
    sha256: crypto.createHash('sha256').update(buf).digest('hex'),
    recordedAt: new Date().toISOString(),
  }, null, 2));
}

function validateFetchedPayload(buf, outDir) {
  try {
    const prefix = buf.subarray(0, 256).toString('utf8').trimStart().toLowerCase();
    if (prefix.startsWith('<!doctype html') || prefix.startsWith('<html')) {
      throw new Error('fetched payload is HTML, not a raw Spark payload');
    }
    const metadata = jsonPayload(buf);
    if (metadata) {
      throw new Error('fetched payload is Spark metadata JSON without a call tree; fetch the bytebin payload instead');
    }
    const det = detectAndDecode(buf);
    if (!['sampler', 'health', 'heap'].includes(det.kind)) {
      throw new Error('fetched payload has unknown or undecodable Spark schema');
    }
    return det;
  } catch (error) {
    writeFetchError(outDir, error, buf);
    throw error;
  }
}

function probeReport(buf) {
  const det = detectAndDecode(buf);
  const lines = [];
  lines.push('report kind   : ' + det.kind);
  lines.push('bytes         : ' + buf.length);
  if (det.kind === 'sampler') {
    const sd = det.data, meta = sd.metadata || {};
    const nW = (sd.time_windows || []).length;
    const threads = sd.threads || [];
    let total = 0;
    for (const th of threads) { for (const v of (th.times || [])) total += v; }
    lines.push('sampling      : ' + ((meta.end_time - meta.start_time) / 1000).toFixed(1) + ' s, interval ' + meta.interval + ' us (' + Math.round(1000000 / (meta.interval || 1)) + ' Hz)');
    lines.push('start / end   : ' + iso(meta.start_time) + ' -> ' + iso(meta.end_time));
    const agg = meta.data_aggregator || {};
    const aggInfo = aggregatorInfo(agg);
    const aggType = aggInfo.type + (aggInfo.defaulted ? ' (protobuf default)' : '');
    const grouper = ['BY_NAME', 'BY_POOL', 'AS_ONE'][agg.thread_grouper] || ('grouper ' + agg.thread_grouper);
    lines.push('aggregator    : ' + aggType + ' / ' + grouper
      + (agg.tick_length_threshold ? ', tick threshold ' + (agg.tick_length_threshold / 1000) + ' ms' : '')
      + (agg.number_of_included_ticks ? ', included ticks ' + agg.number_of_included_ticks : '')
      + (meta.number_of_ticks ? ', of ' + meta.number_of_ticks + ' total ticks' + (agg.number_of_included_ticks ? ' (' + Math.round((agg.number_of_included_ticks / meta.number_of_ticks) * 100) + '%)' : '') : ''));
    const td = meta.thread_dumper || {};
    lines.push('threads dumped: ' + (['ALL', 'SPECIFIC', 'REGEX'][td.type] || td.type) + (td.ids && td.ids.length ? ' ids=' + td.ids.join(',') : '') + (td.patterns && td.patterns.length ? ' patterns=' + td.patterns.join(',') : ''));
    lines.push('thread nodes  : ' + threads.map((th) => JSON.stringify(th.name) + '=' + (th.children || []).length + ' nodes').join(', '));
    lines.push('windows       : ' + nW + ', total sampled time ' + (total / 1000).toFixed(1) + ' s');
    lines.push('class sources : ' + Object.keys(sd.class_sources || {}).length + ' entries, ' + new Set(Object.values(sd.class_sources || {})).size + ' distinct mods');
    if (meta.sampler_mode !== undefined) lines.push('sampler mode  : ' + (['EXECUTION', 'ALLOCATION'][meta.sampler_mode] || meta.sampler_mode) + ', engine ' + (meta.sampler_engine === 1 ? 'ASYNC' : 'JAVA') + ' ' + (meta.sampler_engine_version || ''));
    lines.push('');
    lines.push(describePlatform(meta));
    lines.push('');
    lines.push('environment verdicts:');
    for (const v of envVerdicts(meta, { chunks: lastWindowChunks(sd) })) lines.push('  - ' + v);
    lines.push('');
    lines.push('validation:');
    for (const th of threads) {
      const t = buildTree(th);
      const i = t.issues;
    const bad = i.badRefs || i.dupRefs || i.cycleRefs || i.unreachable || i.negSelf || hasWindowValidationFailure(i) || Math.abs(i.selfVsRoot) > Math.max(0.5, 1e-6 * t.ROOT);
      lines.push('  - ' + JSON.stringify(th.name) + ': sum(self) - root = ' + (i.selfVsRoot >= 0 ? '+' : '') + i.selfVsRoot.toFixed(2) + ' ms'
      + ', refs out of range ' + i.badRefs + ', repeated refs ' + i.dupRefs + ', cycle refs ' + (i.cycleRefs || 0) + ', unreachable nodes ' + i.unreachable
        + ', negative self ' + i.negSelf + ' (' + i.negSelfMs.toFixed(2) + ' ms, clamped to 0)'
        + (bad ? '  ** DECODING MAY BE WRONG - treat the numbers with care **' : '  ok'));
      for (const w of (i.windowIssues || [])) {
        if (w.suspicious) lines.push('    window #' + w.window + ': self-root = ' + (w.selfVsRoot >= 0 ? '+' : '') + w.selfVsRoot.toFixed(2) + ' ms, negative self ' + w.negativeSelf + ' (' + w.negativeSelfMs.toFixed(2) + ' ms)');
      }
    }
    const ws = sd.time_window_statistics || {};
    const keys = Object.keys(ws).map(Number).sort((a, b) => a - b);
    if (keys.length) {
      lines.push('');
      lines.push('per-window statistics:');
      const statsRows = orderedWindowStats(sd);
      const roots = threads.length === 1 ? rootTotalsByWindow(threads[0], nW) : [];
      const denoms = resolveWindowDenominators(meta, statsRows, roots, nW);
      lines.push(table(statsRows.map(({ stats: w, index }) => {
        const d = denoms.rows[index] || {};
        return ['#' + index, iso(w.start_time).slice(11, 19), w.ticks, fmt(w.tps), fmt(w.mspt_median), fmt(w.mspt_max), d.wallMsPerTick === null ? 'N/A' : fmt(d.wallMsPerTick), fmt(w.cpu_process), w.players, w.entities, w.chunks, ((w.duration || 0) / 1000).toFixed(0) + 's'];
      }), ['win', 'start', 'ticks', 'tps', 'mspt_med', 'mspt_max', 'root/tick', 'cpu', 'players', 'entities', 'chunks', 'dur']));
      lines.push('- window denominator: ' + denoms.aggregate.denominatorSource + '; observed root=' + fmt(denoms.aggregate.rootMs) + ' ms, ticks=' + (denoms.aggregate.ticks === null ? 'N/A' : denoms.aggregate.ticks) + ', wall=' + (denoms.aggregate.wallMsPerTick === null ? 'N/A' : fmt(denoms.aggregate.wallMsPerTick) + ' ms/tick') + '.');
      if (threads.length > 1) lines.push('- per-window root wall time is omitted for multi-thread payloads because sampled thread roots are separate wall-clock streams; use a selected thread for per-tick conversion.');
      if (meta.data_aggregator && aggregatorInfo(meta.data_aggregator).type === 'TICKED' && !denoms.aggregate.denominatorReliable) {
        lines.push('- TICKED window denominator is unavailable because the payload does not expose a reliable per-window included-tick breakdown; do not convert owner percentages into per-tick CPU from this table.');
      }
    }
  } else if (det.kind === 'health' || det.kind === 'heap') {
    const meta = det.data.metadata || det.meta || {};
    lines.push('generated     : ' + iso(meta.generated_time));
    lines.push('');
    lines.push(describePlatform(meta));
    if (det.kind === 'heap') {
      const entries = (det.data.entries || []).slice().sort((a, b) => Number(b.size) - Number(a.size));
      lines.push('');
      lines.push('top heap entries:');
      lines.push(table(entries.slice(0, 25).map((e) => [e.order, Number(e.size), e.instances, e.type]), ['order', 'bytes', 'instances', 'type']));
    }
  } else {
    lines.push('Could not identify the report payload as SamplerData/HealthData/HeapData.');
    lines.push('top-level fields: ' + JSON.stringify(det.scan || []));
  }
  return lines.join('\n');
}
const iso = (ms) => (typeof ms === 'number' ? new Date(ms).toISOString().replace('T', ' ').slice(0, 19) + 'Z' : String(ms));

/* ---------------------------------------------------------------- analyze */
function analyzeSampler(sd, opts) {
  const meta = sd.metadata || {};
  const classSources = sd.class_sources || {};
  const threads = opts.thread === undefined ? (sd.threads || []) : [selectThread(sd.threads || [], opts.thread)];
  const nW = (sd.time_windows || []).length;
  const attrib = (t) => makeAttributor(t, classSources, opts.libs);
  const results = { meta, threads: [], totals: {} };

  for (const th of threads) {
    const t = buildTree(th);
    const issue = t.issues;
    const invalid = issue.badRefs || issue.dupRefs || issue.cycleRefs || issue.unreachable || issue.negSelf || hasWindowValidationFailure(issue)
      || Math.abs(issue.selfVsRoot) > Math.max(0.5, 1e-6 * t.ROOT);
    if (invalid) {
      throw new Error('validation failed for ' + JSON.stringify(th.name)
        + '; refusing attribution. Re-fetch or provide the raw payload.');
    }
    const A = attrib(t);
    const pct = pctOf(t.ROOT);
    const byMethod = new Map(), byClass = new Map(), byPackage = new Map();
    const byOwner = new Map(), byOwnerFolded = new Map(), byLib = new Map(), byRealMod = new Map(), byFamily = new Map();
    const byUnmapped = new Map(), byMixin = new Map(), byAwait = new Map(), byScheduler = new Map();
    const mixinTargets = new Map();
    const srcIdx = sourceIndex(meta);
    const famRes = opts.families.map(([name, re]) => [name, new RegExp('^(' + re + ')$')]);
    const parentTotals = new Map();
    const add = (map, k, self, incl) => {
      let e = map.get(k);
      if (!e) { e = { k, self: 0, incl: 0, n: 0 }; map.set(k, e); }
      e.self += self; e.incl += incl; e.n++;
    };
    const rootCache = new Array(t.nodes.length);
    const rootOfI = (i) => (rootCache[i] !== undefined ? rootCache[i] : (rootCache[i] = rootOf(t.nodes[i].class_name || '?')));
    let unmappedSelf = 0, unmappedIncl = 0, publicSelf = 0, blockingSelf = 0, pacingSelf = 0, schedulerSelf = 0, libSelf = 0, mixinSelf = 0;
    const byPacing = new Map();
    const publicRoots = new Map();
    const mixinOf = new Map();
    for (let i = 0; i < t.nodes.length; i++) {
      const s = t.selfTot[i], inc = t.inclTot[i];
      const cn = t.nodes[i].class_name || '?', mn = t.nodes[i].method_name || '?';
      const mk = cn + '.' + mn;
      add(byMethod, mk, s, inc);
      add(byClass, cn, s, inc);
      add(byPackage, cn.split('.').slice(0, 3).join('.'), s, inc);
      if (!classSources[cn] && !VANILLA.test(cn) && !RUNTIME_ROOT.test(cn) && PUBLIC_ROOT.test(cn)) {
        const pr = rootOfI(i);
        publicRoots.set(pr, (publicRoots.get(pr) || 0) + s);
        publicSelf += s;
      } else if (!classSources[cn] && !VANILLA.test(cn) && !RUNTIME_ROOT.test(cn) && !PUBLIC_ROOT.test(cn)) {
        // No mod id for this class. Its self time is billed to whatever mapped frame sits above
        // it, and its inclusive time can be huge while its self time is ~0 - which is exactly how
        // a mod can own 11% of a tick (or be a victim of one) while never appearing in the tables.
        const r = rootOfI(i);
        const p = t.parent[i];
        const pCn = p >= 0 ? (t.nodes[p].class_name || '?') : '';
        const pMapped = p >= 0 && (!!classSources[pCn] || VANILLA.test(pCn));
        const top = p < 0 || pMapped || rootOfI(p) !== r;
        add(byUnmapped, r, s, top ? inc : 0);
        unmappedSelf += s;
        if (top) unmappedIncl += inc;
      }
      if (s > 0) {
        add(byOwner, A.owner(i), s, 0);
        add(byOwnerFolded, A.foldedOwner(i), s, 0);
        const rawMod = A.nearestMod(i);
        if (rawMod && A.isLib(rawMod)) { add(byLib, 'MOD:' + rawMod, s, 0); libSelf += s; }
        const real = A.nearestRealMod(i);
        const fam = real ? (famRes.find((f) => f[1].test(real)) || [null])[0] : null;
        add(byFamily, fam ? fam : (real ? 'Other mods' : 'Vanilla path (no mod frame)'), s, 0);
        if (real) add(byRealMod, real, s, 0);
        const mi = mixinInfo(mn);
        if (mi) {
          // aggregated per *injector* (the report asks "who hooked this"), with its biggest target kept as evidence
          const miKey = 'injected by ' + mi.mod + (mi.uncertain ? ' (injector not in frame name)' : '');
          add(byMixin, miKey, s, 0);
          const tg = mixinTargets.get(miKey) || new Map();
          const tk = cn + '.' + mi.target;
          tg.set(tk, (tg.get(tk) || 0) + s);
          mixinTargets.set(miKey, tg);
          mixinSelf += s; mixinOf.set(mk, mi);
        }
        if (BLOCKING_RE.test(mk) && hasPacingAncestor(t, i)) { add(byPacing, real ? 'MOD:' + real : '(unmapped frame)', s, 0); pacingSelf += s; }
        else if (BLOCKING_RE.test(mk)) { add(byAwait, real ? 'MOD:' + real : '(unmapped frame)', s, 0); blockingSelf += s; }
        else if (SCHEDULER_RE.test(mk)) { add(byScheduler, real ? 'MOD:' + real : '(unmapped frame)', s, 0); schedulerSelf += s; }
        const p = t.parent[i];
        if (p >= 0) parentTotals.set(label(t, p), (parentTotals.get(label(t, p)) || 0) + s);
      }
    }
    const rank = (map, k) => [...map.values()].map((e) => ({ k: e.k, self: e.self, incl: e.incl, n: e.n })).sort((a, b) => b[k] - a[k]);
    const methods = rank(byMethod, 'self').map((e) => Object.assign(e, { mixin: mixinOf.get(e.k) || null }));
    const classes = rank(byClass, 'self');
    const packages = rank(byPackage, 'self');
    const owners = rank(byOwner, 'self');
    const ownersFolded = rank(byOwnerFolded, 'self');
    const libs = rank(byLib, 'self');
    const realMods = rank(byRealMod, 'self');
    const mixins = rank(byMixin, 'self').map((e) => {
      const tg = mixinTargets.get(e.k);
      let top = null;
      if (tg) for (const [k, v] of tg) if (!top || v > top.v) top = { k, v };
      return Object.assign(e, { topTarget: top ? top.k + ' (' + top.v.toFixed(0) + ' ms)' : '', targets: tg ? tg.size : 0 });
    });
    const awaits = rank(byAwait, 'self');
    const pacing = rank(byPacing, 'self');
    const schedulers = rank(byScheduler, 'self');
    // 'Other mods' hides the second-biggest problem of most packs, so expand it into named mods.
    const families = [];
    for (const e of rank(byFamily, 'self')) {
      if (e.k !== 'Other mods') { families.push(e); continue; }
      const rest = realMods.filter((r) => !famRes.some((f) => f[1].test(r.k)));
      const top = rest.slice(0, opts.otherTop);
      for (const r of top) families.push({ k: 'other: mod:' + r.k, self: r.self, incl: 0, n: r.n });
      const shown = top.reduce((a, r) => a + r.self, 0);
      const left = Math.max(0, rest.length - top.length);
      if (left > 0 || e.self - shown > 0) families.push({ k: 'Other mods (remaining ' + left + ')', self: Math.max(0, e.self - shown), incl: 0, n: 0 });
    }
    const unmappedRoots = rank(byUnmapped, 'incl').map((e) => Object.assign(e, { match: matchSource(e.k, srcIdx) }));
    const unmapped = { self: unmappedSelf, incl: unmappedIncl, roots: unmappedRoots, modLike: unmappedRoots.filter((e) => e.match) };
    const awaitTotal = blockingSelf, libTotal = libSelf, mixinTotal = mixinSelf;

    // per-window
    const perWindow = [];
    const windowStats = orderedWindowStats(sd);
    const windowDenominators = resolveWindowDenominators(meta, windowStats, rootTotalsByWindow(th, nW), nW);
    const aggregateWallMsPerTick = windowDenominators.aggregate.wallMsPerTick;
    const aggregateTickDenominator = windowDenominators.aggregate.ticks;
    const methodKeys = new Map();
    for (let i = 0; i < t.nodes.length; i++) methodKeys.set(key(t, i), (methodKeys.get(key(t, i)) || []).concat(i));
    for (let w = 0; w < nW; w++) {
      let total = 0; for (const r of t.roots) total += (t.nodes[r].times || [])[w] || 0;
      const winMethods = [...methodKeys.entries()].map(([k, idxs]) => {
        let s = 0; for (const i of idxs) s += selfVec(t, i, w);
        return { k, v: s };
      }).filter((x) => x.v > 0).sort((a, b) => b.v - a.v);
      const winOwners = new Map();
      for (const [k, idxs] of methodKeys) {
        let s = 0; for (const i of idxs) s += selfVec(t, i, w);
        if (s <= 0) continue;
        const famOf = (() => { for (const i of idxs) { if (selfVec(t, i, w) <= 0) continue; const real = A.nearestRealMod(i); const f = real ? (famRes.find((x) => x[1].test(real)) || [null])[0] : null; if (f || real) return f || ('mod:' + real); } return 'Vanilla path'; })();
        winOwners.set(famOf, (winOwners.get(famOf) || 0) + s);
      }
      const d = windowDenominators.rows[w] || {};
      perWindow.push({
        window: w,
        total,
        ticks: d.ticks,
        rootMs: d.rootMs,
        wallMsPerTick: d.wallMsPerTick,
        denominatorSource: d.denominatorSource,
        denominatorKind: d.denominatorKind,
        denominatorReliable: d.denominatorReliable,
        topMethods: winMethods.slice(0, 10),
        owners: [...winOwners.entries()].map(([k, v]) => ({ k, v })).sort((a, b) => b.v - a.v).slice(0, 8),
      });
    }

    // top-level structural branches (children of the deepest single root)
    const structure = [];
    const walk = (idx, depth, maxDepth, minPct) => {
      const ch = t.kids[idx].map((c) => ({ c, incl: t.inclTot[c] })).sort((a, b) => b.incl - a.incl);
      for (const e of ch) {
        const p = pct(t.inclTot[e.c]);
        if (p < minPct) continue;
        structure.push({ depth, pct: p, selfPct: pct(t.selfTot[e.c]), label: label(t, e.c), inclMs: t.inclTot[e.c], selfMs: t.selfTot[e.c] });
        if (depth + 1 < maxDepth) walk(e.c, depth + 1, maxDepth, minPct);
      }
    };
    for (const r of t.roots) walk(r, 0, opts.treeDepth, opts.treeMin);

    results.threads.push({
      name: t.name, nodes: t.nodes.length, rootTotal: t.ROOT, nW,
      methods, classes, packages, owners, ownersFolded, libs, realMods, families, perWindow, windowDenominators, aggregateWallMsPerTick, aggregateTickDenominator, structure,
      awaits, pacing, schedulers, blockingSelf, pacingSelf, schedulerSelf, awaitTotal: blockingSelf, libTotal, mixins, mixinTotal,
      publicSelf, publicRoots: [...publicRoots.entries()].map(([k, v]) => ({ k, self: v })).sort((a, b) => b.self - a.self),
      unmapped, issues: t.issues,
      callers: topCallers(t, A, parentTotals, opts),
      dives: autoDives(t, methods, opts),
      drivers: inclusiveDrivers(t, methods, opts),
    });
  }
  results.totals.classSources = Object.keys(classSources).length;
  return results;
}

function topCallers(t, A, parentTotals, opts) {
  const out = [];
  const byMethod = new Map();
  for (let i = 0; i < t.nodes.length; i++) {
    if (t.selfTot[i] <= 0) continue;
    const k = key(t, i);
    if (!byMethod.has(k)) byMethod.set(k, []);
    byMethod.get(k).push(i);
  }
  const top = [...byMethod.entries()].map(([k, idxs]) => ({ k, self: idxs.reduce((a, i) => a + t.selfTot[i], 0) })).sort((a, b) => b.self - a.self).slice(0, opts.callers);
  for (const e of top) {
    const idxs = byMethod.get(e.k);
    const owners = new Map(), folded = new Map(), parents = new Map();
    for (const i of idxs) {
      const o = A.owner(i);
      owners.set(o, (owners.get(o) || 0) + t.selfTot[i]);
      const fo = A.foldedOwner(i);
      folded.set(fo, (folded.get(fo) || 0) + t.selfTot[i]);
      const p = t.parent[i];
      if (p >= 0) parents.set(label(t, p), (parents.get(label(t, p)) || 0) + t.selfTot[i]);
    }
    out.push({
      method: e.k, self: e.self, nodes: idxs.length,
      byOwner: [...owners.entries()].map(([k, v]) => ({ k, v })).sort((a, b) => b.v - a.v).slice(0, 6),
      byFolded: [...folded.entries()].map(([k, v]) => ({ k, v })).sort((a, b) => b.v - a.v).slice(0, 6),
      byParent: [...parents.entries()].map(([k, v]) => ({ k, v })).sort((a, b) => b.v - a.v).slice(0, 6),
    });
  }
  return out;
}

function mergedDump(t, roots, maxDepth, minMs) {
  const lines = [];
  const rec = (list, depth) => {
    const g = new Map();
    for (const i of list) {
      const k = label(t, i);
      let e = g.get(k);
      if (!e) { e = { incl: 0, self: 0, kids: [] }; g.set(k, e); }
      e.incl += t.inclTot[i]; e.self += t.selfTot[i];
      for (const c of t.kids[i]) e.kids.push(c);
    }
    for (const [k, e] of [...g.entries()].sort((a, b) => b[1].incl - a[1].incl)) {
      if (e.incl < minMs) continue;
      lines.push({ depth, label: k, incl: e.incl, self: e.self });
      if (depth < maxDepth) rec(e.kids, depth + 1);
    }
  };
  rec(roots, 0);
  return lines;
}

/** Matched nodes whose own ancestor is also matched: merging both would count that time twice. */
function topMostRoots(t, idxs, cap = 64) {
  const set = new Set(idxs);
  return idxs.filter((i) => {
    let c = t.parent[i], d = 0;
    while (c >= 0 && d++ < cap) { if (set.has(c)) return false; c = t.parent[c]; }
    return true;
  });
}

function autoDives(t, methods, opts) {
  const out = [];
  const groups = new Map();
  for (let i = 0; i < t.nodes.length; i++) {
    const k = key(t, i);
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(i);
  }
  const candidates = methods.filter((m) => m.self > 0).slice(0, opts.maxDives);
  for (const m of candidates) {
    const matched = (groups.get(m.k) || []).filter((i) => t.selfTot[i] > 0);
    // a matched node nested inside another match would otherwise be counted twice
    const roots = topMostRoots(t, matched);
    if (!roots.length) continue;
    const minMs = (opts.diveMin / 100) * t.ROOT;
    out.push({ method: m.k, self: m.self, incl: m.incl, occurrences: roots.length, matched: matched.length, nested: matched.length - roots.length, lines: mergedDump(t, roots, opts.diveDepth, minMs) });
  }
  return out;
}

function inclusiveDrivers(t, methods, opts) {
  const minIncl = (opts.driverMin / 100) * t.ROOT;
  return methods
    .filter((m) => {
      const generic = /^(java\.|jdk\.|sun\.|net\.minecraft\.|net\.neoforged\.)/.test(m.k)
        || /\$\$Lambda\//.test(m.k);
      return !generic && m.incl >= minIncl && m.incl > 0 && (m.self / m.incl) <= opts.driverSelfRatio;
    })
    .sort((a, b) => b.incl - a.incl)
    .slice(0, opts.maxDrivers)
    .map((m) => ({
      method: m.k,
      self: m.self,
      incl: m.incl,
      selfPct: (m.self / t.ROOT) * 100,
      inclPct: (m.incl / t.ROOT) * 100,
      selfToInclusive: m.self / m.incl,
    }));
}

function selectThread(threads, selector) {
  if (!threads.length) throw new Error('no threads in payload');
  if (selector === undefined || selector === null || selector === '') return threads[0];
  const raw = String(selector);
  if (/^\d+$/.test(raw)) {
    const index = Number(raw);
    if (index >= 0 && index < threads.length) return threads[index];
    throw new Error('thread index out of range: ' + raw + ' (available 0-' + (threads.length - 1) + ')');
  }
  const exact = threads.find((th) => th.name === raw);
  if (exact) return exact;
  const like = threads.filter((th) => String(th.name || '').toLowerCase().includes(raw.toLowerCase()));
  if (like.length === 1) return like[0];
  const available = threads.map((th, i) => i + ':' + JSON.stringify(th.name)).join(', ');
  throw new Error('thread not found: ' + raw + '; available threads: ' + available);
}

function assertValidAnalysis(res) {
  const failures = [];
  for (const th of res.threads || []) {
    const i = th.issues || {};
    const bad = i.badRefs || i.dupRefs || i.cycleRefs || i.unreachable || i.negSelf || hasWindowValidationFailure(i)
      || Math.abs(i.selfVsRoot) > Math.max(0.5, 1e-6 * th.rootTotal);
    if (bad) {
      failures.push(JSON.stringify(th.name) + ': self-root=' + Number(i.selfVsRoot || 0).toFixed(2)
        + ' refs=' + (i.badRefs || 0)
        + ' duplicate=' + (i.dupRefs || 0)
        + ' cycles=' + (i.cycleRefs || 0)
        + ' unreachable=' + (i.unreachable || 0)
        + ' negative-self=' + (i.negSelf || 0)
        + ' bad-windows=' + ((i.windowIssues || []).filter((w) => w.suspicious).length));
    }
  }
  if (failures.length) {
    throw new Error('validation failed; refusing to publish attribution or repair ranking. ' + failures.join('; ')
      + '. Re-fetch or provide the raw payload; numbers may be incomplete or malformed.');
  }
}

/* ------------------------------------------------------------------- main */
function parseArgs(argv) {
  const out = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const name = a.slice(2);
      const next = argv[i + 1];
      if (next === undefined || next.startsWith('--')) out[name] = true;
      else { out[name] = next; i++; }
    } else out._.push(a);
  }
  return out;
}
const HELP = [
  'spark report analyzer',
  '',
  '  node spark.mjs fetch   <url|code|file> [--out DIR]',
  '  node spark.mjs probe   <file> [--json]',
  '  node spark.mjs analyze <file> [--out DIR] [--thread NAME|INDEX] [--top N] [--tree-min PCT] [--tree-depth N] [--dive-min PCT] [--dive-depth N] [--max-dives N] [--driver-min PCT] [--max-drivers N] [--callers N] [--libs a,b,c]',
  '  node spark.mjs owners  <file> [--thread NAME|INDEX] [--top N]',
  '  node spark.mjs callers <file> <method-key> [--thread NAME|INDEX] [--top N] [--like]',
  '  node spark.mjs dive    <file> <method-key> [--thread NAME|INDEX] [--like] [--depth N] [--min PCT]',
].join('\n');

function printProbe(buf, asJson) {
  process.stdout.write((asJson ? JSON.stringify(probeJson(buf), null, 1) : probeReport(buf)) + '\n');
}

/** Machine-readable probe: same facts as probeReport, so other tools can consume them. */
function probeJson(buf) {
  const det = detectAndDecode(buf);
  const out = { kind: det.kind, bytes: buf.length };
  const commonMeta = (det.data && det.data.metadata) || det.meta || {};
  const commonPs = commonMeta.platform_statistics || {};
  out.ping = commonPs.ping || null;
  out.pingEnvironment = pingEnvironment(commonMeta);
  if (det.kind !== 'sampler') return out;
  const sd = det.data, meta = sd.metadata || {};
  const ps = meta.platform_statistics || {}, ss = meta.system_statistics || {}, agg = meta.data_aggregator || {};
  const chunks = lastWindowChunks(sd);
  const players = pick(ps, ['player_count', 'playerCount']);
  out.sampling = { seconds: meta.end_time && meta.start_time ? (meta.end_time - meta.start_time) / 1000 : null, interval_us: meta.interval || null, hz: meta.interval ? Math.round(1000000 / meta.interval) : null };
  const aggInfo = aggregatorInfo(agg);
  out.aggregator = {
    type: aggInfo.type,
    raw_type: aggInfo.rawType,
    type_present: aggInfo.present,
    type_defaulted: aggInfo.defaulted,
    thread_grouper: ['BY_NAME', 'BY_POOL', 'AS_ONE'][agg.thread_grouper] || null,
    tick_threshold_us: agg.tick_length_threshold || null,
    included_ticks: agg.number_of_included_ticks || null,
    total_ticks: meta.number_of_ticks || null,
    included_ratio: agg.number_of_included_ticks && meta.number_of_ticks ? agg.number_of_included_ticks / meta.number_of_ticks : null,
  };
  out.platform = meta.platform_metadata || null;
  out.threadDumper = meta.thread_dumper || null;
  out.serverConfigurations = meta.server_configurations || null;
  out.system = { cpu: ss.cpu || null, memory: ss.memory || null, gc: ss.gc || null, java: ss.java || null, os: ss.os || null };
  out.platformStatistics = ps;
  out.metadataQuality = {
    gcCounters: Object.keys(ss.gc || {}).map((name) => ({
      name,
      kind: gcKind(name),
      scope: 'process-uptime cumulative',
      pauseShareAvailable: gcKind(name) === 'pause' && Number.isFinite(ss.gc[name] && ss.gc[name].avg_time),
    })),
    windowTicks: {
      source: 'time_window_statistics.ticks',
      semantics: aggregatorInfo(agg).type === 'TICKED' ? 'included ticks when complete and consistent with number_of_included_ticks' : 'all ticks when present',
    },
  };
  out.sources = { count: Object.keys(meta.sources || {}).length, ids: Object.keys(meta.sources || {}) };
  out.tps = ps.tps || null;
  out.mspt = (ps.mspt && ps.mspt.last1m) || (ps.mspt && ps.mspt.last5m) || null;
  out.ping = ps.ping || null;
  out.pingEnvironment = pingEnvironment(meta);
  out.players = players === undefined ? null : players;
  out.chunks = chunks;
  out.threads = (sd.threads || []).map((th) => ({ name: th.name, nodes: (th.children || []).length }));
  out.windows = (sd.time_windows || []).length;
  out.classSources = { classes: Object.keys(sd.class_sources || {}).length, mods: new Set(Object.values(sd.class_sources || {})).size };
  out.heap = (ps.memory || {}).heap || null;
  out.cpu = ss.cpu ? { model: pick(ss.cpu, ['model_name', 'modelName']) || null, threads: ss.cpu.threads || null, process: pick(ss.cpu.process_usage || {}, ['last1m']) } : null;
  out.issues = (sd.threads || []).map((th) => {
    const t = buildTree(th); const i = t.issues;
    return {
      thread: th.name, selfVsRoot: i.selfVsRoot, refsOutOfRange: i.badRefs, repeatedRefs: i.dupRefs,
      unreachable: i.unreachable, negativeSelf: i.negSelf, negativeSelfMs: i.negSelfMs,
      cycleRefs: i.cycleRefs || 0,
      suspicious: !!(i.badRefs || i.dupRefs || i.cycleRefs || i.unreachable || i.negSelf || hasWindowValidationFailure(i) || Math.abs(i.selfVsRoot) > Math.max(0.5, 1e-6 * t.ROOT)),
      windowIssues: i.windowIssues || [],
    };
  });
  { // same per-window table the text probe prints, so a scripted comparison sees it too
    const statsRows = orderedWindowStats(sd);
    const roots = (sd.threads || []).length === 1 ? rootTotalsByWindow(sd.threads[0], statsRows.length) : [];
    const denoms = resolveWindowDenominators(meta, statsRows, roots, statsRows.length);
    out.windowDenominators = denoms;
    out.perWindow = statsRows.map(({ stats: w, index }) => {
      const d = denoms.rows[index] || {};
      return {
        window: index, start_time: w.start_time, ticks: w.ticks, tps: w.tps, mspt_median: w.mspt_median,
        mspt_max: w.mspt_max, cpu_process: w.cpu_process, players: w.players, entities: w.entities,
        chunks: w.chunks, duration_ms: w.duration, root_ms: d.rootMs, wall_ms_per_tick: d.wallMsPerTick,
        denominator_source: d.denominatorSource, denominator_kind: d.denominatorKind,
        denominator_reliable: d.denominatorReliable,
      };
    });
  }
  out.env = envVerdicts(meta, { chunks });
  return out;
}

function writeArtifacts(dir, kind, payload) {
  fs.mkdirSync(dir, { recursive: true });
  for (const [name, content] of Object.entries(payload)) {
    const p = path.join(dir, name);
    fs.writeFileSync(p, typeof content === 'string' ? content : JSON.stringify(content, null, 1));
  }
  return dir;
}

async function main() {
  const argv = process.argv.slice(2);
  const cmd = argv[0];
  const args = parseArgs(argv.slice(1));
  const num = (v, d) => (v === undefined ? d : Number(v));
  const opts = {
    top: num(args.top, 40), treeMin: num(args['tree-min'], 0.3), treeDepth: num(args['tree-depth'], 14),
    diveMin: num(args['dive-min'], 0.1), diveDepth: num(args['dive-depth'], 7), maxDives: num(args['max-dives'], 12),
    driverMin: num(args['driver-min'], 1), driverSelfRatio: num(args['driver-self-ratio'], 0.25), maxDrivers: num(args['max-drivers'], 20),
    callers: num(args.callers, 15),
    libs: args.libs ? String(args.libs).split(',') : DEFAULT_LIBS,
    families: DEFAULT_FAMILIES,
    otherTop: num(args['other-top'], 6),
    thread: args.thread,
  };
  if (!cmd || cmd === 'help' || cmd === '--help') { process.stdout.write(HELP + '\n'); return; }

  if (cmd === 'fetch') {
    const target = args._[0];
    if (!target) throw new Error('fetch requires a spark URL, code, or file path');
    const outDir = args.out ? String(args.out) : 'spark-' + (extractCode(target) || 'report');
    const dest = await fetchReport(target, outDir);
    const buf = loadBuffer(dest);
    validateFetchedPayload(buf, outDir);
    process.stdout.write('saved ' + dest + ' (' + buf.length + ' bytes)\n\n');
    printProbe(buf, !!args.json);
    return;
  }

  const file = args._[0];
  if (!file) throw new Error(cmd + ' requires a raw payload file (see fetch)');
  const buf = loadBuffer(file);
  const rawSha256 = crypto.createHash('sha256').update(buf).digest('hex');

  const asJson = jsonPayload(buf);
  if (asJson) {
    if (cmd === 'probe') { process.stdout.write((args.json ? JSON.stringify(asJson, null, 1) : jsonProbeText(asJson)) + '\n'); return; }
    process.stderr.write(jsonProbeText(asJson) + '\n');
    process.exit(1);
  }

  if (cmd === 'probe') { printProbe(buf, !!args.json); return; }

  const det = detectAndDecode(buf);
  if (det.kind !== 'sampler') {
    process.stdout.write(probeReport(buf) + '\n');
    if (['analyze', 'owners', 'callers', 'dive'].includes(cmd)) {
      process.stderr.write('\nCall-tree command requires a sampler report; this payload is ' + det.kind + '. Use probe only.\n');
      process.exit(1);
    }
    return;
  }
  const sd = det.data;
  const outDir = args.out ? String(args.out) : path.dirname(path.resolve(file));
  const pct = (v, root) => (v / root) * 100;

  if (cmd === 'analyze') {
    const res = analyzeSampler(sd, opts);
    assertValidAnalysis(res);
    const meta = res.meta;
    const lines = [];
    lines.push('# spark analysis summary');
    lines.push('');
    lines.push(probeReport(buf));
    for (const th of res.threads) {
      lines.push('');
      lines.push('## thread ' + JSON.stringify(th.name) + ' - ' + th.nodes + ' nodes, sampled ' + (th.rootTotal / 1000).toFixed(1) + ' s over ' + th.nW + ' window(s)');
      lines.push('');
      const perTick = (p) => (th.aggregateWallMsPerTick === null ? 'N/A' : ((p / 100) * th.aggregateWallMsPerTick).toFixed(2) + ' ms/tick');
      const row = (e) => [e.self.toFixed(0) + ' ms', pct(e.self, th.rootTotal).toFixed(2) + '%', perTick(pct(e.self, th.rootTotal)), e.k];
      lines.push('### self time by owner (library mods folded into the requesting mod)');
      lines.push(table(th.ownersFolded.slice(0, opts.top).map(row), ['self', '%', 'per tick', 'owner (folded)']));
      if (th.libs.length) {
        lines.push('');
        lines.push('### library / loader frames (already folded into the rows above - not a mod\'s own problem)');
        lines.push(table(th.libs.filter((e) => e.self > 0).slice(0, 20).map(row), ['self', '%', 'per tick', 'library frame']));
      }
      lines.push('');
      lines.push('### self time by ecosystem family (heuristic; "Other mods" expanded to the top ' + opts.otherTop + ')');
      lines.push(table(th.families.map(row), ['self', '%', 'per tick', 'family']));
      lines.push('');
      lines.push('### top methods by self time');
      lines.push(table(th.methods.slice(0, opts.top).map((e) => [e.self.toFixed(0) + ' ms', pct(e.self, th.rootTotal).toFixed(2) + '%', pct(e.incl, th.rootTotal).toFixed(2) + '%', e.n, e.k + (e.mixin ? '   [injected by ' + e.mixin.mod + ' -> ' + e.mixin.target + ']' : '')]), ['self', 'self %', 'incl %', 'nodes', 'method']));
      lines.push('');
      if (th.drivers.length) {
        lines.push('### inclusive drivers (high inclusive, low self; inspect callers before naming a cause)');
        lines.push(table(th.drivers.map((e) => [
          e.self.toFixed(0) + ' ms',
          e.selfPct.toFixed(2) + '%',
          e.incl.toFixed(0) + ' ms',
          e.inclPct.toFixed(2) + '%',
          (e.selfToInclusive * 100).toFixed(1) + '%',
          e.method,
        ]), ['self', 'self %', 'inclusive', 'incl %', 'self/incl', 'method']));
        lines.push('');
      }
      lines.push('### top classes by self time');
      lines.push(table(th.classes.slice(0, Math.min(25, opts.top)).map((e) => [e.self.toFixed(0) + ' ms', pct(e.self, th.rootTotal).toFixed(2) + '%', e.n, e.k]), ['self', '%', 'nodes', 'class']));
      lines.push('');
      lines.push('### call tree (>= ' + opts.treeMin + '% inclusive, depth ' + opts.treeDepth + ')');
      for (const s of th.structure) lines.push('  '.repeat(s.depth) + '[' + s.pct.toFixed(2).padStart(6) + '% incl|' + s.selfPct.toFixed(2).padStart(6) + '% self] ' + s.label);
      lines.push('');
      lines.push('### per-window totals and top methods');
      lines.push(table(th.perWindow.map((w, i) => ['#' + i, (w.total / 1000).toFixed(1) + 's', w.ticks === null ? 'N/A' : w.ticks, w.wallMsPerTick === null ? 'N/A' : fmt(w.wallMsPerTick), w.owners.slice(0, 3).map((o) => o.k + ' ' + pct(o.v, w.total).toFixed(1) + '%').join(', '), w.topMethods.slice(0, 2).map((m) => m.k.split('.').slice(-2).join('.') + ' ' + pct(m.v, w.total).toFixed(1) + '%').join(', ')]), ['win', 'sampled', 'ticks', 'root ms/tick', 'top owners', 'top methods']));
      lines.push('- denominator: ' + (th.windowDenominators.aggregate.denominatorSource || 'N/A') + '; aggregate root=' + fmt(th.windowDenominators.aggregate.rootMs) + ' ms, ticks=' + (th.aggregateTickDenominator === null ? 'N/A' : th.aggregateTickDenominator) + ', wall=' + (th.aggregateWallMsPerTick === null ? 'N/A' : fmt(th.aggregateWallMsPerTick) + ' ms/tick') + '.');
      lines.push('');
      if (th.awaitTotal > 0) {
        lines.push('### blocking / parallel-wait frames (park / wait / sleep: sampled thread is blocked)');
        lines.push('- ' + pct(th.awaitTotal, th.rootTotal).toFixed(2) + '% of sampled time (' + th.awaitTotal.toFixed(0) + ' ms, ' + perTick(pct(th.awaitTotal, th.rootTotal)) + ') is the thread parked.');
        lines.push('- this is latency, not CPU work. The method name alone does not prove which worker or lock caused it; use the surrounding path and A/B validation before recommending a change.');
        lines.push('');
        lines.push(table(th.awaits.map(row), ['self', '%', 'per tick', 'requester of the wait']));
        lines.push('');
      }
      if (th.pacingSelf > 0) {
        lines.push('### intentional tick pacing (idle headroom, not lock/worker contention)');
        lines.push('- ' + pct(th.pacingSelf, th.rootTotal).toFixed(2) + '% of sampled time (' + th.pacingSelf.toFixed(0) + ' ms) is under waitUntilNextTick/managedBlock-style pacing and is excluded from blocking attribution.');
        lines.push(table(th.pacing.map(row), ['self', '%', 'per tick', 'pacing frame']));
        lines.push('');
      }
      if (th.schedulerSelf > 0) {
        lines.push('### scheduler / worker-related frames (not automatically blocking)');
        lines.push('- ' + pct(th.schedulerSelf, th.rootTotal).toFixed(2) + '% of sampled time matched scheduler-like names such as unpark, signalWork, scan, yield, join, or invoke. These may be active work; do not count them as waiting without stack-context evidence.');
        lines.push(table(th.schedulers.map(row), ['self', '%', 'per tick', 'scheduler-like frame']));
        lines.push('');
      }
      if (th.publicSelf > 0) {
        lines.push('### known public/platform libraries (not unknown mods)');
        lines.push('- ' + pct(th.publicSelf, th.rootTotal).toFixed(2) + '% of self time is in known platform or public-library namespaces. This is neutral attribution context, not proof of a missing mod.');
        lines.push(table(th.publicRoots.slice(0, 12).map((e) => [e.self.toFixed(0) + ' ms', pct(e.self, th.rootTotal).toFixed(2) + '%', e.k]), ['self', '%', 'namespace']));
        lines.push('');
      }
      if (th.publicSelf + th.unmapped.self > th.rootTotal * 0.05) {
        lines.push('### attribution confidence');
        lines.push('- known public/platform plus truly unmapped self time is ' + pct(th.publicSelf + th.unmapped.self, th.rootTotal).toFixed(2) + '%. Treat cross-mod root-cause claims and recoverable-time estimates as hypotheses; trace nearest mapped callers and validate with a one-variable A/B test.');
        lines.push('');
      }
      if (th.unmapped.self > 0 || th.unmapped.modLike.length) {
        lines.push('### code with no mod id in the payload (unmapped - read this before blaming the owner tables)');
        lines.push('- ' + pct(th.unmapped.self, th.rootTotal).toFixed(2) + '% of self time (' + th.unmapped.self.toFixed(0) + ' ms) is in classes that class_sources does not map, excluding vanilla and JVM/runtime namespaces.');
        lines.push('- unmapped frames are billed to whatever mapped mod sits above them, so an unmapped mod shows up as an innocent-looking row - or disappears from the tables entirely.');
        if (th.unmapped.modLike.length) {
          lines.push('- ' + th.unmapped.modLike.length + ' unmapped package root(s) match a source declared in this payload: that mod (or loader) shipped classes spark could not map. A small self % with a large incl % means it drives work that gets billed to its host mod.');
          lines.push(table(th.unmapped.modLike.slice(0, 8).map((e) => [e.self.toFixed(0) + ' ms', pct(e.self, th.rootTotal).toFixed(2) + '%', pct(e.incl, th.rootTotal).toFixed(2) + '% incl', e.n, e.k + '   ~ ' + e.match.declared + (e.match.version ? ' ' + e.match.version : '')]), ['self', '%', 'incl', 'frames', 'package root   ~ declared source']));
        }
        const rest = th.unmapped.roots.filter((e) => !e.match).slice(0, 10);
        if (rest.length) {
          lines.push('- other unmapped rows (libraries, generated classes): the incl column is the inclusive time of that root\'s top-most frames and rows may overlap, since one unmapped root can sit inside another.');
          lines.push(table(rest.map((e) => [e.self.toFixed(0) + ' ms', pct(e.self, th.rootTotal).toFixed(2) + '%', pct(e.incl, th.rootTotal).toFixed(2) + '% incl', e.n, e.k]), ['self', '%', 'incl', 'frames', 'package root']));
        }
        lines.push('');
      }
      if (th.mixins.length) {
        lines.push('### mixin-injected frames (who hooked this code)');
        lines.push('- a frame named <mixin>$<hash>$<modid>$<target> is injected by <modid> into the target class, so the class alone points at the wrong owner. Rows are aggregated per injector (the target column shows its heaviest target); ' + pct(th.mixinTotal, th.rootTotal).toFixed(2) + '% of self time is attributable this way.');
        lines.push('');
        lines.push(table(th.mixins.map((e) => row(e).concat([e.topTarget])), ['self', '%', 'per tick', 'injected by', 'heaviest target (self)']));
        lines.push('');
      }
      lines.push('### validation');
      {
        const i = th.issues;
        const bad = i.badRefs || i.dupRefs || i.cycleRefs || i.unreachable || i.negSelf || hasWindowValidationFailure(i) || Math.abs(i.selfVsRoot) > Math.max(0.5, 1e-6 * th.rootTotal);
        lines.push('- sum(self) - root = ' + (i.selfVsRoot >= 0 ? '+' : '') + i.selfVsRoot.toFixed(2) + ' ms (should be ~0), refs out of range ' + i.badRefs + ', repeated refs ' + i.dupRefs + ', cycle refs ' + (i.cycleRefs || 0) + ', unreachable nodes ' + i.unreachable + ', negative self ' + i.negSelf + ' (' + i.negSelfMs.toFixed(2) + ' ms, clamped to 0)' + (bad ? '  ** DECODING SUSPECT - analyze is blocked until this is fixed **' : '  ok'));
        for (const w of (i.windowIssues || [])) {
          if (w.suspicious) lines.push('- window #' + w.window + ': self-root = ' + (w.selfVsRoot >= 0 ? '+' : '') + w.selfVsRoot.toFixed(2) + ' ms, negative self ' + w.negativeSelf + ' (' + w.negativeSelfMs.toFixed(2) + ' ms)');
        }
      }
      lines.push('');
      lines.push('### notes');
      const td = meta.thread_dumper || {};
      lines.push('- threads sampled: ' + (['ALL', 'SPECIFIC', 'REGEX'][td.type] || td.type) + '; only sampled threads are visible.');
      if (opts.thread !== undefined) lines.push('- analysis thread selector: ' + JSON.stringify(opts.thread) + '; this report is intentionally limited to the selected thread.');
      const agg = meta.data_aggregator || {};
      if (agg.type === 1) lines.push('- TICKED aggregation: only ticks longer than ' + (agg.tick_length_threshold / 1000) + ' ms were recorded (' + (agg.number_of_included_ticks || '?') + ' of ' + (meta.number_of_ticks || '?') + ' ticks, ' + (meta.number_of_ticks && agg.number_of_included_ticks ? Math.round((agg.number_of_included_ticks / meta.number_of_ticks) * 100) + '%' : '?') + '). Percentages describe those ticks, not the average tick - tie every conclusion back to how much of the timeline they cover.');
      lines.push('- self = inclusive - sum(children). The owner/family tables use self time; the call tree uses inclusive time. Do not add the two.');
      lines.push('- per-tick values use this thread\'s sampled root wall time divided by the matching window tick denominator (' + (th.windowDenominators.aggregate.denominatorSource || 'N/A') + '); the rolling 1m MSPT is reported separately and is not used as an owner denominator.');
      lines.push('- a self time that is a small percentage but a large inclusive percentage means the work happens in callees (often unmapped or vanilla frames) - follow the dive, not the self column.');
      lines.push('- park/wait/sleep rows are waiting time, not CPU work; unpark/signalWork/scan/yield/join/invoke are scheduler-like and require context before being called blocking.');
      lines.push('- inclusive drivers are trigger candidates, not proven root causes; self, inclusive, callers, and mixin rows overlap and must not be added.');
      lines.push('- known public/platform namespaces are reported separately from truly unmapped code.');
      lines.push('- configured library folding applies only to this library list: ' + opts.libs.join(', '));
      lines.push('- family labels come from heuristic mod-id patterns in the script (DEFAULT_FAMILIES). --libs only changes which mods count as libraries; it does not relabel families. Edit DEFAULT_FAMILIES for a different pack.');
      lines.push('');
      lines.push('Artifacts: analysis.json (machine readable), hotspots.txt, callers.txt, tree.txt');
    }
    const treeLines = res.threads.map((th) => th.structure.map((s) => '  '.repeat(s.depth) + '[' + s.pct.toFixed(2).padStart(6) + '% incl|' + s.selfPct.toFixed(2).padStart(6) + '% self] ' + s.label).join('\n')).join('\n\n');
    const hotLines = res.threads.map((th) => th.dives.map((d) => '# ' + d.method + '  self=' + d.self.toFixed(0) + 'ms across ' + d.occurrences + ' node(s)' + (d.nested ? ' (' + d.nested + ' nested inside another match, merged once)' : '') + '\n' + d.lines.map((l) => '  '.repeat(l.depth) + '[' + pct(l.incl, th.rootTotal).toFixed(2).padStart(6) + '% incl|' + pct(l.self, th.rootTotal).toFixed(2).padStart(5) + '% self] ' + l.label).join('\n')).join('\n\n')).join('\n\n');
    const callerLines = res.threads.map((th) => th.callers.map((c) => {
      const head = '# ' + c.method + '  self=' + c.self.toFixed(0) + 'ms (' + pct(c.self, th.rootTotal).toFixed(2) + '%) over ' + c.nodes + ' node(s)';
      const own = c.byOwner.map((o) => '    owner  ' + o.v.toFixed(0).padStart(7) + 'ms ' + pct(o.v, th.rootTotal).toFixed(2).padStart(5) + '%  ' + o.k);
      const fold = (c.byFolded || []).map((o) => '    folded ' + o.v.toFixed(0).padStart(7) + 'ms ' + pct(o.v, th.rootTotal).toFixed(2).padStart(5) + '%  ' + o.k);
      const par = c.byParent.map((o) => '    parent ' + o.v.toFixed(0).padStart(7) + 'ms ' + pct(o.v, th.rootTotal).toFixed(2).padStart(5) + '%  ' + o.k);
      return [head, ...own, ...fold].concat(par.length ? ['    -- direct parent frames --'] : []).concat(par).join('\n');
    }).join('\n\n')).join('\n\n');
    writeArtifacts(outDir, 'sampler', {
      'analysis.json': {
        generatedAt: new Date().toISOString(),
        analyzer: 'spark-profiler-analysis 2026-10-08',
        node: process.version,
        source: path.resolve(file),
        rawSha256,
        options: opts,
        metadata: res.meta,
        threads: res.threads.map((t) => ({
          name: t.name, nodes: t.nodes, rootTotal: t.rootTotal, nW: t.nW, issues: t.issues,
          methods: t.methods.slice(0, 300), drivers: t.drivers, classes: t.classes.slice(0, 200),
          packages: t.packages.slice(0, 120), owners: t.owners, ownersFolded: t.ownersFolded,
          libs: t.libs, realMods: t.realMods, families: t.families, awaits: t.awaits,
          awaitTotal: t.awaitTotal, blockingSelf: t.blockingSelf, pacing: t.pacing, pacingSelf: t.pacingSelf, schedulerSelf: t.schedulerSelf, schedulers: t.schedulers,
          publicSelf: t.publicSelf, publicRoots: t.publicRoots, mixins: t.mixins, mixinTotal: t.mixinTotal,
          unmapped: { self: t.unmapped.self, incl: t.unmapped.incl, roots: t.unmapped.roots },
          perWindow: t.perWindow, windowDenominators: t.windowDenominators, aggregateWallMsPerTick: t.aggregateWallMsPerTick, aggregateTickDenominator: t.aggregateTickDenominator,
          structure: t.structure, callers: t.callers,
          dives: t.dives.map((d) => ({ method: d.method, self: d.self, incl: d.incl, occurrences: d.occurrences, matched: d.matched, nested: d.nested, lines: d.lines })),
        })),
      },
      'tree.txt': treeLines,
      'hotspots.txt': hotLines,
      'callers.txt': callerLines,
      'summary.txt': lines.join('\n'),
    });
    process.stdout.write(lines.join('\n') + '\n');
    process.stdout.write('\nArtifacts written to ' + path.resolve(outDir) + '\n');
    return;
  }

  // single-thread targeted commands
  const th = selectThread(sd.threads || [], opts.thread);
  const t = buildTree(th);
  const singleCheck = {
    threads: [{ name: th.name, nodes: t.nodes.length, rootTotal: t.ROOT, issues: t.issues }],
  };
  assertValidAnalysis(singleCheck);
  const A = makeAttributor(t, sd.class_sources || {}, opts.libs);
  if (cmd === 'owners') {
    const map = new Map(), fold = new Map();
    for (let i = 0; i < t.nodes.length; i++) {
      const s = t.selfTot[i];
      if (s <= 0) continue;
      map.set(A.owner(i), (map.get(A.owner(i)) || 0) + s);
      const fo = A.foldedOwner(i);
      fold.set(fo, (fold.get(fo) || 0) + s);
    }
    const sort = (m) => [...m.entries()].sort((a, b) => b[1] - a[1]).slice(0, opts.top).map(([k, v]) => [v.toFixed(0) + ' ms', pct(v, t.ROOT).toFixed(2) + '%', k]);
    process.stdout.write('thread: ' + JSON.stringify(th.name) + '\n\n');
    process.stdout.write(table(sort(map), ['self', '%', 'nearest mod frame (library mods kept)']) + '\n\n');
    process.stdout.write(table(sort(fold), ['self', '%', 'library mods folded into the requesting mod']) + '\n');
    return;
  }
  const needle = args._[1];
  if (!needle) throw new Error(cmd + ' requires a method key');
  const like = !!args.like;
  const match = (i) => (like ? key(t, i).includes(needle) : key(t, i) === needle);
  if (cmd === 'dive') {
    const all = [];
    for (let i = 0; i < t.nodes.length; i++) if (match(i)) all.push(i);
    if (!all.length) { process.stdout.write('no node matched ' + JSON.stringify(needle) + (like ? '' : ' (try --like)') + '\n'); return; }
    // a matched node nested inside another match would otherwise be counted twice
    const roots = topMostRoots(t, all);
    const nested = all.length - roots.length;
    const incl = roots.reduce((a, i) => a + t.inclTot[i], 0);
    process.stdout.write('thread: ' + JSON.stringify(th.name) + '\n');
    process.stdout.write('matched ' + all.length + ' node(s)' + (nested ? ' (' + nested + ' nested inside another match, merged once)' : '') + ', total inclusive ' + incl.toFixed(0) + ' ms (' + pct(incl, t.ROOT).toFixed(2) + '%)\n\n');
    const minMs = (num(args.min, 0.1) / 100) * t.ROOT;
    const lines = mergedDump(t, roots, num(args.depth, 8), minMs);
    process.stdout.write(lines.map((l) => '  '.repeat(l.depth) + '[' + pct(l.incl, t.ROOT).toFixed(2).padStart(6) + '% incl|' + pct(l.self, t.ROOT).toFixed(2).padStart(5) + '% self] ' + l.label).join('\n') + '\n');
    return;
  }
  if (cmd === 'callers') {
    const idxs = [];
    for (let i = 0; i < t.nodes.length; i++) if (match(i)) idxs.push(i);
    if (!idxs.length) { process.stdout.write('no node matched ' + JSON.stringify(needle) + (like ? '' : ' (try --like)') + '\n'); return; }
    const total = idxs.reduce((a, i) => a + t.selfTot[i], 0);
    const owners = new Map(), folded = new Map(), parents = new Map();
    for (const i of idxs) {
      owners.set(A.owner(i), (owners.get(A.owner(i)) || 0) + t.selfTot[i]);
      const fo = A.foldedOwner(i);
      folded.set(fo, (folded.get(fo) || 0) + t.selfTot[i]);
      const p = t.parent[i];
      if (p >= 0) parents.set(label(t, p), (parents.get(label(t, p)) || 0) + t.selfTot[i]);
    }
    process.stdout.write('thread: ' + JSON.stringify(th.name) + '\n');
    process.stdout.write('self=' + total.toFixed(0) + ' ms (' + pct(total, t.ROOT).toFixed(2) + '%) across ' + idxs.length + ' node(s)\n\nby owner:\n');
    process.stdout.write(table([...owners.entries()].sort((a, b) => b[1] - a[1]).slice(0, opts.top).map(([k, v]) => [v.toFixed(0) + ' ms', pct(v, t.ROOT).toFixed(2) + '%', k]), ['self', '%', 'nearest mod frame']) + '\n\nby owner with library mods folded into requester:\n');
    process.stdout.write(table([...folded.entries()].sort((a, b) => b[1] - a[1]).slice(0, opts.top).map(([k, v]) => [v.toFixed(0) + ' ms', pct(v, t.ROOT).toFixed(2) + '%', k]), ['self', '%', 'folded owner']) + '\n\nby direct parent:\n');
    process.stdout.write(table([...parents.entries()].sort((a, b) => b[1] - a[1]).slice(0, opts.top).map(([k, v]) => [v.toFixed(0) + ' ms', pct(v, t.ROOT).toFixed(2) + '%', k]), ['self', '%', 'parent']) + '\n');
    return;
  }
  throw new Error('unknown command: ' + cmd + '\n' + HELP);
}

main().catch((e) => { process.stderr.write('error: ' + (e && e.message ? e.message : String(e)) + '\n'); process.exit(1); });
