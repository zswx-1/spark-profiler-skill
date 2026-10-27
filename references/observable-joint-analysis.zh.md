# Spark + Observable 联合分析参考

这份参考文档只在用户同时提供 Spark 与 Observable 报告时读取。Spark 的
`references/data-format.md` 仍是 Spark protobuf、self、owner、family 和 validation
语义的权威来源；Observable 的字段语义以 `$observable-profiler-analysis` 的
`references/data-model.md` 为准。

## 数据源

### Spark

- viewer URL 转 bytebin payload；
- 保留 `raw.bin`、`source.json`、SHA-256；
- `probe` → `analyze` → `callers/dive/owners`；
- 先明确线程、聚合器、TICKED 阈值、included ticks 和 validation。

### Observable

- viewer URL `/p/<id>` 优先转为 `https://observable.tas.sh/v1/get/<id>`；
- 保存原始 JSON、实际 endpoint、抓取时刻、字节数和 SHA-256；
- 校验 status、JSON content-type、`data`/`diagnostics`、`data.blocks`/`data.entities`
  group 数组；
- `diagnostics.start` 是报告开始时间，`fetchedAt` 只是本地下载时间，不能混用。

## 时间窗

统一为半开区间：

```text
Spark:      [spark.start, spark.end)
Observable: [diagnostics.start, diagnostics.start + duration)
```

计算并报告：

- `overlap = max(0, min(endA,endB) - max(startA,startB))`
- 非重叠时的 `gap`
- `containment`（一个窗口是否完整包含于另一个）
- `union duration`

窗口不重叠时，只能写相邻状态交叉验证；不得把两份报告的百分比、ms/tick
或对象总量相加。窗口重叠也只说明可交叉验证，不会让两个 profiler 共享一个分母。

## Observable 加权

当前 Observable payload 中：

```text
data.ticks = observed total ticks（若为 numeric）
否则 N = max(entry.ticks) = inferred total ticks

entry rate = ns / active tick
entry active time = rate × entry.ticks
entry window contribution =
  rate × entry.ticks / N / 1,000,000 ms/tick
```

先按 entry 加权，再按 `(group,type)` 或 namespace 聚合。报告至少保留：

- entry 数；
- active ticks；
- active ns；
- weighted ms/tick；
- 单对象最大值；
- 对象稳定 key。

`data.blocks` 可能混合 block entity、random block tick、fluid tick，应写为
`block-side entries`。它不是 Spark `Level.tickBlockEntities` 的同义词。实体优先按
`(group, entityId)`，方块按 `(group, position, type)`；实体位置会移动，只作描述。

## Trace 边界

Observable 有两种 trace：

- global `data.traces`：异步 stack sample；
- entry 内 `traces`：某个对象的 per-entry 关联路径。

两者都不是对象的 self time。`count` 是采样出现次数，不是时间、CPU 周期或调用总数；
父子 count 不能相加。synthetic root（例如 `java.lang.Thread.hash` 或
`tickBlockEntities`）不能直接归责给 Mod。global trace 和 per-entry trace 要分开写；
空 trace、`count=0` 或只到 wrapper 不代表对象 `rate` 为零。

## 交叉解释

| 数据 | 能回答 | 不能回答 |
|---|---|---|
| Spark Server thread self | 被采样线程上时间实际落在哪些帧 | 所有线程的全局 CPU |
| Spark inclusive/callers/dive | 哪些调用链触发或包含工作 | 可直接回收的收益 |
| Spark owner/family | 代码/生态归因 | 与其他视图相加后的总成本 |
| Observable object rate | 哪些对象、维度、类型持续计时 | 全局 MSPT 或 Mod owner |
| Observable global/per-entry trace | 路径共现 | 方法耗时、调用总数 |

禁止相加：

- Observable block/entity weighted ms/tick；
- Observable trace count；
- Spark MSPT；
- Spark owner/family/self/inclusive；
- Spark blocking、scheduler、mixin。

最多可以在同一时间窗、明确 tick period 假设下计算：

```text
instrumented coverage ≈ Observable block/entity ms/tick / independently derived tick period
```

这个 coverage 只是 Observable 插桩覆盖的近似比例，不是全局 CPU 占比，也不是
Spark 的可回收收益。

## 联合证据等级

- L0：schema、版本、时间、SHA-256、线程/聚合器元数据；
- L1：单窗口 Spark validation self 或 Observable weighted object rate；
- L2：相邻 Observable 窗口中对象 key、坐标、active ticks 和 weighted ms/tick 稳定；
- L3：Spark 与 Observable 时间重叠，且调用路径/对象/维度互相对应；
- L4：只改一个变量的 A/B，warm-up 后至少多组成对窗口，目标指标下降且 workload
  没有被简单移除。

## A/B 清单

每次只改一个变量：

1. 记录改动前状态、对象数、active ticks、机器产量和功能状态；
2. warm-up 到稳定状态；
3. 使用相同 Spark 参数和相同 Observable duration/schema 采样；
4. 尽量做至少三组成对窗口；
5. 比较 Spark MSPT median/P95/max、TICKED included ratio、目标 self/inclusive、
   Observable weighted ms/tick、entry 数和 active ticks；
6. 如果停用机器导致对象数或 active ticks 下降，把结果写成
   “移除 workload 后的下降”，不能直接写成优化器减少了同等成本；
7. 功能、产量、存储内容或 chunk 状态异常时停止并回滚。

