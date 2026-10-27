# 报告骨架（中文）

按下列结构撰写；每一节都必须有数据支撑，数字后面给出证据（方法名 / 占比 / 每刻毫秒）。
若用户使用其他语言，保持同样的节次顺序。所有数字都取自 `summary.txt` / `probe` 的同一份产物。

## 一、结论速览

- 一句话结论：是单线程工作量过大，还是 GC / IO / 环境问题。
- 表格：TPS(1m/5m/15m)、MSPT(均值/中位/P95/最大)、主线程占用、进程 CPU、GC 频次与耗时、
  堆使用、实体/区块/玩家数。
- 环境判语（summary 的 environment verdicts）：进程 CPU 是否饱和（"只用 1.x / 16 核 →
  不是算力问题，加核无效"）、swap 是否吃满（会放大一切等待类开销）、GC 是否值得怀疑、
  区块数相对视野/玩家数是否异常（1 个玩家却有上万个已加载区块 = 强制加载）。
- 逐条说明环境判语如何影响结论：纳入、排除，或作为待验证干扰项；缺失值写 N/A。
- 明确写出"瓶颈在哪个环节、影响多少 ms/刻"，并说明分母是 MSPT 中位数还是均值。

## 二、报告元数据与环境

采样时长与频率、原始 payload SHA-256、分析器/Node 版本、聚合模式（SIMPLE/TICKED、tick 阈值、**纳入刻数 N of M 及百分比** - 低于
100% 时必须写明"这份 profile 描述的是慢刻，不是平均刻"）、采样了哪些线程（SPECIFIC /
BY_POOL / AS_ONE，未采样的线程不能声称空闲）、平台与版本、Mod 数量、主机 CPU/内存/交换分区、
Java 与 GC、关键 server.properties、玩家延迟。最后附 summary 的 validation 自检结果。

若同时有 Observable，另列其 JSON endpoint、raw bytes/SHA-256、Observable 版本、
`diagnostics.start/duration/data.ticks`（注明 observed/inferred）、group/type entry 数、
block-side/entity-side 加权公式和数据质量告警。Observable 的 diagnostics user 只能作为
弱同实例证据，需结合版本、loader、Mods/System Report。

## 三、稳定性判断

逐窗表格（窗口、刻数、TPS、MSPT 中位/最大）。说明是"恒定超载"还是"偶发尖峰"；
若有尖峰，单独列出并说明无法从聚合数据定位、需复现。

若有 Observable，使用半开区间 `[start,end)` 对齐 Spark 与每个 Observable 窗口，报告
gap、overlap、containment 和 union；窗口不重叠时只能写相邻状态交叉验证，不能合并百分比
或毫秒。

## 四、单刻时间去向（调用树）

- 代码块形式的调用树，标注占比与 ≈ms/刻。
- 分节说明每个主要分支：AE2/存储、机械方块实体、时间加速、实体、区块、网络等。
- 每个热点给出：方法名、占比、每刻毫秒、触发它的上层路径。

## 五、责任归因（多个视图，不可相加）

| 视图 | 回答的问题 | 来源 |
|---|---|---|
| 折叠后的归属方（folded owner） | 谁请求了这段工作 | summary "self time by owner (library mods folded…)" |
| 未折叠的归属方（owner） | 谁的代码真的在跑 | 'analysis.json' 的 'owners'，或 'owners' 子命令的第一张表 |
| 生态族 Family | 哪个整合包生态该负责 | summary "self time by ecosystem family" |
| 未映射 Unmapped | 哪些代码的归属是猜的 | summary "code with no mod id" |

- 这几张表口径不同，**绝不可相加**；inclusive 列在多个视图下都会重叠。折叠与未折叠的差别是
  实打实的：本次实测 ae2 28.39% → 29.22%、neoforge 6.73% → 11.23%（'--libs' 可改库名单）。
- 另附库/加载器帧小节（summary "library / loader frames"）：这些是别人的代码，不是该 Mod
  自己的问题。
- 若 summary 报了下面任一小节，各补一段说明它如何改变归属结论：
  - **阻塞/并行等待帧**（park/wait/sleep）：这是主线程阻塞，是延迟不是 CPU 工作；
    按"请求方"归因到模组，但必须结合调用链和 A/B 验证。unpark/signalWork/scan/yield/join/invoke
    放在 scheduler 小节，不自动当作等待。
  - **mixin 注入帧**（`wrapOperation$<hash>$<modid>$<target>` 等）：类名只是宿主，动手的是
    注入者；按 summary "mixin-injected frames" 的注入者归因。
  - **公共库/未映射模组**：公共库命名空间不等于未知 Mod；真正 unmapped 若 self 超过 5%，
    所有唯一根因与收益结论降级为关联假设。self 很小但 inclusive 很大、且包根与 payload 声明的 sources 匹配时，
    说明该模组的成本被记到宿主 Mod 头上，需要顺着 callers 上溯再下结论。

### Observable 交叉定位（不并入 Spark 归因表）

| Observable 视图 | 回答的问题 | 不能回答的问题 |
|---|---|---|
| group/dimension weighted ms/tick | 哪个报告返回的世界/虚拟维度持续计时 | 全局 MSPT 或加载区块总数 |
| `(group,type)` / namespace | 哪类对象数量和外围计时最大 | Spark owner 或完整 Mod CPU |
| per-entry rate | 哪个对象的 active-tick 包络较高 | 可直接回收的 self |
| global/per-entry trace | 哪些调用路径共现 | 方法耗时、调用总数、因果 |

`data.blocks` 可能混合 block entity/random/fluid tick，应称为 `block-side entries`；
Observable object rate 按 `rate × entry.ticks / N / 1e6 ms/tick` 加权。N 来自 numeric
`data.ticks` 时标 observed，否则标 inferred。Observable block/entity ms/tick、trace
count 与 Spark MSPT/owner/family/self/inclusive/blocking/mixin 绝不相加；最多作为同时间窗
的 L3 交叉证据。

## 六、优化建议（按收益排序）

分梯队：配置修改（零成本）→ 布局 / 网络结构调整 → 关停或节流特定机械。
每条建议必须写出：对应方法、当前占比、证据等级、可逆性、预期比较指标、停止/回滚条件；
不得从 inclusive 直接承诺可回收的 ms/刻。
重复 tick（加速类模组）与并行等待类问题要分别点名，并说明该找模组作者还是改配置。

## 七、复现与验证

给出与本次完全一致的采样命令（相同聚合模式、阈值、线程集合与采样间隔），列出改动前后要对比的
2-3 个指标（首选 MSPT 中位数、纳入刻比例和逐窗结果；单次只改一件事，避免混淆归因）。
若有 Observable，保持相同的 duration/schema/玩家位置/机器输入；先 warm-up，尽量做至少
三组成对窗口，比较 Observable weighted block/entity ms/tick、entry 数、active ticks、
对象 key 和 Spark 目标 self/inclusive。停用机器导致对象数或 active ticks 下降时，报告
workload change，不把移除的工作量直接当成优化收益。

## 八、方法与局限

说明数据来源（bytebin protobuf；如有则附 Observable `/v1/get` JSON）、raw SHA-256、
self = inclusive - sum(children) 的定义、只采样了哪些线程、
聚合模式带来的偏差（TICKED 只覆盖慢刻）、validation 自检的结论（若打了
`** DECODING SUSPECT **`，先重新抓取再出报告）、未映射 self 占比、mixin 注入帧的归属规则、
公共库与真正未知包的区分、阻塞与 scheduler 的区分，以及缺失的数据（如 Metrics 时间序列、单刻尖峰原因）。
还要说明 Observable 是对象级外围计时和异步 trace，不是完整全局 profiler；global trace 与
per-entry trace 分开；block-side 可能混合类型；trace count 不是时间；未知/非法字段、
重复对象 key 和 inferred ticks 的限制。

---

写作要求：

- 不要罗列无关的 top 方法；只保留能支撑结论的条目。
- 归因的三个视图之间不可相加，注意标注口径；inclusive 与 self 也不要混用。
- 等待时间（park/wait/sleep）是延迟，不是 CPU 工作，不得计入"CPU 成本"；scheduler 帧需上下文确认。
- inclusive drivers 只表示触发链候选；top methods、inclusive、blocking、mixin、dive 子树可能重叠，不能相加。
- 无法定位的问题要如实说明（例如"该尖峰无法从聚合数据定位"）。
- 用 write 工具写文件，不要在 run_code 的模板字符串里拼 Markdown（反引号会截断字面量）。
