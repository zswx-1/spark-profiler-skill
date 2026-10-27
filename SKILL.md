---
name: spark-profiler-analysis
description: 分析 Minecraft spark 性能分析报告（spark.lucko.me 链接、bytebin 代码或原始 payload 文件），并在用户同时提供 Observable 报告时进行时间对齐和联合归因。抓取 protobuf payload、解码采样调用树，把 self 时间归因到所属 Mod 与原生或库代码，结合 Observable 的对象级 block/entity 计时定位机器和自动化放大链，产出基于证据的 TPS/卡顿报告与按收益排序的修复建议。
whenToUse: 提供了 spark 性能分析 URL、代码或原始 payload，任务是解释卡顿、TPS 损失或 CPU 热点；或对比 spark 报告；若同时提供 Observable 链接，联合读取对象级计时和 trace 做时间交叉验证。不适用于编写 spark 插件或只查看网页 UI。
disable-model-invocation: false
---

# Spark 性能分析报告解读

spark 报告是一段 protobuf payload，而不是网页。查看器 URL 只是一个 Next.js 外壳，它从 bytebin 下载数据，因此手工阅读页面既慢又会丢失信息。应当改为解码 payload，然后撰写一份点名方法、Mod 与每刻开销的报告。

本 skill 默认是**只读分析**：只保存原始 payload 的副本和分析产物，不修改服务器、世界、配置、模组、脚本或 skill 文件，不自动重载/重启，也不执行修改性 A/B。任何修改或安装前必须先询问用户并得到明确确认。

## 工作流程

约定 MS = 本 skill 的资源基目录；分析器为 MS/scripts/spark.mjs（Node 18+，仅用标准库，无需安装）。若环境提供 load_workspace_dependencies，用它获取解释器。

1. **抓取** payload（接受查看器 URL、裸 bytebin 代码，或本地文件）：
   ~~~bash
   node MS/scripts/spark.mjs fetch https://spark.lucko.me/<code> --out spark-<code>
   ~~~
   它会立即打印 probe 摘要，并在 `source.json` 记录来源、抓取时间、时区、字节数和 SHA-256。仅当用户已经提供了文件时才跳过这一步。
   - viewer URL 自动改为 bytebin；不要依赖网页 HTML。
   - 403/404、DNS、超时、重定向超限、空响应、HTML、JSON metadata、截断 payload、未知 schema 或 Node OOM 都是**获取/解码失败**：保留输入和错误，不归因、不排序收益，请用户提供 raw payload 或重新采样。
   - `fetch` 在保存 raw.bin 后会立即做 kind/schema 校验；HTML、metadata JSON 和 unknown/truncated payload 以失败状态结束，并在输出目录保留 `error.json`。
   - protobuf enum 缺失字段按 schema 默认值解析，并在机器可读输出保留 `raw/present/defaulted` provenance；未知非零 enum 不得显示为 `undefined`。

2. **先 probe，再分析。** 从 probe 的输出中读取采样参数；所有百分比都取决于这些参数：
   - 类型（sampler / health / heap）、时长、采样间隔与采样率；
   - 聚合模式（aggregator type）、tick 阈值、纳入刻数（含 included/total 与百分比）；
   - 线程转储器：采样了哪些线程（分析器中只包含这些线程）；
   - TPS/MSPT、逐窗表格、CPU/GC/内存、Mod 数量；
   - 环境判语（environment verdicts）：CPU 是否饱和、swap 是否吃满、GC 占比是否值得怀疑、
     区块数相对视野与玩家数是否异常、ping 是否出现网络延迟信号（ping 不能归因于
     Server thread CPU）。
   随时可用 'node MS/scripts/spark.mjs probe <raw.bin>' 重新运行；'probe <raw.bin> --json'
   给出机器可读的核心字段、平台/系统、线程转储器、server.properties、逐窗表格、校验和环境判语，
   便于脚本化对比两次采样。JSON 还包含顶层 `ping`/`pingEnvironment`、`metadataQuality` 和
   `windowDenominators`；后者记录每窗口 `root_ms`、`ticks`、`wall_ms_per_tick`、分母来源和
   是否可靠。缺失值写 `null`/`N/A`，不填 0、不推算。

3. **分析** sampler payload：
   ~~~bash
   node MS/scripts/spark.mjs analyze <raw.bin> --out <dir>
   ~~~
   实测：5.08 MB payload、33832 个树节点、2454 个类、176 个 Mod 约需 0.7 秒（本机 Node 24，三次
   实测 686/694/684 ms）。随后按以下顺序阅读：
   - 'summary.txt' - 元数据与环境判语、折叠库后的归属方表、库/加载器帧表、生态族表、top 方法/类、
     调用树、逐窗细分（breakdown）、阻塞/并行等待帧、未映射代码、mixin 注入帧、validation 自检，
     以及 notes 区块；未折叠的归属方表在同一份 'analysis.json' 的 'owners' 字段里（'owners' 子命令
     会把两张表一起打印）；
   - 'callers.txt' - 每个 top 方法的调用者（最近的 Mod 栈帧，以及折叠库之后的视图）
     外加直接父栈帧；
   - 'hotspots.txt' - top 方法自动展开的子树，即这些方法内部是什么；
   - 'tree.txt' - 围绕根节点裁剪后的 inclusive/self 调用树；
   - 'analysis.json' - 同样的数字，机器可读，需要精确值时使用；含 raw SHA-256、Node/分析器版本、
     完整选项、validation、blocking 与 scheduler 分离、公共库命名空间及 inclusive drivers。

4. **下钻** summary 暴露出的任何问题，按方法 key
   （'fully.qualified.ClassName.methodName'）：
   ~~~bash
   node MS/scripts/spark.mjs dive    <raw.bin> appeng.me.service.StorageService.updateCachedStacks --thread "Server thread" --depth 8 --min 0.05
   node MS/scripts/spark.mjs callers <raw.bin> PalettedContainer.get --thread "Server thread" --like
   node MS/scripts/spark.mjs callers <raw.bin> "net.minecraft.world.item.ItemStack.<init>" --thread 0
   node MS/scripts/spark.mjs owners  <raw.bin> --thread "Server thread" --top 60
   ~~~
   在不确定确切 key 时，'--like' 会做子串匹配。'dive' 会把该方法的所有出现合并成
   一棵聚合子树，因此即使该方法出现数百次，它也能回答"这个热点内部是什么"。重复出现或
   互相嵌套的方法（递归、包装器）只会被合并计一次，'dive' 与 'hotspots' 的输出都会注明
   合并了多少个嵌套匹配。
   默认下钻第一个线程只是兼容行为，不得假定它一定是服务端线程。先从 probe 列出线程名、节点数和
   根总量；需要明确选择时使用 `--thread NAME` 或 `--thread INDEX`。`analyze` 默认覆盖全部线程，
   指定 `--thread` 后只分析所选线程，并在报告中写明选择。多线程结果不得把单个线程百分比当成全局百分比。

5. **撰写报告**，使用用户的语言，遵循 MS/references/report-template.zh.md（中文骨架），
   或在使用其他语言时保持相同的章节顺序。用 write 工具创建文件 - 模板字面量的
   陷阱见下文"常见陷阱"。

6. **如果用户同时提供 Observable 报告，执行联合分析。**
   - 先读取 `$observable-profiler-analysis` 的 `SKILL.md`；需要字段语义时读取其
     `references/data-model.md`，不要重新发明 `rate` 或 trace 口径。
   - 对每个 Observable viewer URL 优先请求 `https://observable.tas.sh/v1/get/<id>`；
     保存 JSON、endpoint、抓取时刻、字节数和 SHA-256。页面 HTML 或 `__data.json` 只是壳，
     不得作为对象数据源。
   - 对齐绝对时间：Spark 用 payload `start/end`，Observable 用
     `diagnostics.start` + `diagnostics.duration`，区间按半开
     `[start, end)` 计算 `gap`、`overlap`、`containment` 和 `union`。下载时间不是报告时间。
   - Observable 的 `data.ticks` 若存在且为 numeric 才标为 observed；否则
     `max(entry.ticks)` 只能标为 inferred，不能伪装成独立窗口刻数。`rate` 是
     `ns / active tick`，对象窗口贡献为
     `rate × entry.ticks / total_ticks / 1e6 ms/tick`。按 entry active ticks 加权，
     不要直接相加裸 rate。
   - `data.blocks` 可能混合 block entity、random block tick、fluid tick，报告中称
     `block-side entries`；`data.entities`/`data.blocks` 是 Observable 返回的对象明细，
     不是服务器全量 census。对象 `traces` 与全局 `data.traces` 分开，trace `count`
     是采样出现次数，不是耗时或调用总数；父子 count 不相加。
   - 同时输出 Observable 的 group/type/namespace 对象数、active ticks、加权 ms/tick、
     单对象最大值和对象 key 稳定性。实体优先按 `(group, entityId)`，方块按
     `(group, position, type)`；稀疏 item/player 条目不能冒充持续瓶颈。
   - Spark 负责 Server thread 的 self/inclusive/owner/family、TICKED 分母和
     validation；Observable 负责对象、维度、类型和 namespace 定位。两者只作同窗口
     交叉证据，不改变各自分母。
   - **绝不能相加或互换解释**：Observable block/entity ms/tick、Observable trace
     count、Spark MSPT、Spark owner/family/self/inclusive、blocking、scheduler、mixin。
     Observable tracked time 不是全局 CPU 百分比，也不能直接转换成 Spark 的可回收收益。
   - 如果窗口不重叠，只能写“相邻状态交叉验证”，不能合并百分比或毫秒；如果两个 Observable
     窗口相邻且对象/坐标/weighted ms/tick 稳定，可区分持续负载和局部波动。
   - 联合建议必须降为单变量 A/B：warm-up、相同长度/参数、固定玩家位置和机器输入，
     尽量至少三组成对窗口；停用机器导致对象数或 active ticks 下降时，写明 workload
     change，不能把移除的工作量直接称为优化收益。

## 解读规则（弄错这些，报告就会出错）

- **self = inclusive - sum(children)**，按每个时间窗分别计算。self 时间表示 CPU 实际执行的
  位置；更准确地说，它是被采样线程的墙钟采样落在该帧的时间，不是 CPU cycles。只有 validation
  通过、同一窗口和同一棵树内才可按此公式解释。
- **三种归因视图，绝不可相加。**
  - *归属方（Owner）*（按最近的 Mod 栈帧统计 self 时间）回答"正在执行的是谁的代码"。summary 的
    主表是**折叠后**的归属方（库帧记到请求方）；未折叠的原表在同一份 'analysis.json' 的 'owners'
    里。两者数值不同（实测 ae2 28.39% → 29.22%、neoforge 6.73% → 11.23%），引用时必须说明用的
    是哪一张。
  - *族/生态（Family/ecosystem）* 按脚本内的 family 正则和可配置库表聚合，回答"哪个生态
    相关"；它不会自动把所有加载器都折叠到请求方。库 Mod（owo、ldlib2、titanium……）列在
    summary 的 notes 中；若整合包的库不同，用 '--libs a,b,c' 覆盖。
  - *未映射（Unmapped）* 收集 'class_sources' 里没有 Mod id、又不是原版或 JVM/运行时命名
    空间的类，回答"哪些代码的归属是猜的"。见下条。
  一个 Mod 可能在一种视图下显得很小，在另一种视图下很大（例如某个蜜蜂机器附属 Mod 的
  self 时间占 3%，却可能位于 12% 的 inclusive 刻工作量之下，因为它的工作是在 Mekanism
  与 AE2 的栈帧中执行的）。
- **未映射代码要先看 "code with no mod id in the payload" 一节。** 这些类的 self 时间会被
  记到它上面最近的 Mod 头上；若某个包根与 payload 自己声明的 sources 模糊匹配，说明那个
  Mod 的类 spark 没映射上，它的成本会伪装成宿主 Mod 的成本（self 占比很小、inclusive
  占比很大是典型特征），需要顺着 callers 上溯再下结论。该节的 inclusive 列在包根互相嵌套
  时会重叠，不可相加。
- **区分阻塞与调度帧。** `park`、`wait`、`sleep`、`awaitDone` 是确定的阻塞候选，summary
  会单列为 latency；`unpark`、`signalWork`、`scan`、`yield`、`join`、`invoke` 只标为
  scheduler/worker-related，方法名本身不能证明线程正在等待，必须结合完整调用链和 A/B 验证。
- **mixin 注入帧是证据，不只是噪音。** 'wrapOperation$<hash>$<modid>$<target>'、
  'redirect$...'、'mixinextras$bridge$<name>$<n>' 这类方法名里带着注入者；summary 的
  "mixin-injected frames" 一节已按注入者聚合。类名指向的归属方往往只是宿主，动手的是注入者。
  'mixinextras$bridge$...' 本身不含 modid（只有 mixinextras 生成的桥接），需要看它的父帧。
- **把占比换算成每刻开销**：优先使用匹配采样根的窗口分母，而不是 rolling MSPT。SIMPLE
  使用每窗口 `root_ms / time_window_statistics.ticks`（all ticks）；TICKED 只有在逐窗口
  ticks 完整且总和等于 `number_of_included_ticks` 时，才使用
  `root_ms / time_window_statistics.ticks`（included ticks）。跨窗口或跨线程不能直接套用
  全局 1m median；如果 TICKED 没有可靠的逐窗口 included-tick 分母，必须输出 `N/A`，不能
  伪造 CPU ms/tick。TICKED profile 只覆盖慢刻时，每刻值描述的是纳入慢刻，不是全刻平均。
- **尊重聚合器的语义。** 在 TICKED 聚合并设置 tick 阈值时，只有长于该阈值的刻会被记录。
  summary 的 TICKED note 会直接打印纳入刻数与总刻数的比例（例如 1725 of 1980 ticks, 87%）；
  比例明显低于 100% 时，这份 profile 描述的是慢刻而不是平均刻 - 必须明确说明。
- **尊重线程转储器。** 如果只采样了一个线程，报告就无法看到 Netty、区块 I/O、GC 或其他
  工作线程。绝不要声称它们空闲；应当说明它们未被采样。
- **核对总量。** summary 的 validation 一节会自动给出 sum(self) 与根总量之差、越界引用数、
  重复引用数、cycle、不可达节点数与负 self 数。任一项异常都是硬闸门：`analyze/owners/callers/dive`
  停止归因和修复排序，先重新 probe/重抓；仍失败就只报告“采集/解码失败”。'analysis.json' 的
  'issues' 字段有同样的数字。
- **逐窗口也要过 validation。** 每个 time window 单独检查 `selfSum - root` 和负 self；
  任一窗口异常时，逐窗 top methods/owners 不能用于归因，`analyze` 必须拒绝发布结果。
- **区分 GC counter 类型。** `*Cycles` 是累计的 GC activity，不等于 stop-the-world pause；
  只有 `*Pauses` 且 `avg_time` 有限时才估算 pause share。缺失值写 `N/A`，并注明 counters
  通常是自进程启动以来的累计值。
- **区分主动 tick pacing 与真正等待。** `waitUntilNextTick`、`managedBlock`、
  `waitForTasks` 等主循环 headroom 只能报告为 intentional pacing，不能归入可回收 CPU 或
  锁/worker contention；`park/wait` 只有脱离 pacing 祖先链时才进入 blocking。
- 区分**恒定超载**（各窗口的归属方占比基本持平，没有明显起伏）与**尖峰**（某一个窗口
  远高于其余窗口）。summary.txt 中的逐窗表格能显示属于哪一种。
- **先看 inclusive drivers。** 高 inclusive、低 self 的方法是触发链候选，不是根因证明；
  自动下钻按 self 排序可能漏掉它们。必须结合 callers/dive、逐窗持续性和单变量 A/B。
- **证据等级。** 直接观测 = validation 通过的 self/窗口数据；强关联 = callers+dive+跨窗一致；
  待验证假设 = 仅 inclusive、mixin、unmapped 或交互共现。报告不能把待验证假设写成唯一元凶，
  也不能直接承诺可回收收益。
- **联合 Observable 的证据等级。** L0 = schema/metadata；L1 = 单窗口字段与公式直接观测；
  L2 = 多个 Observable 窗口重复且对象/坐标/active ticks 稳定；L3 = 与同时间 Spark
  调用路径或对象一致的交叉证据；L4 = 单变量 A/B 后的因果支持。相同 diagnostics user id
  只能作为弱证据，必须结合版本、loader、Observable 版本、Mods/System Report。
- **已知公共库与真正未知包分开。** fastutil、Guava、NeoForge bus、Netty 等公共命名空间
  不是未知 Mod；应报告为中性上下文，并给出最近 Mod 栈帧和归因置信度。

## 报告结构

使用 MS/references/report-template.zh.md。必需内容：

1. 一句话结论，外加 TPS/MSPT/CPU/GC/世界大小的小表格。
2. 每刻预算表：调用路径、占采样时间的比例、ms/刻。
3. 归因表（归属方与折叠后的族），并注明两者使用不同视图；若 summary 报了阻塞/并行等待、
   scheduler 帧、公共库、未映射代码或 mixin 注入帧，各补一小段说明它们如何改变归属结论；
   unmapped self 超过 5% 时，所有唯一根因与收益承诺必须降级为关联假设。
4. 按收益排序的修复建议，每条都绑定到具名方法及其实测开销，并按
   "配置修改 / 布局调整 / 移除或节流"分组。每条还必须写证据等级、可逆性、
   预期比较指标、停止条件；不得从 inclusive 直接承诺收益。
5. 若有 Observable，单列其来源、SHA-256、`[start,end)`、gap/overlap、tick source
   （observed/inferred）、block-side/entity-side 加权公式和对象级热点；注明不与 Spark
   分母相加。
6. 如何验证：用完全相同的 spark 参数重新采样；Observable 使用相同 duration/ticks/schema
   口径；对比 MSPT 中位数、P95、纳入刻比例、Observable weighted ms/tick、对象数/active
   ticks 和逐窗结果。一次只改一个变量，写明 warm-up、workload change 和回滚条件。
7. 本份报告自身的局限（采样了哪些线程、聚合器、缺失的指标、Observable 插桩覆盖范围）。

## 常见陷阱

- 不要抓取查看器页面，也不要获取 'spark.lucko.me/<code>' 的 HTML；数据位于
  'https://bytebin.lucko.me/<code>'，格式是 protobuf（有时被 gzip 包裹；工具两者都能
  处理）。
- 'https://spark.lucko.me/<code>?raw=1' 返回的是一小段 JSON 元数据（以
  '{"type":"sampler"' 开头），没有调用树。analyzer 会识别这种 payload：'probe' 直接打印
  其中的统计信息，其他命令会报错并提示改用 'https://bytebin.lucko.me/<code>'。
- 'api.github.com' 与 'raw.githubusercontent.com' 对 web_fetch 类工具可能被 DNS 屏蔽；
  schema 已内置于脚本中，因此解码无需网络访问。
- `probe --json` 是脚本化核心字段，不保证与文本输出逐字相同；比较两份报告前先核对
  raw SHA-256、采样间隔、aggregator/阈值、线程集合和窗口范围。
- Observable 联合分析不要抓 viewer HTML；使用 `https://observable.tas.sh/v1/get/<id>`，
  校验 JSON/schema/content-type，并在报告中记录 raw JSON SHA-256。未知字段、缺失
  `data.ticks`、非法 rate/ticks、重复对象 key、空 per-entry trace 要显式报告，不能静默
  归零或补全。
- 不要在 run_code 的 'String.raw' 模板字面量里构造报告。Markdown 行内代码使用反引号，
  会提前终止该字面量。请使用 write 工具，或先用占位字符、事后再替换。
- 百分比是*被采样线程的采样墙钟时间*中的占比，而不是 tick 预算的占比。与 50 ms 预算作
  比较前必须先换算。
- Mixin 生成的栈帧（例如 'wrapOperation$...'、'redirect$...'、'mixinextras$bridge$...'）
  会表现为真实方法，甚至可能成为自己的父节点。它们同时是"谁注入了这段代码"的直接证据：
  按 summary 的 "mixin-injected frames" 一节归因给注入者，不要按类名归因给宿主，也不要
  把它们当作使用者代码来报告。
- 大型报告（时长久、线程多）：提高 '--tree-min'、'--dive-min'，并降低 '--max-dives'
  以保持产物可读；当 payload 超过约 200 MB 时，用 'node --max-old-space-size=8192'
  给 Node 更多堆内存。
- 若 Node OOM，保留 raw.bin 和错误输出，先提高堆内存或缩短采样；仍失败就停止归因。
- health 与 heap payload 没有调用树。'probe' 会打印它们的统计信息与 top 堆条目；
  'analyze' 会如实说明这一点，而不会凭空编造一棵树。

## 参考

- MS/references/data-format.md - 线格式（wire format）、消息布局，以及时间窗、self 时间
  与"类到 Mod"归因（含未映射视图）的精确语义。
- MS/references/report-template.zh.md - 交付报告所用的中文报告骨架。
- MS/references/observable-joint-analysis.zh.md - Spark + Observable 的 endpoint、时间窗、
  `rate` 加权、trace 边界、证据等级和单变量 A/B 联合规则；仅在用户提供 Observable
  报告时读取。
