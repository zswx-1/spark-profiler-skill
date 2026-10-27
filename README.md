# spark-profiler-analysis

用于解码和分析 Minecraft [spark](https://spark.lucko.me/) 性能报告的只读分析工具。它直接读取 spark 的 protobuf payload，提取采样调用树、时间窗口、TPS/MSPT、系统指标和 Mod 映射，输出可追溯的热点、归因和验证依据。

查看器 URL 只是网页外壳，真正的报告数据来自 bytebin。这个工具会把 viewer URL 转成 bytebin 请求，不依赖网页 HTML，也不需要安装 npm 依赖。

## 能做什么

- 从 spark viewer URL、bytebin code 或本地 payload 获取原始报告。
- 识别 `sampler`、`health` 和 `heap` payload，并先输出元数据摘要。
- 解码 sampler 的 protobuf 调用树，按线程和时间窗口计算 self/inclusive 时间。
- 输出 TPS、MSPT、CPU、GC、堆、玩家、实体和区块等上下文。
- 按三种互不相加的视图归因：最近 Mod 栈帧、折叠公共库后的请求方、生态族群。
- 标出阻塞候选、主动 tick pacing、scheduler-like 帧、公共库、未映射代码和 mixin 注入帧。
- 对具名方法继续查看 callers 或聚合子树，支持模糊匹配。
- 在 validation 通过后生成文本报告和机器可读 JSON，供后续写性能报告或脚本比较。
- 在另有 Observable 报告时，按时间窗口和对象级计时做交叉定位；Observable 不是本工具的必需输入。

## 运行要求

- Node.js 18 或更高版本。
- 仅使用 Node.js 标准库，无需 `npm install`。
- 能访问 `bytebin.lucko.me` 时才能在线抓取；分析本地 payload 不需要网络。

## 快速开始

在本目录执行：

```bash
# 1. 抓取并保存原始 payload
node scripts/spark.mjs fetch https://spark.lucko.me/<code> --out spark-<code>

# 2. 先查看采样参数、线程、窗口和环境摘要
node scripts/spark.mjs probe spark-<code>/raw.bin

# 3. 生成完整分析产物
node scripts/spark.mjs analyze spark-<code>/raw.bin --out spark-<code>/analysis
```

也可以直接使用 bytebin code：

```bash
node scripts/spark.mjs fetch <code> --out spark-<code>
```

已有本地文件时，`fetch` 会复制文件并写入来源信息；也可以跳过抓取，直接运行：

```bash
node scripts/spark.mjs probe path/to/report.spark --json
node scripts/spark.mjs analyze path/to/report.spark --out analysis
```

`--json` 适合脚本化比较。缺失的数值保持为 `null` 或 `N/A`，不会用 0 或推算值填充。

## 命令

### `fetch`

```bash
node scripts/spark.mjs fetch <url|code|file> [--out DIR]
```

接受以下输入：

- `https://spark.lucko.me/<code>` viewer URL；
- `https://bytebin.lucko.me/<code>` URL；
- 裸 code；
- 本地 payload 文件。

默认输出目录是 `spark-<code>`；本地文件默认使用 `spark-report`。输出至少包含：

- `raw.bin`：抓取或复制得到的 payload；网络响应会按需解压 gzip，本地文件保持原样；
- `source.json`：来源、抓取时间、时区、字节数和 SHA-256。

工具会在保存后立即校验 payload。HTTP 错误、空响应、HTML、metadata JSON、截断数据、未知 schema 或解码失败都会保留错误信息，但不会继续做归因或收益排序。

### `probe`

```bash
node scripts/spark.mjs probe <file> [--json]
```

在分析前使用。文本摘要和 JSON 摘要会报告适用于本次报告的关键分母与边界，包括：

- payload 类型、采样时长、采样间隔和采样率；
- `SIMPLE`/`TICKED` 聚合器、tick 阈值、纳入刻数与总刻数；
- 线程转储器及实际可见的线程；
- TPS、MSPT、CPU、GC、内存、玩家、实体和区块；
- 每个时间窗口的刻数、MSPT、root wall time 和可用的每刻分母；
- payload 的 Mod/source 数量、环境判语和 validation 结果。

`health` 和 `heap` payload 没有调用树，只能使用 `probe` 查看统计信息和堆条目。

### `analyze`

```bash
node scripts/spark.mjs analyze <file> [选项]
```

支持的选项：

| 选项 | 作用 |
|---|---|
| `--out DIR` | 分析产物目录，默认使用输入文件所在目录 |
| `--thread NAME\|INDEX` | 只分析指定线程；不指定时分析全部线程 |
| `--top N` | 各表保留的 top 条目数 |
| `--tree-min PCT` | 调用树的最小 inclusive 百分比 |
| `--tree-depth N` | 调用树最大深度 |
| `--dive-min PCT` | 自动下钻热点的最小 self 百分比 |
| `--dive-depth N` | 自动下钻子树最大深度 |
| `--max-dives N` | 自动下钻的热点数量 |
| `--driver-min PCT` | inclusive driver 的最小 inclusive 百分比 |
| `--max-drivers N` | inclusive driver 的数量上限 |
| `--callers N` | 每个热点保留的调用者数量 |
| `--libs a,b,c` | 覆盖默认公共库 Mod 列表，用于折叠归因 |

调用树命令要求 payload 是 sampler。工具会先执行结构校验；只要出现引用越界、重复引用、环、不可达节点、负 self，或 self 总量与 root 不一致，就拒绝发布归因和修复排序。

### `owners`

```bash
node scripts/spark.mjs owners <file> [--thread NAME|INDEX] [--top N]
```

同时打印：

1. 未折叠的最近 Mod 栈帧归因；
2. 将配置的公共库折叠到请求方后的归因。

未指定线程时，这个定向命令使用第一个线程，仅用于兼容旧用法。报告中必须明确线程选择，不能把单线程百分比当成全局结果。

### `callers`

```bash
node scripts/spark.mjs callers <file> <method-key> [--thread NAME|INDEX] [--top N] [--like]
```

查看方法的最近 Mod 归属、折叠归属和直接父栈帧。`method-key` 是分析输出中的完整方法名，例如：

```bash
node scripts/spark.mjs callers report.spark \
  "net.minecraft.world.item.ItemStack.<init>" \
  --thread "Server thread"

# 不确定完整 key 时使用子串匹配
node scripts/spark.mjs callers report.spark PalettedContainer.get --like
```

### `dive`

```bash
node scripts/spark.mjs dive <file> <method-key> [--thread NAME|INDEX] [--like] [--depth N] [--min PCT]
```

把方法的所有匹配节点合并为一棵聚合子树，查看热点内部的实际工作：

```bash
node scripts/spark.mjs dive report.spark \
  appeng.me.service.StorageService.updateCachedStacks \
  --thread "Server thread" --depth 8 --min 0.05
```

重复、递归或嵌套匹配会合并计数；输出会注明匹配节点数和被合并的嵌套节点数。

## 分析产物

`analyze` 默认生成以下文件：

| 文件 | 内容 |
|---|---|
| `summary.txt` | 元数据、环境判语、归因表、top 方法/类、调用树、逐窗口表、阻塞/调度/未映射/mixin 小节和 validation |
| `analysis.json` | 机器可读的完整结果、raw SHA-256、分析选项、线程结果、窗口分母、owners、drivers、callers 和 dives |
| `tree.txt` | 裁剪后的 inclusive/self 调用树 |
| `hotspots.txt` | 自动下钻热点的聚合子树 |
| `callers.txt` | 热点的调用者、归属和直接父栈帧 |

推荐阅读顺序：

1. 先读 `probe` 或 `summary.txt` 的元数据、聚合器、线程集合和 validation；
2. 再看逐窗口结果，判断是恒定超载还是单个尖峰；
3. 用 `summary.txt` 的 top methods、owners 和 inclusive drivers 建立候选；
4. 用 `callers.txt`、`hotspots.txt` 或 `dive` 查调用路径；
5. 最后根据证据等级写建议，并用相同参数重新采样验证。

## 关键口径

### self 与 inclusive

调用树节点的 inclusive 时间包含子调用；self 时间按以下公式逐窗口计算：

```text
self = inclusive - sum(children)
```

self 是被采样线程上的采样墙钟时间，不是 CPU cycles。只有 validation 通过、且处于同一窗口和同一棵树中时，才可以按这个公式解释。

### 每刻毫秒

每刻值使用与采样 root 对应的窗口分母，不使用 rolling 1 分钟 MSPT 替代：

- `SIMPLE`：`root_ms / time_window_statistics.ticks`，分母是全部刻数；
- `TICKED`：只有逐窗口刻数完整，且总和与 `number_of_included_ticks` 一致时，才使用纳入刻数；
- 分母不可靠时输出 `N/A`。

`TICKED` 只记录超过阈值的慢刻。纳入刻数低于总刻数时，结果描述的是慢刻集合，不是平均刻。

### 三种归因视图不可相加

- **Owner**：最近的 Mod 栈帧，回答“代码实际在哪个归属方中运行”；
- **Folded owner**：跳过配置的公共库 Mod，把时间记到请求方，回答“谁请求了这段库工作”；
- **Family/ecosystem**：按脚本中的启发式规则聚合，回答“哪个生态相关”。

同一工作会同时出现在不同视图中，inclusive 行还可能重叠。不要把 owner、family、self、inclusive 或多个热点表相加。

未映射代码的 self 会被记到它上方最近的已映射 Mod，可能造成宿主 Mod 看起来承担了不属于它的工作。公共库命名空间（例如 fastutil、Guava、Netty、NeoForge bus）与真正未映射包分开报告。分析器会在公共库与未映射 self 合计超过 5% 时提示降低归因置信度；报告规范还要求真正未映射 self 超过 5% 时，将唯一根因和收益结论降级为关联假设。

### 等待、调度和 mixin

- `park`、`wait`、`sleep`、`awaitDone` 是阻塞候选，表示延迟，不是 CPU 工作；
- `unpark`、`signalWork`、`scan`、`yield`、`join`、`invoke` 只表示 scheduler/worker-related，方法名本身不能证明线程在等待；
- `waitUntilNextTick`、`managedBlock` 等主循环 pacing 是主动让出 headroom，不应当作为可回收 CPU 或 contention；
- `wrapOperation$...`、`redirect$...`、`mixinextras$bridge$...` 等帧需要按注入者理解，不能只按目标类名归因。

## 与 Observable 联合分析

同时提供 Observable 报告时，联合分析应使用其 `/v1/get/<id>` JSON endpoint，不抓取 viewer HTML。Spark 和 Observable 要分别保存原始数据、来源、时间窗口、字节数和 SHA-256。

时间窗口使用半开区间：

```text
Spark:      [spark.start, spark.end)
Observable: [diagnostics.start, diagnostics.start + duration)
```

需要记录 `gap`、`overlap`、`containment` 和 `union`。窗口不重叠时只能写相邻状态交叉验证，不能合并百分比或毫秒。

Observable 对象贡献按 entry 加权：

```text
entry contribution = rate × entry.ticks / total_ticks / 1,000,000  ms/tick
```

`data.ticks` 是 numeric 时标记为 observed，否则用 entry 最大 ticks 时必须标记为 inferred。`data.blocks` 统一称为 block-side entries，因为它可能混合 block entity、random block tick 和 fluid tick；`data.entities`/`data.blocks` 是返回的对象明细，不是服务器全量 census。

以下数据不能相加或互换解释：

- Observable block/entity weighted ms/tick；
- Observable trace count；
- Spark MSPT、owner、family、self、inclusive；
- Spark blocking、scheduler、mixin。

联合证据最多用于提高定位置信度。因果建议必须降为单变量 A/B：相同采样参数、相同 Observable duration/schema、warm-up、固定玩家位置和机器输入，尽量做至少三组成对窗口，并记录 workload change、回滚条件和功能状态。

## 写分析报告时应包含什么

建议按 `references/report-template.zh.md` 的顺序组织报告：

1. 结论速览：一句话结论、TPS/MSPT/CPU/GC/世界规模表格；
2. 报告元数据与环境：来源、SHA-256、采样线程、聚合器、纳入刻数和 validation；
3. 稳定性判断：逐窗口趋势，区分恒定超载和尖峰；
4. 单刻时间去向：调用树、方法名、占比和每刻毫秒；
5. 责任归因：标明使用的是哪种 owner/family 视图；
6. 优化建议：按配置、布局、移除或节流排序，每条绑定方法、实测开销、证据等级、可逆性、指标和停止条件；
7. 复现与验证：一次只改一个变量，比较 MSPT 中位数/P95、纳入刻比例、逐窗口结果及 Observable 对象指标；
8. 方法与局限：采样线程、分母、未映射代码、公共库、阻塞判定和 Observable 插桩覆盖范围。

不要从 inclusive 直接承诺可回收收益，也不要把等待时间写成 CPU 成本。

## 常见失败处理

- `spark.lucko.me/<code>?raw=1` 返回 metadata JSON，不包含调用树；需要使用 viewer URL、bytebin URL 或裸 code 让工具获取 payload。
- 403、404、DNS、超时、重定向超限、空响应、HTML、截断 payload、未知 schema 和 OOM 都属于获取或解码失败；保留原始输入和错误，不继续归因。
- `analyze`、`owners`、`callers`、`dive` 只能处理 sampler；health/heap 只能执行 `probe`。
- 大型 payload 可提高 Node 堆上限，例如：

```bash
node --max-old-space-size=8192 scripts/spark.mjs analyze report.spark --out analysis
```

若仍然 OOM，应缩短采样或改用更小的报告；不能根据不完整结果发布根因和收益排序。

## 目录结构

```text
spark-profiler-analysis/
├── SKILL.md
├── README.md
├── scripts/
│   └── spark.mjs
└── references/
    ├── data-format.md
    ├── observable-joint-analysis.zh.md
    └── report-template.zh.md
```

- `scripts/spark.mjs`：标准库实现的抓取、protobuf 解码、probe 和分析器；
- `references/data-format.md`：payload 结构、self、归因和 validation 语义；
- `references/report-template.zh.md`：中文性能报告骨架；
- `references/observable-joint-analysis.zh.md`：Spark 与 Observable 的时间对齐、加权和 A/B 规则；
- `SKILL.md`：面向代理执行分析任务的工作流与约束。

## 只读边界

本工具默认只读，仅抓取 payload 并生成分析产物，不修改服务器、世界、配置、模组、脚本或 skill 文件，不自动重载/重启，也不执行修改性 A/B。若要执行配置修改、停用模组、调整机器布局或安装操作，必须先向使用者说明并取得明确确认。
