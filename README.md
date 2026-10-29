# reasoning-relay-benchmark

**English abstract.** On 2026-10-28 we ran the same 20 DeepSWE v1.1 coding tasks, four rounds each, through three local relays — Stravia, CLIProxyAPI (CPA) and sub2api — with the same DSH client speaking Chat Completions, the same model (`gpt-6-luna`, reasoning effort `xhigh`), the same ChatGPT account, the same machine and a one-hour agent limit per task. After voiding 6 of 80 task-rounds for all three routes alike, Stravia passed 47/74 (63.5%), CPA 13/74 (17.6%) and sub2api 18/74 (24.3%); Stravia minus the mean of the other two is +42.6 pp (task-clustered bootstrap 95% CI +29.1 to +56.3 pp). 98.3% of Stravia's upstream requests carried the model's encrypted reasoning back to the model; none of CPA's or sub2api's did. This repository publishes the harness, the frozen task list, sanitized per-attempt evidence for the batch (3.77 billion tokens across 279 attempts, 97.6% of them cached input) and a script that rebuilds every published number from that evidence. Code is MIT; data is CC BY 4.0.

## 目录

```text
.
├── README.md
├── LICENSE                         MIT（harness/、tools/）
├── LICENSE-DATA                    CC BY 4.0（results/、workload/）
├── workload/
│   ├── sample.json                 冻结的 20 个任务（seed 0，113 选 20，无排除）
│   └── workload-manifest.json      DeepSWE 上游仓库、commit 与清单哈希
├── harness/                        测试代码，目录结构与原仓库一致，harness/ 即运行根目录
│   ├── MANIFEST.json               每个文件的 SHA-256，及是否与正式批启动前冻结的哈希一致
│   ├── video/chat-reasoning-replay/project/
│   │   ├── meterd/                 出站计量代理（Go），只记录请求结构，不记录正文和密钥
│   │   └── scripts/
│   │       ├── services/           三路隔离实例 + 计量端口的启动、接线、零推理冒烟
│   │       ├── pier/               Pier 适配：DSH agent、容器内中继、离线包、环境与 oracle 验收
│   │       ├── orchestrate/        批次编排、分析（通过率与 bootstrap）、零推理自测
│   │       ├── gate/               推理上下文是否被带回的门槛测试
│   │       └── meter/              计量汇总
│   └── runtime/dsh-bundle/         DSH 0.1.0-rc.8 的 package.json 与锁文件
├── results/
│   ├── formal-xhigh-threeway-20261028/        正式 xhigh 三路批
│   │   ├── plan.json / environment.json / activation.json
│   │   ├── results.jsonl           每次尝试一行（含作废标记行）
│   │   ├── state.jsonl / runs.json / summary.json / postprocess.json
│   │   ├── analysis*.json          analyze.py 的原始输出
│   │   ├── final-report.json       审计后的总报告
│   │   ├── meter-summary.json      计量汇总（逐请求明细已折叠为计数）
│   │   ├── attempts/<job>/         outcome.json、dsh-run.json、reward.json
│   │   └── SANITIZATION.json       脱敏清单、替换计数、原文件与脱敏后哈希
│   ├── formal-xhigh-threeway-20261028-audit/  严格审计原始输出、作废与计量缺口证据、收尾记录
│   └── summary/
│       ├── reproduction.json       28 项重算比对，全部通过
│       └── headline.json           README 与视频使用的派生数字
└── tools/
    ├── sanitize_evidence.py        脱敏复制并断言无残留
    └── reproduce_summary.py        从证据重建并比对全部公开数字
```

仓库不包含任何二进制、依赖、录屏、DSH 会话记录、模型生成的补丁、测试原始输出或凭据。

## 结论

正式 xhigh 批：20 个任务 × 4 轮 × 3 路，计划 240 个试验；6 个任务轮被三路一并作废，每路剩 74 个计分试验。

| 路线 | 版本 | 通过 | 通过率 | 95% CI | agent 耗时中位数 | 计分试验中超时 | 上游请求带回加密推理 |
|---|---|---:|---:|---:|---:|---:|---:|
| Stravia | dev `e67b6e0f`（0.5.3） | 47/74 | 63.5% | 48.6%–77.9% | 18.2 min | 1 | 11,640 / 11,845（98.3%） |
| CPA | v8.0.43 | 13/74 | 17.6% | 6.9%–30.0% | 59.6 min | 35 | 0 / 13,772 |
| sub2api | v0.2.18 | 18/74 | 24.3% | 11.7%–38.9% | 41.0 min | 14 | 0 / 15,220 |

- Stravia 通过率 − 另两路平均 = **+42.6 个百分点**，95% CI [+29.1, +56.3]（按 20 个任务聚类，10,000 次 bootstrap，seed 0）。分别对 CPA +45.9 [+31.5, +60.5]、对 sub2api +39.2 [+22.7, +55.4]。
- 通过率是另两路平均的 **3.03 倍**（63.5% ÷ 20.9%）。单看每一家：CPA 的 3.6 倍，sub2api 的 2.6 倍。
- 配对：同一任务同一轮，Stravia 过而 CPA 没过 35 次，反过来 1 次；对 sub2api 是 33 比 4。
- agent 耗时中位数（每个试验把重试时间加在一起）：另两路中位数的平均是 Stravia 的 2.76 倍；CPA 3.27 倍，sub2api 2.25 倍。
- 每次模型调用的平均输出 token：Stravia 489，CPA 816，sub2api 875。
- 官方测试（最新计分记录）：应修复测试（F2P）共 2,858 个，Stravia 过 2,788、CPA 2,283、sub2api 2,362。

## 观测到的差别

这一节只列数据里看得到的东西。三路共用一个账号、实现和传输方式不同、各自用默认策略、还有 1 小时时限，这些都不允许把差距单独归因到某一个机制。

**推理有没有被带回去。** 客户端 DSH 走 Chat Completions，这个协议里没有放加密推理的字段。三款工具都把请求转成 Codex 的 Responses 请求发给上游，并且都要求上游返回加密推理（`include` 里都有 `reasoning.encrypted_content`，`store` 都是 `false`），区别在下一轮：

- Stravia 在给客户端的 assistant 内容里留一段 History Marker，客户端原样回传后，Stravia 在服务端把它还原成原来的 `encrypted_content` 推理项，再发给上游。计量里 98.3% 的上游请求带着这些推理项，共 1,068,706 个。
- CPA 与 sub2api 的上游请求里没有任何推理项。sub2api 把推理摘要以明文 `<thinking>` 写进返回给客户端的内容，不保留加密推理项；CPA 不保留推理项。

OpenAI 的推理模型文档建议在函数调用时把上一轮的推理项一起传回（[Keeping reasoning items in context](https://developers.openai.com/api/docs/guides/reasoning)）。`store: false` 时，推理项要靠 `encrypted_content` 往下一轮传。

**耗时与超时。** Stravia 每次调用平均输出 489 个 token，另两路 816 和 875 个（输出 token 包含推理 token；上游没有单独报告推理 token，记为未知）。按全部尝试的 agent 耗时除以模型调用次数，平均每一步（模型回一次加上执行这一步的工具）Stravia 8.8 s，CPA 17.9 s，sub2api 13.2 s。CPA 有 35 个、sub2api 有 14 个计分试验在 1 小时内没做完，Stravia 有 1 个。

**传输。** Stravia 的上游请求全部走 WebSocket，CPA 与 sub2api 全部走 HTTP。三路都没有用 `previous_response_id`。传输方式不同也是耗时差别的一个可能来源，本批没有把它单独拆开。

## 测了什么

- 基准：DeepSWE v1.1（[datacurve-ai/deep-swe](https://github.com/datacurve-ai/deep-swe)，commit `0b9fabbb63b9104d678fe965e1632f2dd9eaa2ea`，113 个任务）。用 seed 0 选 20 个，排除规则（需要 GPU、单任务内存超过 8 GB、镜像拉取失败、`task.toml` 无法解析）实际没有排除任何任务，也不按难度、语言或历史通过率筛选。语言分布：TypeScript 7、Python 6、Go 4、Rust 2、JavaScript 1。
- 执行器：Pier `datacurve-pier==0.3.1` + 自写 DSH agent。任务容器 `network_mode=none`，DSH 经宿主挂进容器的 Unix socket 访问本路工具。
- 客户端：DSH `0.1.0-rc.8` headless，`api: openai-completions`，三路请求形态完全相同，只改 base URL 与 key。
- 模型：`gpt-6-luna`，reasoning effort `xhigh`。
- 时限：agent 墙钟 3,600 s（Pier 超时 3,900 s），单个试验总墙钟 5,400 s。
- 并发：一次最多 5 组 × 3 路 = 15 个试验，每路最多 5 个。组顺序与组内启动顺序由 seed 决定，先写进 `plan.json` 再执行。
- 重试：模型侧基础设施错误（连接错误、429、5xx、并发上限）在全新容器里整次重跑，最多 3 次；任何一路 3 次仍失败，该任务轮三路一并作废。
- 评分：每个试验取账本中该（任务, 轮, 路线）的最后一条记录，不挑最好的一次。reward 为 1 才算通过；`partial` 只记录。
- 工具配置：各工具用默认配置，只加接入所需的监听地址、客户端 key、出站代理和 OAuth 登录；默认的请求级重试保持开启。
- 计量：每路出站都经过自写的 `meterd`，只记录请求结构（推理项个数、密文长度、强度、传输、状态码），不记录正文和密钥。

### 版本与环境

| 组件 | 版本 | SHA-256（前 16 位） |
|---|---|---|
| Stravia | `StraviaPlatform` dev 分支 `e67b6e0f0e4117a308a2ea1947ed93a46c3fc817`，工作区版本 0.5.3 | `b993afe3f33dd16e` |
| CPA（CLIProxyAPI） | v8.0.43（`30780351`） | `0e6445974aca0fd4` |
| sub2api | v0.2.18（`ac1b8f12`），PostgreSQL 17.11 + Valkey 9.1.2 | `ffd1b1fc769ed9dc` |
| meterd | 本仓库 `harness/.../meterd` | `05465de0024d61ab` |
| DSH | `0.1.0-rc.8`，portable Node 24.21.0 | — |

- 单台机器：Intel i5-13600K（20 个逻辑 CPU）、64 GB 内存、Linux 6.18 x86_64、rootless Podman。
- 2026-10-28 12:27:09 到 2026-10-29 04:20:36（UTC+8），调度器墙钟 57,207 s（约 15 小时 53 分）。
- 三路用同一个 ChatGPT 账号各登录一次（各自的 refresh token），共享同一份额度与限流。只用自己的账号、单人测试。
- Stravia 是 pre-1.0 的开发版本；被测提交在 dev 分支上，尚未进入正式发布。本仓库不承诺任何版本的稳定性或兼容性。

## 作废与缺口

- **作废 6 个任务轮**（三路一并排除，原始尝试与用量全部保留）：`koota-composite-trait-aspects@r2` 因 CPA 3 次 HTTP 503 与外层墙钟；`numba-stencil-boundary-modes@r4`、`cliffy-config-file-parsing@r1`、`go-genai-streamed-function-args@r1`、`kombu-single-active-consumer-priority@r3`、`testem-bail-on-test-failure@r4` 因 sub2api 3 次触发并发上限。这 6 轮里 Stravia 有 5 次官方通过，同样被排除；不作废时 Stravia 为 52/80。
- **全部尝试**：Stravia 80 次、0 次重试；CPA 92 次（12 次 HTTP 503、1 次外层墙钟）；sub2api 107 次（32 次并发上限）。
- **流错误**：CPA 9 次、sub2api 7 次流中断按现有分类器记为普通失败，没有重试，也没有事后改判。
- **严格计量审计未通过**：CPA 有 3 个上传在中途被取消，计量只拿到部分请求体，无法确认这 3 个请求的强度与工具形态。审计原始输出保留在 `results/formal-xhigh-threeway-20261028-audit/strict-audit-result.json`，`final-report.json` 顶层状态保持 `failed_audit`。任务产物审计（279 份 outcome、279 份 DSH 元数据、222 个最新 reward）全部一致。
- **token 覆盖**：279 次尝试里 276 次有 token 记录（sub2api 有 3 次缺失）；推理 token 上游未单独报告，全部记为未知（null），不当作 0。

## token 用量

| 路线 | 尝试 | 输入（不含缓存） | 缓存输入 | 输出 |
|---|---:|---:|---:|---:|
| 三路合计 | 279 | 61,120,376 | 3,682,120,576 | 30,284,810 |

- 总 token = 输入（不含缓存）+ 缓存输入 + 输出 = **3,773,525,762（37.7 亿）**，其中缓存输入占 97.6%。取自每次尝试的 DSH 用量记录，作废轮次的尝试也计入；未知用量不计入，因此是下限。
- 环境验收、gate、单任务探针与示例录屏没有计入。
- 账号是 ChatGPT 订阅登录，不按 token 计费；这里的 token 数不代表花费。

## 如何复现分析

```bash
python3 tools/reproduce_summary.py --check
```

只用 Python 3.11+ 标准库，几秒钟跑完。脚本做三件事：

1. 加载 `harness/` 里正式批实际使用的 `analyze.py`，在脱敏后的 `results.jsonl` 上重算通过率与 bootstrap 区间，要求与批次目录里的 `analysis*.json` 逐字段相同；
2. 从账本、`attempts/` 与计量汇总重算配对格、超时、token、耗时分位数、官方测试计数和上游请求里的加密推理，与 `final-report.json` 比对；逐次核对 `attempts/` 的用量、超时、模型、强度、客户端版本和 reward；
3. 重建 `results/summary/` 下的两个文件并与已提交版本比对。

任何一项不一致都会以非零状态退出。不带 `--check` 会重写 `results/summary/`。

## 如何重跑测试（自担额度）

完整重跑会真实调用模型，消耗你自己账号的额度；xhigh 正式批在上面这台机器上跑了约 16 小时。脚本按原仓库的相对路径定位文件，下面的命令都在 `harness/` 目录里执行。

### 依赖

- Linux x86_64，rootless Podman 5 与 Compose；Python 3.11+；Go 1.26（构建 meterd）；Node.js 与 npm（打 DSH 离线包）；uv（安装 Pier）。
- 三款工具的二进制，放到 `.scratch/research/chat-reasoning-replay/tools/` 下与 `scripts/services/threeway-services.py` 里 `BINARIES` 相同的位置：Stravia 按上面的提交从 `StraviaPlatform` 源码构建；CPA 与 sub2api 用对应版本的官方 release。
- 一个你自己的 ChatGPT 账号，在三款工具里各登录一次。

### 准备

```bash
cd harness
S=.scratch/research/chat-reasoning-replay
mkdir -p $S/pier && cp ../workload/sample.json $S/pier/sample.json

# DeepSWE 任务（固定 commit）
git clone https://github.com/datacurve-ai/deep-swe $S/pier/deep-swe
git -C $S/pier/deep-swe checkout 0b9fabbb63b9104d678fe965e1632f2dd9eaa2ea

# Pier 0.3.1，二进制需要在 $S/pier/bin/pier
UV_TOOL_DIR=$PWD/$S/pier/uvtools UV_TOOL_BIN_DIR=$PWD/$S/pier/bin uv tool install datacurve-pier==0.3.1

# DSH 离线包：按锁文件安装（cordis-plugin-hmr 锁在 1.0.16，1.0.20 会让 rc.8 启动即崩溃）
mkdir -p $S/pier/dsh-bundle && cp runtime/dsh-bundle/package*.json $S/pier/dsh-bundle/
npm ci --prefix $S/pier/dsh-bundle
video/chat-reasoning-replay/project/scripts/pier/build_dsh_bundle.sh

# meterd
(cd video/chat-reasoning-replay/project/meterd && go build -o ../../../../$S/tools/meterd/meterd .)
```

### 启动三路并验证

```bash
P=video/chat-reasoning-replay/project/scripts
python3 $P/services/threeway-services.py start --fresh --zero-inference
python3 $P/services/threeway-services.py smoke        # 零推理：每路一个请求，确认计量记到外连
python3 $P/orchestrate/selftest.py                    # 零推理：假 Pier 跑通编排、重试、作废与分析
python3 $P/services/threeway-services.py stop

python3 $P/services/threeway-services.py start --fresh
# 在三款工具各自的管理界面里用你的 ChatGPT 账号完成 OAuth 登录，然后：
python3 $P/services/threeway-services.py wire
python3 $P/gate/gate.py --route stravia --reasoning xhigh   # 真实推理：检查第 2 次起的上游请求是否带回加密推理
```

凭据和运行态只写在 `~/.local/state/chat-reasoning-replay/private/`（目录 0700、文件 0600），不进仓库，也不出现在命令行和日志里。

### 正式批次

```bash
python3 $P/orchestrate/run_batch.py --mode batch --reasoning xhigh --rounds 4 --seed 0 \
  --max-groups 5 --agent-wall-seconds 3600 --trial-wall-seconds 5400 \
  --batch-id <你的批次名> --out $S/pier/batches/<你的批次名> --confirm-real-inference
python3 $P/orchestrate/analyze.py $S/pier/batches/<你的批次名>/results.jsonl --out analysis.json
```

## 数据处理

`tools/sanitize_evidence.py` 从原始批次目录复制，规则如下：

- 只复制批次顶层的 JSON / JSONL，以及每次尝试的 `outcome.json`、`dsh-run.json`、`reward.json`。不复制 `jobs/` 下的 DSH 会话、模型补丁、测试原始输出、Pier 日志和任务副本。
- 删除 `dsh-run.json` 的 `stderr_tail`（进程尾部输出，可能夹带任意错误原文）；归一化后的失败原因见 `final-report.json`。
- 计量日志是整台机器跨批次累积的，`meter-summary.json` 只保留本批时间窗内的 bucket，并删除全日志行数。逐请求明细（时间戳、flow 编号、每段密文长度）约 96 MB，折叠成每个 bucket 的计数 `requests_detail_rollup`；bucket 自带的汇总字段不变。
- 批次报告与审计记录里指向本批以外批次的冻结哈希、核对位和说明句在发布前删除；`SANITIZATION.json` 的 `transforms` 逐文件记录。
- 绝对路径替换为 `<repo>`、`<private-path>`、`<home>` 等占位符；邮箱、令牌、私钥、内网地址按形状替换。每个文件替换前后比较数值与结构，不一致即失败；复制结束后按凭据形状扫描残留，有残留即失败。
- 每个目录的 `SANITIZATION.json` 记录替换计数、做过的结构变换、每个文件脱敏后的 SHA-256 与原文件 SHA-256。原文件哈希可与 `final-report.json` 的 `input_sha256` 对照：`results.jsonl` 的原文件哈希就是报告里冻结的那一个。

## 已知局限

1. 一台机器、一个账号、一次批次。三路共享额度与限流，并发试验会互相挤占。
2. 三款工具的实现、上游传输和默认重试策略都不同，本批没有把其中任何一个因素单独拆开。
3. 1 小时时限比 DeepSWE 官方的 3 小时短；客户端是 DSH，不是榜单用的 mini-swe-agent。结果不能和榜单直接比较。
4. 只测了 GPT（`gpt-6-luna`）经 Chat Completions 的情况，没有测 Claude、Gemini 或其他客户端协议。
5. sub2api 属于使用条款有风险的类别。本批只用自己的账号、单人测试，不涉及账号共享或额度转售。
6. 同一路线并发多个试验时，计量只按路线和时间窗归因，不能精确到单个试验；因此计量里的请求数与 DSH 的调用数不完全相同。

## 许可

- `harness/`、`tools/` 下的代码：MIT，见 `LICENSE`。
- `results/`、`workload/` 下的数据：CC BY 4.0，见 `LICENSE-DATA`。
- DeepSWE 任务本体不在本仓库，以上游仓库的许可为准。

## 与视频的关系

本仓库是 Stravia 推理上下文实测视频的公开代码与数据；视频里的结果数字都取自 `results/summary/headline.json`，可以用上面的复现命令自己核对。
