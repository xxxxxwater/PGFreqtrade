# PM ownership — 第二轮独立评审与修复（Staff Engineer Review）

状态：本地候选，未部署，未修改服务器，未重启实盘，未解除任何实盘门禁，未下真实订单。
基线：分支 pm-redo-native，起点提交 5bf5efc13。第二轮红队之后，根据外部复现的
确定性安全反例又完成了一轮 Protection Correctness / Pre-Staging 修复（e57c9f719 的
评审结论为 NOT READY FOR STAGING；本轮修复见 §0）。

## 结论（先给）

**A. READY FOR STAGING VALIDATION**（仅限 staging 验证，不代表生产无人值守验收）。

上一提交（e57c9f719）因保护替换与校验的确定性反例被判定 NOT READY FOR STAGING。
本轮已把全部 P0/P1 确定性反例修成 regression tests 并通过（§0、§12）：无效新止损
（rejected / missing status / closed / 错合约 / 错方向 / 错 positionSide / 非
reduce-only / 数量不足）永不撤旧保护；DCA/入场成交路径与 trailing 路径共用同一
CREATE→PERSIST→FETCH→VALIDATE→RETIRE 原语；保护验证独立于 open orders 且比较覆盖
数量；instrument 门禁需完整证据重放才解除；数量核对使用有界在途区间+时限；
ownership 缓存失效键修正；journal purge 不删除持久子单 alias。

仍 BLOCK 生产无人值守验收的边界见 §16。staging 部署必须约束单实例（见 §15.1）。

---

## 0. Protection Correctness / Pre-Staging 修复轮（e57c9f719 评审反例）

外部评审复现的确定性安全反例（e57c9f719 判 NOT READY FOR STAGING）全部修复：

| 编号 | 反例 | 修复 |
|---|---|---|
| P0-1 | trailing 与 DCA 两条路径实现不一致；新止损无效时仍撤旧 | 统一原语 `_pm_replace_stop_protection`（CREATE → PERSIST CANDIDATE → FETCH EXCHANGE TRUTH → VALIDATE NEW → 才 RETIRE OLD），接入全部六条路径：trailing 替换、DCA resize、入场成交创建、全撤重建、缺保护自愈（循环经原语重建 + 不变量 gate）、启动恢复重建 |
| P0-2 | 只按裸 ID 判保护有效 | 新增 `_pm_validate_protection_order`：canonical instrument 精确匹配、预期订单 ID、exit side、positionSide（BOTH/同向）、active/trigger-pending 状态（rejected/canceled/expired/missing status 一律 invalid）、reduce-only 语义、覆盖数量 ≥ 仓位。CLOSED/FILLED/triggered → "terminal"：先经 update_trade_state 处理成交、刷新 Trade、重算保护需求，绝不当作替换成功 |
| P0-3 | `_update_trade_after_fill` DCA 成交仍先撤后建 | PM 分支改调 `_pm_resize_stop_protection`：旧 conditional 保持有效直到新保护经交易所验证；创建/验证失败保留旧保护并 SAFE_HOLD（stop_protection_missing） |
| P0-4 | open_orders 非空即跳过保护验证 | 删除豁免：只要有已确认敞口（amount>0）就独立验证 stop protection；DCA/部分成交/退出挂单不构成豁免 |
| P0-5 | 存在 stop ID 即视为受保护 | 保护验证比较覆盖数量（0/1/10/99/100/101 矩阵测试）+ 错合约同 ID / closed / rejected / 错方向 / 非 reduce-only fixture 测试 |
| P0-6 | 双保护窗口语义未证明 | 测试证明：撤旧只撤指定旧 ID、绝不触碰既有第二保护与新替换；stop 成交事件处理路径从不撤任何保护；reduce-only 由创建请求（reduceOnly=true）与 validator 双重保证，不会 over-reduction / position flip |
| P1-1 | raw symbol 解析成功即解门禁 | 未解析事件持久保存完整证据（raw symbol + UM/CM namespace + orderId + clientOrderId + 状态 + 成交证据）；解析成功后把原事件重放进权威 matcher 重跑 ownership / fill 高水位 / incident；仅当事件被归属或解释、且账户数量不变量干净才解除 instrument_identity_unknown |
| P1-2 | 有任意 open order 即整体豁免数量核对 | 有界在途区间：confirmed local ± Σ(entry 剩余) ∓ Σ(exit 剩余)，按 side/origQty/executed/remaining/方向构造；`account_position_inflight_max_seconds`（默认 3600）过期后豁免失效。999 vs 11 + 一张工作订单的模拟现正确触发 mismatch |
| P1-3 | 缓存失效键缺 client 维度 | `_pm_note_stream_incident` 现在删除 (pair, order_id, *) 的全部 client-key；补 clean-cached → 新 incident → 立即失效重评估测试 |
| P1-4 | purge 删除持久子单 alias | `purge_resolved` 只删 exchange_order_id 与本地 Order 匹配的普通证据行；alias 行（子单真实 ID 不在 orders 表）永不清理。测试：alias 60 天 → purge → 存活 → 重启 → 子单事件仍解析 ownership |
| P1-5 | 回退文档把 purge 当成 git revert 可恢复 | §17 已拆分 code rollback 与 DB/data rollback，purge 不可逆 |

新增测试文件 `tests/freqtradebot/test_pm_protection_correctness.py`（31 项）与
`test_pm_round2_faults.py` / `test_pm_round2_pg_faults.py` 的补充项。

---

## 1. 第一轮实现中确认正确的部分

- 统一 PM 合约身份（canonical_pm_pair，raw id / settled pair / legacy alias 只在 UM
  命名空间内归一，spot 优先陷阱已修）。
- 终态 Order / 已关闭 Trade 仍可精确查询 ownership（_pm_owned_order 的 orders 表终态回查）。
- 持久化 incident journal（pm_stream_journal：首事件证据 + 累计成交高水位 + saw_filled），
  乱序 NEW 不会覆盖较新成交证据。
- 事件应用失败先 rollback 再落 incident（test_update_failure_rolls_back_before_journaling）。
- 条件单子订单 alias 持久化，重启后仍可关联已关闭 Trade。
- 相同报警去重（record_unresolved 返回 first，只通知一次）。
- 错合约 ACK 保留 raw evidence 并转 UNKNOWN（第二事故路径）。
- 前置写 intent/outbox（PREPARED+PENDING 同事务）→ resolve-by-clientId 重派，不重复 POST。
- WS / 定时 / RPC 恢复共用同一进程生命周期 RLock；RPC /pm_recover 保留审计。
- 保护下单（stoploss）不经过入场 gate。
- 交易所 ACK 证据（exchange_order_id + raw_response）在本地 LINK 前不删除。

## 2. 第二轮发现的第二事故路径（均已在本地修复）

| # | 问题 | Severity | 详情 |
|---|---|---|---|
| R2-1 | 真实 outbox payload 无 pair 键 → 已知订单被误判 FAIL-CLOSED | CRITICAL | _pm_enqueue 持久化的是 PAPI 请求体（含 raw symbol，如 DASHUSDT），但 _pm_client_owned_orders 只读 payload["pair"]。生产 DASH outbox 行存在时，任何带 clientId 的 WS 事件都会抛 instrument conflict → FAIL-CLOSED。第一轮测试用的是手写 {"pair": ...} payload，没覆盖真实形状。修复：payload 优先 pair，回退 symbol 按 PM 命名空间解析；解析不出时以 linked Order 行作权威归属（client id 唯一）。 |
| R2-2 | instrument_identity_unknown 是永久 gate | HIGH | grep 确认无任何自动解除路径。一个事件携带临时不在 markets 的 symbol 即永久 BLOCK。修复：记录未解析 raw symbol，recovery 重试 canonical 化，全部解析成功后自动解除。 |
| R2-3 | 全账户持仓数量不变量缺失（P0-1） | HIGH | 启动一致性只比较存在性不比较数量。新增 _pm_account_position_reconcile：按 instrument 汇总本地净敞口 vs 交易所 position（contracts 换算 + 相对容差 1% 默认），在途订单 instrument 跳过。不一致 → 专用 gate position_quantity_mismatch + 单次告警；收敛自动解除。 |
| R2-4 | 保护切换 crash 窗口（P0-2） | HIGH | trailing 止损先撤旧再建新，中间 crash = 无保护。PM 路径改为：建新 → 交易所验证 active → 只撤旧 id；任何失败路径保留旧保护（双 reduce-only 共存窗口安全）。 |
| R2-5 | 止损保护缺失无检测（Invariant 5） | HIGH | 新增 _pm_verify_stop_protection：stoploss_on_exchange 启用时 settled 非零仓位必须在交易所条件单列表可验证；缺失 → stop_protection_missing gate + 单次告警；循环重建后自动解除。 |
| R2-6 | 未知订单突发 × 全量恢复 = REST 权重放大 | MEDIUM | 每个新 unknown 订单触发一次全量恢复。修复：先定向 incident 恢复，全量扫受 user_stream_auto_recovery_cooldown_seconds（默认 60s）节流。 |
| R2-7 | 重放风暴的 DB 放大（P4） | MEDIUM | 每事件 3 次 DB 查询。新增 per-batch 归属解析缓存（只缓存已证实归属）与 clean-terminal 缓存（bounded 4096，新 incident 自动失效），10k 事件风暴 journal 查询恒定 3 次。 |
| R2-8 | gate release 基于过程快照而非决策时状态（P0-6） | MEDIUM | 释放判定改为决策时重读 journal（generation-safe），并修复 journal+内存双重计数回归。 |
| R2-9 | replay 解除 incident 后 gate 等下一周期 | MEDIUM | 已闭环：replay 解析 incident 后立即重评估剩余 durable incident（绝不 blanket clear）。 |
| R2-10 | resolved journal 行无 retention | LOW-MED | 新增 purge_resolved：保留期默认 30 天（可配），每小时至多一次，unresolved 永不清理；晚到事件仍可经 clientId/outbox 归属（outbox 永久审计）。 |
| R2-11 | 启动即 RUNNING，无冷恢复（P1-5） | MEDIUM | 启动流程追加 _pm_order_recovery()：订单 reconcile + incident 恢复 + instrument 重试 + 数量/保护不变量验证。 |

## 3. 修改文件

- freqtrade/pm_order_ownership.py：outbox payload 身份容忍；per-batch 归属缓存；clean-terminal
  缓存；resolve 缺失行跳过；generation-safe gate 判定；定向恢复优先 + 全量节流。
- freqtrade/freqtradebot.py：_pm_auto_recovery_due、_pm_retry_unresolved_instruments、
  _pm_account_position_reconcile、_pm_verify_stop_protection、_pm_purge_resolved_journal_if_due、
  _pm_switch_trailing_stoploss；_pm_reconcile_open_orders 接入两个不变量；_pm_order_recovery
  接入 instrument 重试与 retention purge；启动追加冷恢复；事件处理记录未解析 symbol +
  replay 闭环；每批重置归属缓存。
- freqtrade/persistence/pm_stream_journal.py：purge_resolved（bounded，unresolved 不删）。
- freqtrade/config_schema/config_schema.py + freqtrade/exchange/binance.py：三个新配置键
  （cooldown / tolerance / retention）进 schema 与 PM 键白名单。
- tests/freqtradebot/test_pm_recovery.py：make_pm_bot 提供与本地敞口一致的 fetch_positions 桩。
- 新增 tests/freqtradebot/test_pm_round2_faults.py（18 项，SQLite 确定性故障注入）。
- 新增 tests/freqtradebot/test_pm_round2_pg_faults.py（6 项，真实 PG 事务/重启/并发/purge）。
- .gitignore：忽略本地 scratch PG 与 pytest 证据目录。

## 4. Database migration

- 无 schema 变更，无 destructive migration。全部为 additive 行为：
  - pm_stream_journal 仅新增行删除策略（resolved 行 ≥30 天后 bounded 清理），表结构不变。
  - 新配置键有默认值，旧配置无需改动；schema 校验只增项。
- Rollback：改动全部为行为层，回退到 5bf5efc13 即恢复第一轮行为；journal 数据原样兼容。

## 5. Transaction 变更

- 新增路径沿用既有约定：journal 行随 Trade.commit() 提交，失败 rollback 并 block
  stream_journal_unavailable（不变）。
- purge_resolved 在 _pm_order_recovery 内 commit；失败仅告警，不影响 gate 语义。
- 不变量不引入新表，无跨表原子性要求；其 gate 是内存 reason，由 durable 来源动态补齐，
  restart 后由启动冷恢复重建。

## 6. Locking 变更

- 生命周期 RLock 不变；定向恢复与 replay 闭环都在 @pm_order_locked 覆盖内，不新增锁序。
- 跨进程依旧：PG advisory lock + 单实例 wrapper（同机 OS 锁 + account 级锁）。
  跨机器 fence 未实现（见 §16 B4）。

## 7. Gate / Recovery 行为 before / after

| 场景 | before | after |
|---|---|---|
| 已归属终态订单的晚到事件 | 可吸收；但真实 outbox payload 会误报冲突 → FAIL-CLOSED | 吸收为 KNOWN_LATE/DUPLICATE，不 block 不通知不 REST |
| 单个未知事件 | block + 每 5 分钟恢复重试 | 同左 + 立即定向恢复 + 全量节流 |
| 未知 instrument 事件 | block 永久（直到重启） | block，markets 解析成功后自动解除 |
| 恢复完成释放 gate | 基于循环内快照 | 决策时重读 journal（generation-safe） |
| replay 解决 incident | 等下一周期释放 | 立即重评估并释放 |
| 数量漂移 | 不检测 | SAFE_HOLD（专用 reason），收敛自动解除 |
| 保护缺失 | 不检测 | SAFE_HOLD（专用 reason），循环重建后自动解除 |

风险增加（ENTRY/DCA/PYRAMID）与风险减少（EXIT/STOPLOSS/REDUCE_ONLY）分离不变：所有新 gate
只作用于 risk increase；create_stoploss_order、execute_trade_exit、_safe_force_exit、
_pm_force_close_foreign_position 均不经过 entry gate（有测试覆盖）。

## 8. Protection switching before / after

- before：cancel 全部 open SL → create 新单；crash 窗口 = 无保护。
- after（PM）：create 新单 → fetch 验证 active → 只撤旧 id → 本地记账。create 失败 / 验证
  失败：保留旧保护（双 reduce-only 共存窗口，安全）。非 PM 路径保持原行为。

## 9. Restart 变更

- startup（PM 实盘）追加第 4 步 _pm_order_recovery()：订单 reconcile、durable incident 恢复、
  未解析 instrument 重试、位置数量与止损保护不变量验证、journal retention purge。
- STARTING 期间新敞口保持 fail-closed。覆盖 P1 启动矩阵（position/open order/终态差异/
  UNKNOWN intent/未决事件/条件单/gate reason 重建）。

## 10. User Stream matching before / after

- before：open-order 索引 + 终态 Order 回查 + outbox/intent clientId 回退；outbox payload
  pair 假设（生产数据不成立，R2-1）。
- after：同一优先级，outbox payload 兼容 raw symbol；per-batch 解析缓存；clean-terminal
  缓存；状态转换幂等（NEW/PARTIAL/FILLED replay + 累计量高水位 + REST 单调性校验）。

## 11. PostgreSQL integration result

环境：本地便携 PostgreSQL 16.4（仓库内 .pg-portable，gitignore），端口 5433，trust 认证，
隔离库 pm_scratch，psycopg2-binary 2.9.12。

- 此前 skipped 的集成项现在全部执行：
  - test_pm_pg_fault_injection.py：4 passed（POST 前 crash 精确重派一次 / ACK 后 commit 前
    crash 保持 block 不重 POST / LINK 与本地 commit 原子 / advisory lock 跨 session 互斥）。
  - test_pm_order_intent.py PG 集成项：passed。
- 新增 test_pm_round2_pg_faults.py：6 passed（表/唯一约束、record→rollback gate 保持、
  跨引擎重启可见、resolve→rollback 保持再 commit 释放（真实 FK）、purge 保留近期与
  unresolved、双 session 并发同 key 唯一约束只产出一行）。
- 结论：持久化语义不再以 skipped 交付。真实 PG17 生产升级演练仍属 staging（§16 B3）。

## 12. Fault injection matrix（第二轮新增）

| 路径 | duplicate | late/out-of-order | crash/rollback | release 语义 |
|---|---|---|---|---|
| 已归属终态订单 | 10,000 事件风暴：0 block、0 通知、journal 查询恒定 | REST 先终态后 174/10k WS 重放 | — | — |
| 未知订单突发 | 同 key 只通知一次 | 5 个 distinct unknown：定向 5 次、全量 1 次 | journal 落库失败 → stream_journal_unavailable | 定向失败保持 block |
| 多 incident ABA | — | A 可解 B 不可解 → gate 保持；B 解决 → 释放 | 释放后新 incident → 重新 block | generation-safe 重读 |
| 数量不变量 | 重复 reconcile 不重复告警 | 在途 instrument 跳过 | 交易所读取失败 → fail-closed | 数量收敛自动解除 |
| 保护切换 | — | 建新→验证→撤旧（顺序断言） | create/验证失败保留旧保护 | 保护缺失 gate 重建后解除 |
| instrument 身份 | — | 未知 symbol → block；markets 出现后自动解除 | 仍未知 → 保持 block | 全部解析成功才解除 |
| journal retention | — | — | purge 只删 resolved ≥30d；每小时一次 | unresolved 永不删 |
| PG（真实） | 并发同 key 唯一约束一行 | 跨引擎重启可见 | record/resolve + rollback 保持 gate 关闭 | commit 后释放 |
| 单实例锁 | — | — | 同机 wrapper 锁 + account 锁（回归 12 passed） | 见 §16 B4 |

## 13. Test result

- 完整 PM 定向套件（test_(pm_|binance_pm|state_persistence)，24 文件，含 FREQTRADE_TEST_PG_URL
  与新增 protection-correctness 文件）：**375 passed / 0 failed / 0 skipped**。
- 其中新增：
  - test_pm_protection_correctness.py：31 passed（P0-1..P0-6 全部确定性反例）。
  - test_pm_round2_faults.py：22 passed（含 10k 风暴、gate ABA、instrument 证据重放、
    有界在途区间、cache 失效、alias 存活重启）。
  - test_pm_round2_pg_faults.py：6 passed（真实 PG 事务/重启/并发/purge-alias）。
  - test_pm_pg_fault_injection.py：4 passed；test_pm_order_intent PG 集成：passed。
- 新增模块 Ruff 通过。全仓 lint / 全仓测试未跑（与第一轮一致，超出本轮范围）。

## 14. Remaining unverified boundaries（写实，不宣称安全）

B1. kill -9 成交提交后、通知/新止损重建前的窗口（第一轮 §6 已列，属 staging 演练）。
B2. DB 完全不可用时紧急退出（stoploss 前置写依赖 DB，届时同 pair intent fail-closed 拦截
    保护创建——保守但需演练确认可接受）。
B3. 真实 PG17 旧库升级 + 新表约束验证（本地只跑了 PG16.4 隔离库）。
B4. 跨机器 / 多 API-key 实例竞争：同机 wrapper 锁 + account 锁 + PG advisory lock + IP
    白名单；无 epoch/fencing，网络分区后旧 leader 恢复可继续下单。
B5. 全账户数量守恒与外部手工仓位并轨：数量不变量会把无法解释的仓位视为 mismatch →
    SAFE_HOLD（保守），合并净仓批准策略未定。
B6. 晚到费用修正、重复 partial 的 REST 放大（governor 权重上限存在，未做专项注入）。
B7. 只读/断电文件系统下 PAUSED 恢复。

## 15. Production rollout prerequisites（staging → shadow → 生产）

1. **单实例约束（本轮强制）**：staging/生产必须经 scripts/pm_single_instance.py wrapper
   启动（同机 OS 锁 + account 级锁 + PG advisory lock + API-key IP 白名单）。跨机器部署
   不在本轮范围；在完成 B4 之前禁止双机/双进程同时持有 API key。
2. staging 隔离 Binance 账户 24/7 shadow：WS 断线/重连、REST 超时、kill -9、PG 切换、
   listenKey 过期风暴、DCA 成交后 kill、trailing 替换中途 kill（重点验证旧保护始终存活）。
3. 生产 PG17 先在 staging 做迁移演练（本仓库无 schema 变更）。
4. 明确 B5：生产现有外部/手工仓位清单与数量不变量容忍配置。
5. 冷启动 reconcile 演练：RECOVERING 期间新敞口 BLOCK、保护持续、invariants 通过后自动
   RUNNING，全程不依赖 /pm_recover。
6. canary：小仓位单 instrument 实盘观察 24h，再解除全局新单 gate。

## 16. 仍 BLOCK 生产无人值守验收的项目

- B1（kill -9 成交提交后通知/止损重建窗口）、B3（真实 PG17 升级）、B4（跨机器 fencing）
  在 staging 验证完成前，不得宣称生产无人值守验收通过。B4 在本轮由 §15.1 的单实例约束
  兜底（staging 期间禁止多机部署）。
- 服务器未修改、未重启、实盘门禁未解除——本交付不包含任何上线动作。

## 17. Rollback plan（code 与 data 分开说明）

- **Code rollback**：git revert 相应提交即回到 5bf5efc13 代码行为（本仓库无 schema
  变更，回退后旧代码可继续使用同一数据库）。
- **DB/data rollback**：git revert 不能恢复任何已被 purge / 状态机删除的数据。
  - pm_stream_journal：purge 只删除 resolved 且 ≥30 天的普通证据行；**持久子单 alias
    行永不删除**（P1-4），未决行永不删除。若需完整审计证据，请在开启保留期清理前
    对 pm_stream_journal 表做一次逻辑备份。
  - pm_order_intents：LINKED 行由既有对账墓碑删除（outbox 保留永久审计）。
  - 任何已执行的数据变更（purge / tombstone）都不可逆，需按运维备份流程恢复。
- 配置层：新键（cooldown / tolerance / retention / inflight_max）可留可删（有默认值）。

