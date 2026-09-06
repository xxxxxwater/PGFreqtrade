# PM ownership — 第一轮实现与第二轮审计交接

状态：本地候选，未部署，未批准长期无人值守。范围为原生分支 `pm-redo-native`，
起点 `cb2b466d7`。本轮没有修改服务器、触发 pm_recover、下单、撤单、重启或解除实盘闸门。
用户指定 GPT6-ASTRA 做架构和第一轮实现，后续审计由用户安排；本文件不代表第二轮已通过。

## 实盘证据与根因

2026-09-05 北京时间：23:45:09 创建订单；23:45:13 REST 先确认成交；
23:45:14 本地 Trade 完成入场更新；23:45:18 才消费用户流队列。
`10660398840` 是 Binance orderId，非独立事件序号。数据库 Order #5 为 closed、
Trade #2 为 OPEN，pair 为 `DASH/USDT:USDT`。对应 intent 已清理；outbox 为
RECONCILED，仍保留 clientId、exchange_order_id、linked_order_id、linked_trade_id=2。

旧 matcher 仅索引 open orders，因此 REST 胜出的正常成交被误判为丢失 ownership。
另一个独立缺陷是按 markets 中第一个相同 raw id 取 symbol，命中了现货 `DASH/USDT`。
每条未匹配消息均发送通知/调用 recovery，且 recovery 的空 open-order 扫描不能证明
该未匹配事件已解决。多条同 orderId 的日志不能单独证明网络重传：可能包含不同成交事件、
排队的 NEW/PARTIAL/FILLED 以及重复事件，必须比较原始事件字段才能进一步区分。

## 身份与状态约束

- canonical instrument 保持 Freqtrade 已有 settled contract symbol，不另造 DB pair 格式。
- `DASHUSDT` / legacy `DASH/USDT` 仅在明确 UM namespace、加载元数据证明唯一时归一化。
- 不剥掉结算后缀，不盲拼 USDT；不同 settle、UM/CM、普通单/Algo 单不得按数字 ID 混用。
- Order/事件 key = `(canonical instrument, orderId)`；clientId 经保留的 outbox/intent
  查到已提交的本地 Order/Trade 后才可以关联。clientId 前缀只是线索，不是接管授权。
- 终态 Order 与关闭的 Trade 仍可供精确查询；不靠 TTL 删除 ownership。
- 条件单真实子订单映射同时写入 `pm_stream_journal`，重启后可查询已关闭 Trade 的别名。
- PM 适配器仍只执行 UM；pure resolver 能区分 CM 不等于新增了 CM 实盘交易支持。

## 六类事件动作

| 分类 | 必须具备的证据 | 第一轮动作 | 解除/恢复边界 |
|---|---|---|---|
| KNOWN_LATE | instrument+Order+Trade 归属成立，本地终态，事件为较早 NEW/PARTIAL 且累计成交不超过本地 | 不重复更新交易/费用/通知，保留 ownership | 不新增 block；存在历史事件高水位时必须满足它 |
| KNOWN_DUPLICATE | 归属成立、终态一致、累计成交不增加 | 同上 | 不以“同 orderId”直接忽略新增成交或冲突终态 |
| RECOVERABLE_UNKNOWN | 身份可归一，但归属暂无法证明，或 REST/DB 查询失败 | durable incident、禁新增敞口、一次通知、周期恢复 | 精确 Order/Trade 关联 + REST 身份/状态/累计成交验证 + 本地提交 |
| FRAMEWORK_ORPHAN | outbox/intent 有交易所接受证据，缺已提交的本地关联 | 保留证据并 block，不自动创建 Trade、不重下单 | 本轮仅能关联之后已存在的正确本地记录；真正 orphan 仍需审计裁决 |
| EXTERNAL_ORDER | 无本地关联、无框架 client 线索、条件子单排查无关联 | 只读账户刷新，不写入策略 Trade、不擅自撤单 | 不能据此推断账户没有额外敞口；由账户级对账继续判断 |
| UNEXPLAINED_EXPOSURE | 账户持仓无法由本地 Trade 解释 | 现有 startup consistency 的 unknown_positions 路径，默认 pause；绝不仅凭 pair 接管 | 全账户数量/方向/在途变化验证仍是第二轮重点，不能把“有同 pair Trade”当作完整证明 |

第一轮显式分类/日志已覆盖 order-event 路径。UNEXPLAINED_EXPOSURE 枚举与上表是账户
对账的契约，**不声称已完成新的、持续运行的全账户数量守恒 reconciler**。

## Gate 反向证明（本轮相关范围）

| Gate/hold | 为什么 block | 自动解除证据/负责人 | restart | reduce-only |
|---|---|---|---|---|
| unmatched_stream_order | 可能已成交但尚不能证明归属 | `_pm_recover_stream_incidents` 查精确 durable incident，REST 不落后于事件高水位，本地 Order 一致，提交成功 | 查询 `pm_stream_journal.unresolved`，不能靠重启抹除 | 不接到 entry-only gate 之外；保护下单有定向测试 |
| stream_journal_unavailable | 无法持久证明未决事件清空 | journal 查询/提交恢复后才解除；内存未知记录未清时仍 block | 表/数据库不可读时 durable gate 失败关闭 | 数据库完全不可用是否仍可紧急退出需第二轮隔离演练 |
| instrument_identity_unknown | 不能安全归属交易工具 | 本轮不以猜测解除；元数据/命名空间修复后需要再对账 | startup 会重新核对账户；未知 symbol 的独立持久事件捕获待补审 | 身份未知时不能对猜测合约下退出单；已知仓位保护应独立工作 |
| unresolved_intent / pending_intent_unresolved | POST 结果或本地提交不确定 | 原有 intent/outbox recovery；缺字段/未知工具现在返回 uncertain | 原有 PG intent/outbox 持久恢复 | reduce-only intent 不代表可无条件丢弃证据；全局 DB gate 仍需复审粒度 |
| reconciliation_incomplete | 某个必要读取或生命周期更新失败 | 原有多个调用点负责，第一轮只补订单事件错误 rollback 和 incident 验证 | startup 重建/对账 | 原有控制流，非全表状态机重写 |
| user_stream_unavailable / events_dropped | 消息视图可能不完整 | 原有 health monitor + recovery；不得当成 incident gate 的替代证据 | SYNCING 初始阻断 | 入场门禁不应拦截已有仓位减仓 |
| startup_consistency_error/mismatch | 启动读取失败/账户与数据库不符 | 现有 configured startup 模式；真正外部/孤儿仓位不自动接管 | PAUSED 持久化，重启重查 | PAUSED 与 STOPPED 行为必须分别验收，不能假设 STOPPED 仍处理仓位 |

注意：上表不等于已证明所有历史 risk/data/daily-loss gate 都完备。仓库中尚有共享
`reconciliation_incomplete` 被多个子系统清除的设计；第二轮必须验证一个组件恢复不能
清除另一个组件的失败原因。`startup_consistency_mode=warn` 也不能作为无人值守验收模式。

## 事务、并发与 crash 边界

1. 原有 PREPARED+outbox 在 POST 前同事务提交；ACK 保存交易所编号和原始响应；
   LINK 与本地订单提交关联。新增身份校验拒绝错合约 ACK，但先保存其 raw ACK 并标 UNKNOWN，
   防止“已接受却只剩可替换 PREPARED”的第二事故。
2. matcher 不只读内存 open 集合；精确历史 Order 查询与持久 clientId journal 构成回退。
3. incident 保留首事件，另外保存累计成交最大值和曾观察 FILLED 的标记，乱序 NEW 不得覆盖
   较新的成交证据。反复未知事件不反复全量恢复/发通知。
4. 调度 recovery、WS 消费和 RPC recovery 共用生命周期 RLock。该锁只管同进程，不是
   分布式租约，不替代现有账户级单实例防线。
5. 事件应用异常先 rollback，再落库 incident，避免把半更新的 Order/Trade 一起提交。
   recovery 清除事件前校验 REST 不倒退、本地状态/数量已更新；提交失败不能宣布解除。
6. **未解决的原有边界**：`update_trade_state` 中有提交，`_update_trade_after_fill` 又含
   撤止损、wallet、strategy callback 等外部副作用。RLock 与 incident 不等于这些副作用
   获得跨进程 exactly-once。成交提交后、通知/新止损前崩溃，以及已撤旧止损未建新止损窗口，
   需要第二轮真实 PG/隔离交易所故障注入，不能以 SQLite 单测替代。

## 第二轮最优先的故障注入矩阵

| 路径 | duplicate | late / out-of-order | crash-before-commit | crash-after-accept |
|---|---|---|---|---|
| 普通入场 | 同订单174消息不重复成交通知 | REST先终态、WS后NEW/PARTIAL/FILLED | 半更新rollback，incident仍未决 | 原始ACK/outbox保留；无Trade时FRAMEWORK_ORPHAN |
| 部分成交→撤单→成交 | 不重复累计数量 | 更大累计量不能当旧消息忽略 | Order和Trade金额一致 | 重派只查原clientId，禁止新client重单 |
| Algo→实际子订单 | 同父/子ID不能重复平仓 | 重启/Trade已关闭仍能关联子单 | alias+Order外键有效、无错误归属 | Algo ACK与child ID的身份边界分别验收 |
| Recovery→Gate | 重复recover幂等 | 新事件不能在验证与解锁之间穿插 | journal清除提交失败继续block | 空open-order扫描不得清unknown事件 |
| 退出→保护 | 重复退出不超减 | 旧止损事件不能错误撤新止损 | 撤旧保护后崩溃恢复 | 真实reduce-only FILLED、仓位归零才完成 |

额外未批准项：跨进程/多API-key实例竞争；实际 PG17 旧库升级并验证新表约束；不同交易工具
相同 orderId；缺失/歧义工具；外部手工成交与策略同 pair 合并净仓；新仓保护缺失时限；
重复 partial 消息导致 REST 放大；晚到费用修正；只读或断电文件系统下 PAUSED 恢复。

## 验证与交接

- 本轮 PM 定向套件：311 passed、2 skipped；跳过原因为缺少 psycopg2、未配置隔离
  `FREQTRADE_TEST_PG_URL`。未宣称跑过全仓测试或真实 PG17 故障注入。
- 新增 identity/ownership/journal 模块和对应新增测试的 Ruff 检查通过；被修改的大文件
  仍有复杂度等 lint 告警，本轮未做全仓清理，不能把新增模块通过写成全仓 lint 全绿。
- 定向新增 ownership 测试涵盖 terminal Order/closed Trade 查询、spot优先陷阱、instrument
  ID碰撞、outbox client回退、重复未知消息去重、空扫描不解锁、自动精确恢复、REST冲突、
  撤单后新增成交、事件高水位、部分更新回滚、子单重启关联、RLock串行、保护不经入场gate。
- PMStreamJournal 的 SQLite 事务/约束测试及 PostgreSQL 方言 DDL 编译已执行；DDL 编译
  **不等于**真实 PG17 故障注入。
- 全量 PM 定向套件命令（PowerShell，仓库根）：

```powershell
$pmTestFiles = @(rg --files tests | Where-Object { $_ -match '(^|[\\/])test_(pm_|binance_pm|state_persistence)' })
.\.venv\Scripts\python.exe -B -m pytest -o addopts='' -p no:cacheprovider -q @pmTestFiles --basetemp=.pytest-pm-round2
```

- 新增表 `pm_stream_journal` 由 metadata.create_all 创建；备份恢复验收变为7表。
  旧版备份须隔离恢复并执行新版本迁移，再使用新版检查，不能直接替换生产库。
- 此交接不包含服务器切换。实盘仍有 DASH 仓位时，先完成第二轮、验收保护和部署窗口，
  不可自动重启/蓝绿并跑，也不可把现存交易强制改成外部仓位或新 Trade。
