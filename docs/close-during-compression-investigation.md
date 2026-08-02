# 压缩中遇到优雅关闭：消息送达边界调查

调查对象：本仓库（`ws`
8.21.1）中“压缩后的大快照刚发出就开始优雅关闭”时，消息在发送管线中的位置判定。仅做行为调查与复现，未改动
`lib/`、公开 API 或协议语义，未新增依赖。

复现测试：[`test/close-during-compression.test.js`](../test/close-during-compression.test.js)（mocha，真实本地服务端/客户端，`npm test`
会一并执行）。

## 结论速查

| 现场可观察信号                                                            | 消息所处位置                                 | 交付判断                                                                      |
| ------------------------------------------------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------- |
| `send()` 回调报错 `WebSocket is not open: readyState 2 (CLOSING)`         | 从未进入发送层（被 `WebSocket#send` 拒绝）   | 确定未发出，对端不可能收到                                                    |
| `send()` 回调报错 `The socket was closed while data was being compressed` | 已被发送层接收，但压缩完成前 socket 已销毁   | 确定未写入 socket，已丢失；同一发送队列里排队的消息同样丢失                   |
| `send()` 回调无错误，之后 `'close'` 事件码为协商码（如 1000）             | 数据帧已写入本地 socket，且先于 close 帧发出 | 对端在协议上先于 close 帧收到该消息；除非底层连接异常，可视为已送达对端接收层 |
| `send()` 回调无错误，之后 `'close'` 事件码为 1006                         | 数据帧已写入本地 socket，但连接随后异常断开  | 无法确定对端是否已处理；回调成功从来不等于对端已收到                          |

## 两个关键边界，以及它们与“对端实际收到”之间隔着什么

### 边界 A：已被发送层接收

`WebSocket#send()`（`lib/websocket.js:455-485`）先做 `readyState` 检查：

- 若状态不是 `OPEN`（例如 `close()` 已被调用、状态为 `CLOSING`），走
  `sendAfterClose()`（`lib/websocket.js:1138-1159`）：消息**不会**进入 `Sender`
  的队列，回调在下一 tick 收到
  `WebSocket is not open: readyState 2 (CLOSING)`。注意它仍会把长度累加进
  `_bufferedBytes`（`lib/websocket.js:1148`），所以 `bufferedAmount`
  会“虚涨”，但这不代表任何发送动作。
- 若状态为 `OPEN`，消息被交给
  `Sender#send()`（`lib/sender.js:351-414`），此刻才算“已被发送层接收”。如果需要压缩，消息进入
  `dispatch()`（`lib/sender.js:503-529`），`Sender` 状态从 `DEFAULT` 变为
  `DEFLATING`（`lib/sender.js:22-24`），并调用 `PerMessageDeflate#compress()`。

**压缩是异步的**（zlib 线程池），从 `send()`
返回到帧真正发出之间有一个明确的时间窗。窗口期内，`Sender` 处于
`DEFLATING`，所有后续发送操作——包括 close 帧——都通过
`enqueue()`（`lib/sender.js:551-554`）进入 `_queue` 排队，等压缩完成后由
`dequeue()`（`lib/sender.js:536-543`）按序执行。

### 边界 B：已写入本地 socket

压缩完成的回调里（`lib/sender.js:513-528`）：

1. 先检查 `this._socket.destroyed`。若 socket 已销毁，调用
   `callCallbacks()`（`lib/sender.js:585-594`）：当前回调**和队列中所有排队操作的回调**都收到
   `The socket was closed while data was being compressed`，消息丢弃。此路径不触发
   `sender.onerror`，因此不会伴随 `'error'` 事件——唯一的信号就是 `send()`
   的回调错误。
2. 否则 `sendFrame()`（`lib/sender.js:563-572`）执行
   `socket.cork() → socket.write(帧头) → socket.write(负载, cb) → socket.uncork()`。`socket.write`
   的回调（即 `send()` 传入的回调）在数据被写入流/内核发送缓冲区后触发。

**这就是“已写入本地 socket”**：`send()` 回调成功只说明数据交给了本地 TCP 栈。

### 边界 B 与“对端实际收到”之间还隔着

1. 本地内核发送缓冲区 → 网卡 → 网络 → 对端内核接收缓冲区（可能被对端
   `pause()`、拥塞控制、丢包重传拖延）；
2. 对端 socket 的 `'data'` 事件 →
   `Receiver`（`lib/websocket.js:1373-1377`）按帧解析；
3. 若带 RSV1，`Receiver` 再经 `PerMessageDeflate#decompress()`（同样过
   `zlibLimiter`）解压、校验；
4. 最后才触发对端的 `'message'` 事件（`lib/websocket.js:1238-1240`）。

本仓库不提供任何“对端已确认收到”的信号；要确认送达只能由应用层协议自带 ACK。复现测试场景 8 演示了这段距离：对端暂停读取时，本端
`send()` 回调已成功、close 帧也已排队发出，但对端尚未触发 `'message'`。

## 压缩协商、发送队列、回调与关闭握手在代码中的关系

### 压缩协商

- 服务端：`WebSocketServer` 默认
  `perMessageDeflate: false`；开启后在 upgrade 阶段 `accept()`
  客户端的 offer（`lib/websocket-server.js:299-313`），并把实例挂到
  `extensions['permessage-deflate']`，随 101 响应返回协商参数（`lib/websocket-server.js:414-417`）。
- 客户端：默认 `perMessageDeflate: true`，`offer()`
  发起协商（`lib/websocket.js:769-776`），收到响应后
  `accept()`（`lib/websocket.js:1018-1026`）。
- 协商结果共用一份实例，挂在 `ws._extensions` 上，`Sender` 与 `Receiver`
  各取所需。`WebSocket#send()` 只有在扩展存在时才保持
  `compress: true`（`lib/websocket.js:480-482`），否则静默不压缩。

### 压缩执行

`PerMessageDeflate#compress()`（`lib/permessage-deflate.js:322-329`）把任务丢进**进程级全局**
`zlibLimiter`（`lib/limiter.js`，默认并发 10，`lib/permessage-deflate.js:65-71`），实际的 deflate 在
`_compress()`（`lib/permessage-deflate.js:404-460`）中通过 `Z_SYNC_FLUSH`
完成并剥掉 4 字节尾。高峰期多个连接共享这个限流器，压缩窗口会被进一步拉长——这正是现场容易撞上的时序。

### 关闭握手与压缩的相遇点

`WebSocket#close()`（`lib/websocket.js:302-340`）把状态置为 `CLOSING` 并调用
`Sender#close()`（`lib/sender.js:184-229`）：

- 若 `Sender` 正在压缩（`_state !== DEFAULT`），close 帧走 `enqueue()`
  排到压缩消息**之后**（`lib/sender.js:224-225`）；否则立即发帧。
- close 帧写入 socket 后，`_closeFrameSent = true`；若此时对端 close 帧也已收到（`_closeFrameReceived`），执行
  `socket.end()` 半关闭（`lib/websocket.js:329-336`）。注意用的是 `end()` 而非
  `destroy()`，已写入的数据会被冲刷出去。
- 同时
  `setCloseTimer()`（`lib/websocket.js:1309-1314`）启动兜底定时器：`closeTimeout`（默认 30 秒，`lib/websocket.js:671`）内握手未完成就
  `socket.destroy()`，之后所有在途压缩都会以边界 B 第 1 条的错误收场。
- 对端 close 帧到达时，`Receiver` 发出
  `'conclude'`，`receiverOnConclude()`（`lib/websocket.js:1168-1182`）回显
  `close(code, reason)`——同样经 `Sender#close()`
  排队，不会插队在途压缩消息之前。
- socket `'close'` 后 `emitClose()`（`lib/websocket.js:266-280`）调用
  `PerMessageDeflate#cleanup()`（`lib/permessage-deflate.js:130-150`）：若此刻 deflate 仍在处理，其回调会收到
  `The deflate stream was closed while data was being processed`。

由此得到不变式：**只要走优雅关闭且 socket 存活，任何已被 `Sender`
接收的消息都会先于 close 帧写入 socket**；close 帧永远不会越过在途压缩消息。风险只来自三处：`close()`
之后再 `send()`（边界 A 拒绝）、socket 在压缩完成前被销毁（对端
`terminate()`、网络故障或 `closeTimeout`
兜底）、以及把“回调成功”误读为“对端已处理”。

## 三种关闭时序的对比

下表汇总三种时序下“正在压缩的数据、排队中的发送、回调、close 事件”各自的结果（对应复现场景1/3/4/5/6/7）：

| 时序                                      | 正在压缩的数据         | 排队中的发送                          | `send()` 回调                     | close 帧                       | `'close'` 事件码        |
| ----------------------------------------- | ---------------------- | ------------------------------------- | --------------------------------- | ------------------------------ | ----------------------- |
| 本地优雅关闭 `close()`                    | 压缩完成后正常成帧发出 | 按序全部发出，仍先于 close 帧         | 全部成功                          | 排在最后一个排队消息之后       | 双方为协商码（如 1000） |
| 本地强制终止 `terminate()`                | 压缩结果被丢弃，不成帧 | 全部丢弃，从不出队                    | 全部报 `...being compressed` 错误 | 不发出                         | 双方为 1006             |
| 对端在压缩中开始关闭（收到对方 close 帧） | 压缩完成后正常成帧发出 | 按序全部发出；回显的 close 帧排在队尾 | 全部成功                          | 回显帧排在最后一个排队消息之后 | 双方为协商码（如 1000） |

注意两点：

- **对端先关不等于接收侧截断**：`Receiver` 在 `'conclude'`
  之前持续解析，主动发起关闭的一方仍会按序收到全部在途消息（场景3/7）。
- **`closeTimeout` 兜底会把第一行退化为第二行**：30 秒内握手未完成就
  `socket.destroy()`，之后的观察者看到的是 1006 + 回调报错，与 `terminate()`
  无法区分。

## 现有实现为何是这样

1. **为什么 close 帧排队而不是插队在途消息之前**：`Sender`
   以“一整条消息”为原子单位（`DEFAULT` / `DEFLATING` / `GET_BLOB_DATA`
   三态，`lib/sender.js:22-24`）。RFC
   6455 虽然允许控制帧穿插在消息分片之间，但实现选择在消息级排队：压缩中的消息不可被截断，线上帧序严格等于应用调用序，close 帧只是继承了这个约束。代价是 close 帧的延迟取决于队首压缩的耗时（高峰期还包含
   `zlibLimiter` 排队），大快照之后立刻 `close()` 时尤为明显。
2. **为什么优雅关闭用 `socket.end()` 而非
   `destroy()`**：让已写入的数据被 TCP 正常冲刷出去，也允许对端把它的排队数据发完（`lib/websocket.js:1290-1294`
   的注释明确写了这一点）。`destroy()` 只出现在 `terminate()` 与 `closeTimeout`
   兜底路径。
3. **为什么压缩回调检查 `socket.destroyed` 而非 `readyState`**：优雅关闭期间
   `readyState` 已经是 `CLOSING`，但 socket 存活且可写；只有 `destroyed`
   才代表“写不出去了”。同一条压缩路径因此按socket 状态分流：优雅关闭走成功分支，销毁走失败分支——这正是三种时序结果迥异的根源。
4. **为什么丢失只通过回调报告、不发 `'error'`
   事件**：`callCallbacks()`（`lib/sender.js:585-594`）不调用
   `sender.onerror`。关闭竞态属于预期路径而非连接异常；而且
   `senderOnError()`（`lib/websocket.js:1281-1301`）会对 socket 再
   `end()`，对已销毁的 socket 没有意义。结论：**这种丢失唯一的现场信号就是
   `send()` 回调的错误**。
5. **为什么排队中的消息没有独立状态**：`_queue`
   中的操作出队后才执行（`lib/sender.js:536-543`），其命运由队首压缩的结果决定——成功则按序出队成帧，失败则被
   `callCallbacks()`
   一并清掉。它们的回调时序因此总是跟在队首消息之后，且与发送顺序一致（场景 5/6/7 断言了回调顺序）。
6. **为什么异常时 `'close'` 码恒为 1006**：`_closeCode`
   在构造时初始化为1006（`lib/websocket.js:58`），只有 `receiverOnConclude()`
   收到对端 close 帧才覆盖为协商码。1006 因此等价于“关闭握手没有完成”，与消息是否发出无关。

## 投递台账口径

接入方把 `send()`
回调接到投递台账时，按下表记账；核心原则：**台账只记账到“本机”边界为止，“对端已送达”这一栏没有任何库层信号可以填**。

| 可观察信号                                                                     | 可以记为                                                    | 绝不能记为                                           |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------- | ---------------------------------------------------- |
| `send()` 在 `CONNECTING` 同步抛错，或回调报 `not open: readyState 2 (CLOSING)` | 网关未受理（未进入发送层）                                  | 本机已接收；对端已送达                               |
| 回调报 `The socket was closed while data was being compressed`                 | 本机已接收；确定未写入 socket、未发出                       | 对端已送达                                           |
| 回调成功                                                                       | 已写入本地 socket（本机已发出）                             | 对端已送达；对端已处理                               |
| 回调成功 + `'close'` 为协商码                                                  | 可附加一条：“先于 close 帧发出，对端接收层在协议上先看到它” | 对端已送达（仍可能卡在对端内核、解压或应用处理之前） |
| `'close'` 为 1006                                                              | 连接未完成握手；已写入的数据也可能未到达                    | 任何送达结论                                         |
| “对端已送达”                                                                   | 只能由应用层 ACK 产生                                       | 任何库层信号，包括回调成功                           |

操作原则：

- **以回调为唯一记账时点**。`send()`
  返回时消息可能还在 zlib 线程池里，此刻连“本机已发出”都不成立。
- 排队消息的回调顺序与发送顺序一致，可安全按回调次序落账。
- 需要“对端已送达”语义的业务，只能在应用协议里加确认帧；本仓库的实现不提供也不暗示这一语义。

## 复现场景与观察结果

测试用 1
MiB 不可压缩负载（`crypto.randomBytes`）把 deflate 窗口拉宽到数十毫秒，远大于 localhost 往返，时序在实际运行中是确定的。

1. **先 `send()` 后 `close()`（优雅关闭撞上在途压缩）**：对端先触发
   `'message'`（负载逐字节一致），再触发 `'close'`（码 1000）；本端 `send()`
   回调无错误。证实顺序保证。
2. **`close()` 之后再 `send()`**：回调收到 `readyState 2 (CLOSING)`
   错误，对端只收到第一条消息。证实边界 A。
3. **对端先发 close 帧、本端正在压缩**：发起关闭的一方仍然完整收到在途消息后才完成握手（双方 1006 未出现，均为 1000）。说明“我先关了”不等于“收不到对方已发出的消息”。
4. **压缩期间对端
   `terminate()`（对照组，非优雅关闭）**：本端在压缩消息和排队消息的两个回调都收到
   `The socket was closed while data was being compressed`，`'close'`
   码 1006，对端什么都没收到。证实边界 B 的丢失信号，以及与优雅关闭的区别。
5. **本地优雅关闭 + 排队消息**：`send(A); send(B); close(1000)`
   连续调用后，对端按序收到 A、B 再收到 close（1000）；两个回调按发送顺序成功。证实排队消息在优雅关闭下不丢失、不越过 close 帧。
6. **本地 `terminate()` + 排队消息**：`terminate()`
   同步销毁 socket 后，在途压缩消息与排队消息的两个回调都报
   `The socket was closed while data was being compressed`；对端零消息，双方
   `'close'` 码 1006。与场景 5 构成同一调用序列的两种结局。
7. **对端在压缩中开始关闭 + 排队消息**：对端 `'open'` 后立即 `close(1000)`，本端
   `send(A); send(B)`。发起关闭的一方仍按序收到 A、B，握手以 1000 完成；本端两个回调成功，回显的 close 帧排在 B 之后。证实对端发起关闭不会截断本端发送队列。
8. **回调成功 ≠ 对端已处理**：对端 `pause()` 期间，本端 `send()`
   回调成功、close 帧已发出，但对端尚未触发 `'message'`；`resume()`
   后才按“消息 → close（1000）”完成。量化边界 B 之后的距离。

运行：

```sh
npx mocha --throw-deprecation test/close-during-compression.test.js
```

8 个场景全部通过（连跑多轮无抖动）。

## 给接入方的现场判断口径

高峰期“快照刚发出就优雅关闭”时，按回调与 `'close'` 事件码即可定位消息：

- 两笔都关心时，**以 `send()` 回调为准而不是 `send()`
  返回**：返回时消息可能还在 zlib 线程池里。
- 回调无错误 + 正常关闭码 ⇒ 消息先于 close 帧离开发送侧；若对端随后报未收到，问题在边界 B 之后（对端读取、解压或应用处理），不在本端发送管线。
- 回调报 `being compressed`
  错误 ⇒ 消息在压缩窗口内随 socket 销毁而丢失，需要应用层重发；若业务要求关闭前确保送达，应等
  `send()` 回调成功后再调用 `close()`。
- 回调报 `readyState 2 (CLOSING)` ⇒ 调用顺序问题，消息根本没进发送层。
- `closeTimeout`
  兜底销毁会把优雅关闭退化为场景 4/6 的结果：大快照压缩慢、对端响应慢时，30 秒窗口内握手不完就会触发，现场可用
  `'close'` 码 1006 + 上述回调错误识别。

## 发布评审结论

### 协商默认行为：客户端与服务端并不对称

- **服务端 `WebSocketServer`：默认不启用压缩**。`perMessageDeflate` 默认
  `false`（`lib/websocket-server.js:76`），此时即使客户端发来offer，服务端也不会实例化扩展、不会
  `accept()`（`lib/websocket-server.js:299-313`），连接全程不压缩。**是否压缩由服务端单方面决定**。
- **客户端 `WebSocket`：默认发起 offer**。`perMessageDeflate` 默认
  `true`（`lib/websocket.js:677`），但只能接受服务端回包的参数（`lib/websocket.js:1018-1026`），不能单方面强制压缩。
- **调用侧无法从 `send()` 感知协商结果**：`compress`
  选项默认true，但扩展未协商成功时被静默改为false（`lib/websocket.js:480-482`）。协商参数（窗口大小、context
  takeover）在 upgrade 时定死，连接期间不可变更。

### 影响资源与排队表现的配置

| 配置                                                  | 默认值                  | 对资源 / 排队的影响                                                                                                                                                                               | 出处                                                                                      |
| ----------------------------------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `concurrencyLimit`                                    | 10                      | 进程级全局 `zlibLimiter` 并发；**只在第一个 `PerMessageDeflate` 实例创建时读取，之后的配置静默失效**。高扇出下所有连接的压缩与解压共享它，队首阻塞会同步拉长每条连接的压缩窗口与 close 帧排队时间 | `lib/permessage-deflate.js:65-71`                                                         |
| `threshold`                                           | 1024                    | **仅在协商了对应方向的 no_context_takeover 时才生效**；默认协商（context takeover 开启）下被忽略，所有消息一律压缩。想让小消息跳过压缩必须同时配置 no_context_takeover                            | `lib/sender.js:371-384`、`lib/permessage-deflate.js:56-57`                                |
| `serverMaxWindowBits` / `clientMaxWindowBits`         | 未设置（zlib 默认 15）  | 每连接 deflate/inflate 流内存随窗口指数增长，是高扇出下的主要内存乘数；流首次使用时创建，`'close'` 时由 `cleanup()` 释放                                                                          | `lib/permessage-deflate.js:404-417`、`130-150`                                            |
| `serverNoContextTakeover` / `clientNoContextTakeover` | false                   | 每条消息 fin 后 reset 流：放弃跨消息字典（压缩率下降），换取无跨消息状态，并解锁 `threshold` 门                                                                                                   | `lib/permessage-deflate.js:454-456`                                                       |
| `zlibDeflateOptions` / `zlibInflateOptions`           | 无                      | 透传 zlib（level、memLevel 等），直接决定单次压缩的 CPU / 内存                                                                                                                                    | `lib/permessage-deflate.js:414-417`                                                       |
| `maxPayload`                                          | 100 MiB（双端默认相同） | 解压侧超限即 RangeError（1009）并触发关闭，是接收方向资源的上限保险                                                                                                                               | `lib/websocket.js:675`、`lib/websocket-server.js:74`、`lib/permessage-deflate.js:485-506` |
| `closeTimeout`                                        | 30 s                    | 优雅关闭的兜底；压缩慢或对端响应慢导致窗口耗尽时关闭退化为 `destroy()`，在途与排队消息按场景 4/6 的方式丢失                                                                                       | `lib/constants.js:10`、`lib/websocket.js:1309-1314`                                       |

### 只能说明本机进度、不能代表交付完成的信号

- `send()` 不抛错地返回：仅通过 `readyState`
  检查（边界 A），消息可能还在 zlib 线程池。
- `send()` 回调成功：边界 B，已写入本地 socket。
- `bufferedAmount`：本地账本，且会被被拒收的消息虚增（`lib/websocket.js:1148`），连“待发出”都不准确。
- `'close'` 事件及事件码：只描述握手结局；1006 不区分 `terminate()`、断网与
  `closeTimeout` 兜底。
- 台账与监控可观察的指标（仍全部属于本机进度）：按回调错误消息分类计数（两类错误含义见“结论速查”）；`send()`
  → 回调的时延分布（约等于压缩 + 全局限流排队窗口）；`close()` → `'close'`
  的时延（逼近 `closeTimeout` 是降级前兆）；1006 占比。

### 最终建议：高扇出是否继续启用压缩

按瓶颈决策，不建议一刀切：

- **带宽是瓶颈、快照大且可压缩**：继续启用，但建议同时——
  1. 协商双方向 `no_context_takeover` 并设置 `threshold`，让小消息跳过压缩；
  2. 用 `serverMaxWindowBits` 把每连接内存压到可接受水位；
  3. 按 CPU 核数评估 `concurrencyLimit`，并在进程中最先创建 `PerMessageDeflate`
     的位置设置（首实例生效陷阱）；
  4. 监控 close 握手时延，逼近 `closeTimeout` 时告警——这是高峰期消息丢失的前兆。
- **CPU 是瓶颈，或关闭时延 SLO 严格**：服务端不启用压缩（默认即如此）。客户端的默认 offer 不会造成压缩，协商结果就是“不压缩”，协议语义不变，同时消除了本文档全部“压缩窗口”类风险（关闭时 close 帧立即发出）。
- **无论是否启用**：台账以 `send()`
  回调为唯一记账时点；“对端已送达”只能由应用层确认帧产生。本调查不提供、评审也不应要求用库层信号伪造该语义。

### 剩余风险

1. 回调成功之后连接再 1006：消息是否到达对端不可判定（边界 B 之后没有任何信号）。
2. `closeTimeout` 兜底、对端 `terminate()`
   与网络故障在观察侧不可区分（同为 1006 + 同类回调错误）。
3. 全局 `zlibLimiter`
   的队首阻塞在高扇出高峰期同时拉长压缩窗口与 close 帧时延，正是场景 4/6 的温床。
4. 配置陷阱：`threshold` 在无 `no_context_takeover`
   时静默失效；`concurrencyLimit` 仅首实例生效。
5. 排队中的消息没有独立状态，队首压缩失败会整队牵连（`callCallbacks()`）。
6. 本调查的观察基于本仓库双端实现对 localhost 的复现；与其他实现（如浏览器客户端）交互时，“对端先关仍收到在途消息”依赖对端在关闭期间继续读取，未在本次复现范围内。

### 证据索引

| 结论                                          | 源码依据                                                        | 本地证据（场景）                     |
| --------------------------------------------- | --------------------------------------------------------------- | ------------------------------------ |
| 优雅关闭下在途与排队消息先于 close 帧发出     | `lib/sender.js:224-225`、`536-543`                              | 1、5、7                              |
| `close()` 后 `send()` 被拒绝、不进入发送层    | `lib/websocket.js:467-469`、`1138-1159`                         | 2                                    |
| socket 销毁时在途与排队消息整队丢失、回调报错 | `lib/sender.js:514-521`、`585-594`                              | 4、6                                 |
| 发起关闭的一方仍收到在途消息                  | `lib/websocket.js:1168-1182`（conclude 前 `Receiver` 持续解析） | 3、7                                 |
| 回调成功不代表对端已处理                      | `lib/sender.js:563-572`（write 回调语义）                       | 8                                    |
| 异常关闭恒为 1006                             | `lib/websocket.js:58`、`1168-1173`                              | 4、6                                 |
| 服务端默认不压缩、客户端默认 offer            | `lib/websocket-server.js:76`、`lib/websocket.js:677`            | 全部场景均需显式开启服务端压缩才成立 |
| `threshold` 需 no_context_takeover 才生效     | `lib/sender.js:371-384`                                         | ——（代码审查结论，未单独复现）       |
| `concurrencyLimit` 全局且首实例生效           | `lib/permessage-deflate.js:65-71`                               | ——（代码审查结论，未单独复现）       |

补充精度：对端 conclude 之后会移除 socket 的 `'data'`
监听（`lib/websocket.js:1177`），在 close帧之后到达的数据帧会被忽略——因此“先于 close 帧发出”是精确条件，而非保守说法。

### 一致性复查记录

- 场景编号与 `test/close-during-compression.test.js`
  的执行顺序一一对应（1–8）。复查中发现文档场景列表曾被回退为 5 项、与正文引用（场景 5/6/7/8）矛盾，已修正回 8 项。
- “三种关闭时序的对比”与“投递台账口径”两张表的每一行均有场景断言支撑（见证据索引）。
- 协商默认值、`threshold` 生效条件、`concurrencyLimit`
  生效时机等代码审查结论与复现场景不冲突：场景全部显式启用服务端压缩并使用
  `threshold: 0`；默认协商（context takeover 开启）下 `threshold`
  本就不生效，写 0 仅为显式声明意图，不影响任何断言。
- 除上述已修正项外，未发现实验观察与源码解释相互矛盾的残留项。
