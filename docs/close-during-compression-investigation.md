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
5. **回调成功 ≠ 对端已处理**：对端 `pause()` 期间，本端 `send()`
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
