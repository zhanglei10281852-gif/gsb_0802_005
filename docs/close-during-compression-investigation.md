# 压缩快照与优雅关闭相遇时的投递语义调查

## 背景与结论摘要

行情网关用本仓库（`ws`，v8.21.1）管理长连接。高峰期的典型场景是：一条**压缩后的大快照**刚通过
`ws.send(data, cb)` 发出，应用几乎立刻调用 `ws.close()` 发起优雅关闭。现场反馈"很难判断这笔消息
到底走到了哪里"。

本调查**不修改 `lib/`、公开 API 或协议语义**，只把现有机制说清楚，供接入方做交付判断。核心结论：

- 库内部对"消息进展"只暴露**两个可观测边界**，二者都**不等于**对端真正收到消息：
  - **边界 A —— 已被发送层接收**：`ws.send()` 在 `readyState === OPEN` 时正常返回。此刻数据已交给
    `Sender`（要么正在压缩，要么排在队列里），计入 `bufferedAmount`，但**尚未成帧、尚未压缩完成、
    尚未写 socket**。
  - **边界 B —— 已写入本地 socket**：该条 `send()` 的**回调以无错误方式触发**。此刻压缩帧已交给
    `socket.write()`，进入本机 OS 发送缓冲区。这是库能给出的**最强保证**，但仍**不是对端收到**。
- 压缩是**异步**的（走 zlib 线程池），因此边界 A 和边界 B 之间**必然存在一个时间窗口**。优雅关闭
  正是在这个窗口内被排队处理的，这就是"说不清走到哪里"的根因。
- 只要走的是**优雅关闭**（`ws.close()`，socket 用 `end()` 而非 `destroy()`），排在压缩快照后面的
  关闭帧会**保持顺序**，快照会被正常压缩、写出并投递给对端。若是**强制拆链**（对端消失、
  `terminate()`、socket 出错被 `destroy()`），则回调会带错误，明确告知"这笔从未写入 socket"。

复现见 [`test/close-during-compression.test.js`](../test/close-during-compression.test.js)。

---

## 一、压缩协商：`compress` 标志从哪来

只有握手阶段协商成功启用了 `permessage-deflate`，快照才会被压缩。

- 客户端在 [initAsClient()](../lib/websocket.js#L769-L778) 里根据 `perMessageDeflate` 选项构造
  `PerMessageDeflate` 实例，并把 `offer()`（见
  [permessage-deflate.js](../lib/permessage-deflate.js#L87-L106)）写进 `Sec-WebSocket-Extensions`
  请求头。
- 服务端在 [websocket-server.js](../lib/websocket-server.js#L295-L313) 里 `accept()` 该 offer，
  把协商结果回写响应头，并把扩展实例挂到 `ws._extensions`。
- 客户端在 [upgrade 回调](../lib/websocket.js#L985-L1027) 里校验并 `accept()` 服务端应答，最终
  `websocket._extensions[PerMessageDeflate.extensionName] = perMessageDeflate`。

只有该扩展存在时，`WebSocket#send()` 才允许压缩：在
[send()](../lib/websocket.js#L472-L484) 里，若未协商扩展则强制 `opts.compress = false`。

进入 `Sender#send()` 后，是否真正设置 RSV1（压缩位）还要看阈值逻辑
（[sender.js](../lib/sender.js#L371-L390)）：当协商了 `*_no_context_takeover` 时，小于
`threshold`（默认 1024 字节）的消息不会被压缩。大快照通常远超阈值，因此**会**走压缩路径——这正是本
调查关注的场景。

---

## 二、发送队列与状态机：快照如何被异步处理

`Sender` 是一个带状态机的串行队列，状态见 [sender.js](../lib/sender.js#L22-L24)：
`DEFAULT` / `DEFLATING` / `GET_BLOB_DATA`。

一条需要压缩的数据消息流程如下：

1. [Sender#send()](../lib/sender.js#L351-L414)：计算 `opcode`/`rsv1`/`byteLength`。若当前
   `_state === DEFAULT` 则直接 `dispatch()`；否则 `enqueue()` 排队（此时
   `_bufferedBytes += byteLength`）。
2. [Sender#dispatch()](../lib/sender.js#L503-L529)：对压缩消息，把 `_bufferedBytes` 记账、状态置为
   `DEFLATING`，然后调用 `perMessageDeflate.compress(data, fin, cb)`。**压缩是异步的**——
   [compress()](../lib/permessage-deflate.js#L322-L329) 经 `zlibLimiter`（并发上限默认 10，见
   [permessage-deflate.js](../lib/permessage-deflate.js#L65-L71)）排队，再进 zlib 线程池做
   `deflateRaw` + `Z_SYNC_FLUSH`（[_compress()](../lib/permessage-deflate.js#L404-L459)）。
3. 压缩回调里（[sender.js](../lib/sender.js#L513-L528)）：**先检查 `this._socket.destroyed`**。
   - 若 socket 已被销毁 → 调 `callCallbacks(this, err, cb)`，错误信息为
     `'The socket was closed while data was being compressed'`，**不写 socket**。
   - 否则 → 归还 `_bufferedBytes`、状态复位 `DEFAULT`、`sendFrame()` 写 socket、`dequeue()` 处理
     队列里后续项。
4. [Sender#sendFrame()](../lib/sender.js#L563-L572)：真正的 `socket.cork()/write()/uncork()`。
   传入的 `cb` 就是 `WebSocket#send` 的用户回调——**它由 `socket.write()` 触发**。

关键点：**在压缩期间到达的任何 `send()/close()/ping()/pong()` 都会因为 `_state !== DEFAULT` 而被
`enqueue()`**（见 [close()](../lib/sender.js#L224-L228)、[send()](../lib/sender.js#L409-L413)）。
队列严格 FIFO，由 [dequeue()](../lib/sender.js#L536-L543) 串行推进。这保证了**快照在关闭帧之前被写出**。

---

## 三、回调语义：用户回调到底承诺了什么

`WebSocket#send(data, cb)` 的 `cb` 一路透传到 `Sender`，最终有两种归宿：

- **成功路径**：作为 `socket.write(payload, cb)` 的写回调触发
  （[sendFrame()](../lib/sender.js#L563-L572)）。语义 = **"压缩帧已被本地 socket 接收进发送缓冲区"**
  ＝ **边界 B**。这**不**代表对端收到，也不代表已离开本机网卡。
- **失败路径**：`callCallbacks(sender, err, cb)`（[sender.js](../lib/sender.js#L585-L594)）带错误调用。
  触发条件包括：压缩完成时发现 `socket.destroyed`（[dispatch 回调](../lib/sender.js#L514-L521)）、
  blob 读取时 socket 已关、或 deflate 流被 `cleanup()` 关闭
  （[permessage-deflate.js](../lib/permessage-deflate.js#L130-L150)）。语义 = **"这笔数据从未写入
  socket"**。注意 `callCallbacks` 还会**把队列里所有后续待发项的回调一并以同一错误回调**，因此关闭
  时一批消息可能集体收到失败回调。

若 `send()` 在 `readyState !== OPEN` 时被调用，则走
[sendAfterClose()](../lib/websocket.js#L1138-L1159)：数据不入队，`bufferedAmount` 象征性增加，回调
在下一 tick 收到 `WebSocket is not open: readyState ...` 错误。

> 交付判断要点：**没有回调、或回调未触发**，只能推断到边界 A；**回调无错触发**才到边界 B；
> **回调带错**说明未达边界 B。任何一种都**不能**推断对端已收到——那需要应用层 ACK。

---

## 四、关闭握手：优雅关闭如何与压缩交错

[WebSocket#close()](../lib/websocket.js#L302-L340) 的关键行为：

- 若已 `CLOSED` 直接返回；若 `CONNECTING` 走 `abortHandshake`。
- 若已 `CLOSING`：仅当 `_closeFrameSent && (_closeFrameReceived || receiver errorEmitted)` 时
  `socket.end()`。
- 否则置 `readyState = CLOSING`，调用 `this._sender.close(code, data, mask, cb)` **发送关闭帧**，
  并 `setCloseTimer()` 启动兜底定时器（默认 `CLOSE_TIMEOUT = 30000ms`，见
  [constants.js](../lib/constants.js#L10)，超时后 `socket.destroy()`）。

关键在于 [Sender#close()](../lib/sender.js#L224-L228)：如果此刻 `_state !== DEFAULT`（即快照正在
压缩），**关闭帧被 `enqueue()` 到快照之后**，而不是立即发出。于是顺序被保证：

```
send(snapshot)  -> dispatch -> DEFLATING（异步压缩中）
close(1000)     -> Sender.close 发现 _state=DEFLATING -> 关闭帧入队
压缩完成回调    -> sendFrame(快照)  -> dequeue()
                -> 关闭帧出队 -> sendFrame(关闭帧, cb)
close 回调置 _closeFrameSent=true；若对端关闭帧已到则 socket.end()
```

只有当关闭帧真正写出（`Sender#close` 的 `cb` 无错触发）后，`_closeFrameSent` 才置真
（[websocket.js](../lib/websocket.js#L322-L337)）。随后本端根据是否已收到对端关闭帧决定
`socket.end()`。因为优雅关闭走的是 `end()` 而非 `destroy()`，**socket 不会在压缩途中被销毁**，所以
上面 dispatch 回调里的 `socket.destroyed` 检查为假，快照能顺利写出并投递。

对比路径：

- **对端先消失 / socket 出错**：`socketOnError`/`socketOnClose`
  （[websocket.js](../lib/websocket.js#L1321-L1365)、[L1397-L1407](../lib/websocket.js#L1397-L1407)）
  会 `destroy()` socket；此时压缩回调命中 `socket.destroyed`，快照回调带
  `'The socket was closed while data was being compressed'` 错误——**未达边界 B**。
- **`terminate()`**：[websocket.js](../lib/websocket.js#L492-L504) 直接 `socket.destroy()`，效果同上，
  队列中所有待发项回调集体报错。

---

## 五、三个边界与"对端真正收到"之间还隔着什么

```
应用调用 ws.send(snapshot, cb)
      │
      ▼
[边界 A] send() 返回，readyState=OPEN
      │   数据在 Sender：DEFLATING 或队列中，计入 bufferedAmount
      │   —— 还未成帧、未压缩完、未写 socket
      ▼   (zlib 线程池异步压缩；期间到达的 close 被排到快照之后)
[边界 B] send 回调无错触发
      │   压缩帧已交给 socket.write()，进入本机 OS 发送缓冲区
      │   —— 库能给的最强保证到此为止
      ▼
   ── 库不可见的部分 ──
      • OS TCP 发送缓冲 → 网卡 → 网络（可能重传/排队）
      • 对端 OS 接收缓冲 → 对端应用 read
      • 对端 ws 解帧 + 解压（permessage-deflate 也异步）
      • 对端触发 'message' 事件
      ▼
对端"真正收到并解出快照"
```

边界 B 与"对端收到"之间隔着：本机内核发送缓冲、物理网络、对端内核接收缓冲、对端解帧与**异步解压**、
对端事件派发。这些都在库之外，`ws` 无从观测。**唯一可靠的"对端已消费"信号是应用层 ACK。**

一个容易误解的量：`bufferedAmount`
（[websocket.js](../lib/websocket.js#L120-L124)）＝ `socket._writableState.length + sender._bufferedBytes`。
它=0 只说明"库和本机 socket 缓冲已排空"（约等于全部到达边界 B），**同样不代表对端收到**。

---

## 六、给接入方的交付判断清单

1. **想知道"已交给库"**：`send()` 未抛异常且当时 `readyState === OPEN` → 到达**边界 A**。仅代表数据
   被 `Sender` 接管，可能还在压缩/排队。
2. **想知道"已写入本机 socket"**：**必须传 `send()` 回调**并检查：
   - 回调无错 → **边界 B**（帧已入本机发送缓冲，仍非对端收到）。
   - 回调 `err.message === 'The socket was closed while data was being compressed'` → **未达边界 B**，
     这笔从未写出（连接在压缩途中被销毁）。
   - 回调 `WebSocket is not open: readyState ...` → 调用时已不在 OPEN，数据未入队。
3. **优雅关闭要保住这笔快照**：先 `ws.send(snapshot, cb)` 再 `ws.close()`。关闭帧会排在快照之后，
   快照会被正常压缩并投递；**不要用 `terminate()`**，它会 `destroy()` socket 导致在途快照丢失。
4. **确认对端真的收到**：库层面无法保证，需应用层 ACK / 序号确认。`bufferedAmount === 0`、`'close'`
   事件、边界 B 回调都**不能**替代它。

---

## 七、复现场景

[`test/close-during-compression.test.js`](../test/close-during-compression.test.js) 用仓库现有测试工具
（`mocha` + `assert` + 真实的 `WebSocket.Server` / `WebSocket` 客户端，不新增依赖）复现四种结局，对应
第八节的时序对比：

1. **本机优雅关闭 + 压缩快照（时序①）**：客户端 `send(大快照)` 后立即 `close(1000)`。断言：
   - 边界 A：`send()` 返回时仍 `OPEN`、`bufferedAmount > 0`、回调尚未触发；
   - 边界 B：`'close'` 前回调无错触发，且 socket 未被销毁；
   - 对端收到：服务端拼接收到的分片，长度与内容与原快照完全一致，关闭码为 1000。
2. **压缩途中被拆链**：服务端在握手后立即 `_socket.end()`，客户端仍在压缩快照。断言 `send()` 回调
   带 `'The socket was closed while data was being compressed'` 错误、`readyState === CLOSING`——即
   **未达边界 B**。
3. **本机 `terminate()`（时序②）**：客户端 `send` 两条快照（第一条 DEFLATING、第二条排队）后
   `terminate()`。断言**两个**回调都带 `'The socket was closed while data was being compressed'` 错误
   ——在途的与排队的一并失败，验证 `callCallbacks` 遍历整条队列的行为。
4. **对端优雅关闭（时序③）**：客户端只 `send(大快照)`、**从不**自己 `close()`；服务端在压缩未结束时
   `close(1001)`。断言快照回调仍无错触发（边界 B）、socket 未被销毁，且对端字节级完整收到、关闭码
   1001——证明"对端发起的优雅关闭"与"本机优雅关闭"对在途快照的效果一致。

运行：

```
npx mocha --throw-deprecation test/close-during-compression.test.js
```

四个用例均通过，覆盖了压缩消息与不同关闭时序相遇时能观察到的结果（下一节详列）。

---

## 八、三种关闭时序对比（供投递台账映射）

接入团队要把 `send()` 回调接到投递台账，需要知道**哪些状态能记为"本机已接收"（边界 B），哪些
绝不能写成"对端已送达"**。下面按"一条压缩快照正在 zlib 线程池里压缩(`_state = DEFLATING`)"这个
统一起点，比较三种关闭时序。判定的**唯一分叉点**都在
[dispatch 的压缩回调](../lib/sender.js#L513-L528)：压缩完成时 `this._socket.destroyed` 是真还是假。

| 时序 | 触发路径 | 压缩回调看到 `socket.destroyed` | 正在压缩的快照 | 排队中的后续发送 | send 回调 | `'close'` 事件 |
|---|---|---|---|---|---|---|
| ① 本机主动**优雅**关闭 `close()` | [WebSocket#close](../lib/websocket.js#L302-L340) → [Sender#close 入队](../lib/sender.js#L224-L228) → 顺序 `end()` | **false**（`end()` 不销毁） | **压缩→写出→对端收到** | 关闭帧排在快照之后，依次写出 | **无错**（边界 B） | 正常，`code` 为应用传入值 |
| ② 本机**强制**终止 `terminate()` | [WebSocket#terminate](../lib/websocket.js#L492-L504) → `socket.destroy()` | **true** | **从未写出** | [callCallbacks 遍历队列](../lib/sender.js#L585-L594)集体报错 | **带错** `The socket was closed while data was being compressed` | 触发，`code=1006` |
| ③ **对端**在压缩未结束时开始关闭 | 收到对端关闭帧 → [receiverOnConclude](../lib/websocket.js#L1168-L1182) → 本机 `close()` → 顺序 `end()` | **false** | **压缩→写出→对端收到** | 关闭帧排在快照之后，依次写出 | **无错**（边界 B） | 触发，`code` 为对端所发（如 1001） |

要点解读：

- **①③ 本质相同**：无论关闭由本机还是对端发起，只要走的是**优雅**路径（`socket.end()`），在途快照
  都不会被中断。区别仅在 `close` 事件的 `code` 来源和谁先发关闭帧。③ 中即使应用**根本没调用
  `close()`**，快照回调仍会无错触发到达边界 B（见复现用例 4）。
- **② 是唯一会丢在途数据的常见路径**：`terminate()` 同步 `destroy()` socket，导致压缩回调命中
  `socket.destroyed`。此时**不仅在压缩的这条**，**排在它后面的整条队列**的回调都会以同一错误信息被
  调用（见复现用例 3）。同源路径还有：对端 RST / socket `'error'`（[socketOnError](../lib/websocket.js#L1397-L1407)）、
  连接异常 `'close'`（[socketOnClose](../lib/websocket.js#L1321-L1365)）。
- **`bufferedAmount` 在三种时序下的读数不能作为投递依据**：它只反映库内队列 + 本机 socket 写缓冲，
  归零至多约等于"全部到达边界 B"，与对端是否收到无关。

> 为何现有实现是这样：库把"是否还能安全写出"归结为一个**单一、可判定**的信号——
> `socket.destroyed`。优雅关闭用 `end()` 保留写方向直到缓冲排空，所以在途压缩能完成；强制终止用
> `destroy()` 立即放弃写方向，所以在途压缩被判定为失败。这是**有意的顺序保证 + 快速失败**取舍，而非
> 缺陷，也因此**回调成功只可能意味着到达边界 B，永远不表示对端已收到**。

---

## 九、投递台账映射规则（必须遵守）

把 `send(data, cb)` 的回调接入台账时，按下表记账。**"对端已送达"这一状态，任何库内信号都不可推出**，
只能由应用层 ACK 置位：

| 观测到的状态 | 可记为 | 绝不可记为 |
|---|---|---|
| `send()` 未抛异常且当时 `readyState === OPEN`，回调尚未触发 | `本机已入队`（边界 A） | 本机已发送 / 对端已送达 |
| 回调触发且 `err == null` | `本机已发送`（边界 B，帧入本机 OS 发送缓冲） | **对端已送达** |
| 回调触发且 `err.message === 'The socket was closed while data was being compressed'` | `发送失败-未写出`（时序②及同源路径） | 本机已发送 / 对端已送达 |
| 回调触发且 `err.message` 以 `WebSocket is not open: readyState` 开头 | `发送失败-连接非 OPEN`（数据未入队） | 本机已入队 / 已发送 / 对端已送达 |
| `'close'` 事件（任意 `code`，含 1000/1001/1006） | `连接已关闭` | 在途/历史消息对端已送达 |
| `bufferedAmount === 0` | `本机缓冲已排空`（约等于均达边界 B） | 对端已送达 |

**红线**：边界 B（回调 `err == null`）只证明"本机已把压缩帧交给 OS 发送缓冲"。它与"对端已送达"之间
仍隔着本机内核缓冲、物理网络、对端内核缓冲、对端解帧与**异步解压**、对端事件派发（见第五节图）。
台账里的"对端已送达"列**必须**由对端应用回传的 ACK/序号确认驱动，不得由 `ws` 的任何回调或事件推断。
