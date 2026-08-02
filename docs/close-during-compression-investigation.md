# 压缩期间优雅关闭：消息边界调查

> 调查对象：`ws@8.21.1`（本仓库
> `lib/`）问题场景：高峰期压缩后的大快照刚发出，随即触发优雅关闭，现场难以判断消息走到了哪一步。约束：不修改
> `lib/`、公开 API 或协议语义，不新增依赖。

---

## 1. 结论速览

当一笔压缩消息与优雅关闭（`ws.close()`）在同一连接上相遇时，**库的发送队列是严格 FIFO 的**：先调用的
`ws.send()`
一定先于后入队的 close 帧被写入本地 socket。调用方可以通过两个明确的边界来判断消息进度，但这两个边界都**不能等同于对端已经收到消息**。

| 边界                     | 触发时机                                            | 可观察信号                                                                                    | 能保证什么                                                                                       | 不能保证什么                                                                                         |
| ------------------------ | --------------------------------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| **A. 已被发送层接收**    | `ws.send(data, opts, cb)` 同步返回，未抛异常        | 调用正常返回；`ws.bufferedAmount > 0`（若压缩未完成）；`ws._sender._state === 1`（DEFLATING） | 消息已被 WebSocket 层接纳，将按 FIFO 顺序处理；若后续 `close()` 被调用，close 帧会排在该消息之后 | 消息尚未压缩、尚未写入 socket；若此时 socket 被 `terminate()` 或网络中断，回调收到错误，消息可能丢失 |
| **B. 已写入本地 socket** | `send()` 的 `cb(err)` 以 `err === undefined` 被调用 | 回调无错执行；此时 `ws._sender._state === 0`（DEFAULT）                                       | 帧头 + 压缩后的 payload 已经通过 `socket.write()` 交给操作系统 TCP 发送缓冲区                    | 数据可能还在本机内核缓冲区中，尚未经过网络传输；对端未必已 ACK；对端应用层未必已解析完               |
| **C. 对端实际收到**      | 对端 `WebSocket` 触发 `'message'` 事件              | 对端事件回调执行                                                                              | 对端的 Receiver 已完成帧解析、解压（若启用）、UTF-8 校验，并将完整消息交给应用层                 | 这是唯一的"对端确实拿到了"的信号，但只能由对端观察到                                                 |

**从 B 到 C 之间还隔着：**

1. 本机 TCP 栈将数据切分为 TCP 段并交给网卡发送；
2. 网络传输（可能丢包、重传、乱序）；
3. 对端 TCP 栈接收、重组、ACK；
4. 对端 Node.js 从 socket 读取字节流；
5. 对端 `Receiver` 解析 WebSocket 帧头、payload；
6. 若 RSV1 置位，对端 `PerMessageDeflate.decompress()` 异步解压；
7. 对端 `Receiver` 校验 payload 长度、UTF-8（文本帧），emit `'message'`。

其中 5-7 步在对端也是异步的；若对端在接收过程中关闭连接，数据可能停留在对端内核缓冲区或 Receiver 内部队列中而不触发
`'message'`。

---

## 2. 代码机制串联

### 2.1 压缩协商

压缩扩展（permessage-deflate）在 HTTP Upgrade 握手阶段协商完成。

**客户端发起协商**（[websocket.js:769-778](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L769-L778)）：

```js
if (opts.perMessageDeflate) {
  perMessageDeflate = new PerMessageDeflate({
    ...opts.perMessageDeflate,
    isServer: false,
    maxPayload: opts.maxPayload
  });
  opts.headers['Sec-WebSocket-Extensions'] = format({
    [PerMessageDeflate.extensionName]: perMessageDeflate.offer()
  });
}
```

**服务端接受协商**（[websocket-server.js:298-321](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket-server.js#L298-L321)）：解析
`Sec-WebSocket-Extensions` 头，调用
`perMessageDeflate.accept(offers[...])`，将接受的参数通过响应头返回（[websocket-server.js:414-421](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket-server.js#L414-L421)）。

**客户端确认响应**（[websocket.js:985-1027](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L985-L1027)）：在
`upgrade` 事件中校验服务端返回的扩展参数，调用
`perMessageDeflate.accept(...)`，最终将扩展实例挂载到 `websocket._extensions`。

关键参数：

- `server_no_context_takeover` /
  `client_no_context_takeover`：每条消息后重置压缩字典；
- `threshold`（默认 1024 字节）：当启用 no context
  takeover 时，小于此阈值的消息不压缩（见
  [sender.js:374-383](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L374-L383)）；
- `concurrencyLimit`（默认 10）：全局 zlib 并发限制，通过 `Limiter`
  控制（[permessage-deflate.js:65-71](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L65-L71)），防止 zlib 线程池内存碎片化。

### 2.2 发送状态机与队列

`Sender`
有三种状态（[sender.js:22-24](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L22-L24)）：

```
DEFAULT(0) → DEFLATING(1) → (压缩回调) → DEFAULT(0)
DEFAULT(0) → GET_BLOB_DATA(2) → (Blob 读取回调) → DEFAULT(0)
```

发送一笔压缩消息的完整路径：

1. **`WebSocket.send()`**（[websocket.js:455-485](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L455-L485)）：
   - 若 `readyState !== OPEN`，走
     `sendAfterClose()`，回调在下个 tick 收到错误，数据不入队。
   - 若扩展未协商，强制 `compress: false`。
   - 调用 `this._sender.send(data, opts, cb)`。

2. **`Sender.send()`**（[sender.js:351-414](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L351-L414)）：
   - 处理分片状态、压缩阈值判断、RSV1 位设置。
   - **若 `_state !== DEFAULT`，调用 `this.enqueue(...)` 将操作推入 `_queue`
     数组**（[sender.js:409-410](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L409-L410)）。
   - 否则直接调用 `this.dispatch(...)`。

3. **`Sender.dispatch()`
   → 压缩路径**（[sender.js:503-529](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L503-L529)）：
   - `this._bufferedBytes += 原始长度`
   - `this._state = DEFLATING`
   - 调用 `perMessageDeflate.compress(data, fin, callback)`

4. **`PerMessageDeflate.compress()`**（[permessage-deflate.js:322-329](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L322-L329)）：
   - 通过全局 `zlibLimiter` 排队，限制并发。
   - 实际工作在 `_compress()` 中，使用 `zlib.createDeflateRaw()`，调用
     `deflate.flush(zlib.Z_SYNC_FLUSH, ...)`
     异步完成（[permessage-deflate.js:404-460](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L404-L460)）。

5. **压缩回调**（[sender.js:513-528](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L513-L528)）：
   - **若 `this._socket.destroyed` 为 true**：回调收到
     `"The socket was closed while data was being compressed"`，且
     `callCallbacks()` 会将 `_queue`
     中所有待发操作的回调也以同一错误触发（[sender.js:585-594](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L585-L594)）。**不会调用
     `dequeue()`**，队列中后续操作全部失败。
   - 否则：`_state = DEFAULT`，调用
     `this.sendFrame(Sender.frame(buf, options), cb)` 写入 socket，然后调用
     `this.dequeue()` 处理队列。

6. **`Sender.sendFrame()`**（[sender.js:563-572](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L563-L572)）：
   - 使用 `socket.cork()` / `socket.uncork()` 合并帧头与 payload。
   - `socket.write(list[1], cb)` —— **这里的 `cb` 就是用户传入 `ws.send()`
     的回调**，它由 Node.js socket 在数据写入内核发送缓冲区后调用。

7. **`Sender.dequeue()`**（[sender.js:536-543](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L536-L543)）：
   - 当 `_state === DEFAULT` 且队列非空时，循环取出队首操作并执行。
   - 每个操作从队列取出时，`_bufferedBytes` 减去其原始长度。

### 2.3 `bufferedAmount` 的含义

[websocket.js:120-124](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L120-L124)：

```js
get bufferedAmount() {
  if (!this._socket) return this._bufferedAmount;
  return this._socket._writableState.length + this._sender._bufferedBytes;
}
```

`bufferedAmount` = 内核 socket 写入队列中尚未 flush 的字节数 +
Sender 内部正在压缩/排队的原始数据字节数。它在边界 A 之后、边界 B 之前为正值，在边界 B 之后取决于 TCP 发送缓冲区是否排空。**它不包含对端已接收但未 ACK 的数据，也不反映对端应用层的处理进度。**

### 2.4 优雅关闭握手

`WebSocket.close()`（[websocket.js:302-340](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L302-L340)）：

1. 若 `CLOSED` → 直接返回；若 `CONNECTING` → 中止握手。
2. 若已处于 `CLOSING`：当
   `_closeFrameSent && (_closeFrameReceived || receiver出错)` 时调用
   `socket.end()`，然后返回。
3. 若 `OPEN`：
   - 立即将 `readyState` 设为 `CLOSING`。
   - 调用 `this._sender.close(code, data, mask, callback)` 发送 close 帧。
   - 启动关闭定时器（默认 30s，[constants.js:10](file:///e:/newGsb/questions/GSB-005/Steve/lib/constants.js#L10)），超时后
     `socket.destroy()`。

**关键点：`Sender.close()`
同样遵守发送队列**（[sender.js:224-228](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L224-L228)）：

```js
if (this._state !== DEFAULT) {
  this.enqueue([this.dispatch, buf, false, options, cb]);
} else {
  this.sendFrame(Sender.frame(buf, options), cb);
}
```

close 帧使用
`rsv1: false`、`opcode: 0x08`，不压缩。当 Sender 处于 DEFLATING 状态时，close 帧被推入
`_queue`，等当前压缩消息写入 socket 后才会通过 `dequeue()` 发出。

close 帧写入 socket 的回调触发后（[websocket.js:327-336](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L327-L336)）：

- `_closeFrameSent = true`
- 若已经收到对端的 close 帧（`_closeFrameReceived`）或 Receiver 出错，调用
  `socket.end()`（半关闭写端）。

**接收对端 close 帧**（[websocket.js:1168-1182](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1168-L1182)）：

- 设置 `_closeFrameReceived = true`
- 移除 socket 的 `'data'` 监听器（**停止读取新数据**），resume socket
- 调用 `this.close(code, reason)` 回应 close 帧

**socket 关闭**（[websocket.js:1321-1365](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1321-L1365)）：

- 清除关闭定时器
- 若 socket 接收缓冲区还有残留数据，尝试写入 Receiver
- 调用 `receiver.end()`
- 当 Receiver finish 或 error 后，调用 `emitClose()`

**`emitClose()`**（[websocket.js:266-280](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L266-L280)）：

- 调用 `perMessageDeflate.cleanup()`
- `cleanup()` 关闭 inflate/deflate 流；若 deflate 流正在处理数据，以
  `"The deflate stream was closed while data was being processed"`
  错误调用其挂起的回调（[permessage-deflate.js:130-150](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L130-L150)）。

### 2.5 `terminate()` 与 `close()` 的区别

`terminate()`（[websocket.js:492-504](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L492-L504)）直接调用
`socket.destroy()`，**不经过关闭握手，不等待发送队列排空**。若此时正在压缩：

1. socket 被销毁，`_socket.destroyed = true`。
2. 压缩完成回调检查到 `socket.destroyed`，以
   `"The socket was closed while data was being compressed"` 失败当前操作。
3. `callCallbacks()` 遍历 `_queue`，将所有排队操作的回调以同一错误失败。
4. 不会调用 `dequeue()`，队列中后续数据全部丢弃。

`close()` 则不同：它将 close 帧排入队列，等待所有在它之前 `send()`
的消息（包括正在压缩的）先写入 socket，然后才发送 close 帧。

---

## 3. 场景分析：大快照 send 后立即 close

以"服务端向客户端推送一笔压缩大快照，紧接着调用 `ws.close()`"为例：

```
时间线 →

服务端应用层
  │
  ├─ ws.send(snapshot, {compress:true}, cb)
  │     └─ Sender.state = DEFLATING
  │        _bufferedBytes += snapshot.length
  │        （边界 A：消息已被发送层接收）
  │
  ├─ ws.close()
  │     └─ readyState = CLOSING
  │        Sender.close() → state 仍是 DEFLATING
  │        close 帧被 enqueue 到 _queue 末尾
  │
  │   ... zlib 异步压缩中 ...
  │
  ├─ 压缩完成回调
  │     ├─ socket 未销毁 → state = DEFAULT
  │     ├─ sendFrame(压缩帧, cb)  ← socket.write()
  │     │     （数据进入内核 TCP 发送缓冲区）
  │     └─ dequeue() → 取出 close 帧 → sendFrame(close帧)
  │
  ├─ socket.write() 回调（数据帧）
  │     └─ 用户 cb() 无错执行
  │        （边界 B：数据已写入本地 socket）
  │
  ├─ socket.write() 回调（close 帧）
  │     └─ _closeFrameSent = true
  │        若已收到对端 close → socket.end()
  │
  └─ 关闭定时器启动（30s）
        若对端不回应 → 30s 后 socket.destroy()
```

**复现场景验证：**

测试文件
[test/close-during-compression.test.js](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js)
覆盖了以下场景，均可通过 `npx mocha test/close-during-compression.test.js`
运行：

**基础场景（8 个）：**

| 场景 | 验证内容                                                       |
| ---- | -------------------------------------------------------------- |
| A    | 大快照 `send()` 后立即 `close()`，对端能收到完整快照           |
| B    | close 帧在 `sendFrame` 层面确实排在数据帧之后（FIFO）          |
| C    | `terminate()` 在压缩期间调用时，当前及排队消息的回调都收到错误 |
| D    | 多笔压缩消息排队后 `close()`，全部按序送达                     |
| E    | `send` 回调在数据写入本地 socket 时触发，早于对端实际接收      |
| F    | `close()` 之后再调用 `send()` 会被拒绝，而已入队的数据仍能送达 |
| G    | `bufferedAmount` 在压缩/关闭过程中反映发送层+内核缓冲区        |
| H    | 压缩期间收到对端 close 帧，不影响出站压缩数据的发送            |

**三种关闭时序结果矩阵（7 个）：**

| 时序                   | 验证内容                                                                               |
| ---------------------- | -------------------------------------------------------------------------------------- |
| timing 1               | 本地优雅关闭：正在压缩 + 排队中消息全部 cb 成功，FIFO 顺序，close 码 1000              |
| timing 2               | 本地 terminate：正在压缩 + 排队中消息全部 cb 报错，close 码 1006，对端无数据           |
| timing 3               | 对端关闭：出站压缩数据仍刷出，cb 成功，close 码为对端码（1000）                        |
| boundary B + terminate | cb 在边界 B 成功后立即 terminate，cb 无错但 close 码 1006（证明 cb 成功≠送达）         |
| late send              | `close()` 后的 `send()` 被 WebSocket 层拒绝，不入 Sender 队列，cb 收到 readyState 错误 |
| FIFO order             | 4 笔消息排队后 close，回调严格按序执行，close 事件在所有回调之后                       |
| queued ping            | terminate 时排队中的 ping 控制帧也被丢弃，对端收不到                                   |

---

## 4. 给接入方的交付判断建议

### 4.1 如何判断"消息确实发出去了"

- **`ws.send(data, cb)` 的 `cb` 无错执行**
  = 数据已写入本机 TCP 发送缓冲区（边界 B）。这是本端能获得的最强信号。
- 若需要**对端确认收到**，必须在应用层协议中设计 ACK 机制，由对端在 `'message'`
  事件中回发确认。WebSocket 协议层和本库都不提供应用层接收确认。

### 4.2 如何判断"消息可能丢了"

- `send` 回调收到 `Error`（消息为
  `"The socket was closed while data was being compressed"` 或
  `"The deflate stream was closed while data was being processed"`）→ 消息在压缩阶段被中断，**未写入 socket，需要重发**。
- `'close'` 事件触发但 `send` 回调从未被调用 → 检查 `close` 事件的 code：
  - `1006`（异常关闭）：连接在数据传输过程中断开，数据可能丢失。
  - `1000`（正常关闭）：关闭握手完成，但只保证 close 帧交互完成；**不保证所有之前 send 的数据都被对端应用层处理**。
- `'error'` 事件触发 → 底层连接出错，回调可能收到错误，待发数据可能丢失。

### 4.3 优雅关闭时的安全做法

1. **在 `close()` 之前等待关键消息的 `send` 回调**，确保大快照已经到达边界 B。
2. 若需要对端确认，在应用层等待对端的 ACK 消息，然后再调用 `close()`。
3. 不要依赖 `close`
   事件本身来判断消息是否送达——它只表示 WebSocket 关闭握手结束。
4. `ws.close()` 不会丢弃在它之前调用的 `send()` 数据（会排队），但
   `ws.terminate()` 会。高峰期需要快速释放连接时，应明确区分这两者的语义。
5. 关闭定时器默认 30s（`closeTimeout`
   选项可调整）。超时后 socket 会被强制销毁，此时内核发送缓冲区中尚未发出的数据会丢失。

### 4.4 回调语义的注意事项

- `send(data, cb)` 的 `cb` 来自
  `socket.write()`，其语义是"数据已被操作系统接受发送"，**不是"对端已收到"**。
- 在压缩路径上，回调在压缩完成且 `socket.write()`
  接受数据后才被调用；若压缩期间 socket 销毁，回调收到错误。
- `bufferedAmount`
  包含 Sender 内部待压缩数据和内核待发送数据，但不反映网络传输状态。
- 回调的执行顺序与 `send()` 调用顺序一致（FIFO），因为每帧的 `socket.write()`
  按队列顺序执行，而 Node.js 保证同一 stream 的 write 回调按写入顺序触发。

---

## 5. 三种关闭时序的结果矩阵

以下分别追踪一笔连接中同时存在"正在压缩的消息"（M0）和"已排队等待压缩的消息"（M1、M2）时，三种关闭方式各自导致的结果。所有结论均由
[test/close-during-compression.test.js](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js)
中的 `close timing outcome matrix for delivery ledger` 测试组验证。

### 5.1 时序一：本端主动优雅关闭（`ws.close()`）

**触发条件**：M0 正在压缩（`_state === DEFLATING`），M1、M2 已在 `_queue`
中，此时调用 `ws.close(1000)`。

**代码路径**：

1. `WebSocket.close()` 将 `readyState` 设为 `CLOSING`，调用 `Sender.close()`。
2. `Sender.close()` 检查
   `_state !== DEFAULT`（当前是 DEFLATING），将 close 帧作为
   `[this.dispatch, closeBuf, false, opts, cb]` 推入 `_queue` 末尾。
3. M0 压缩完成，回调检查 `socket.destroyed === false` → `state = DEFAULT` →
   `sendFrame(M0)` → `dequeue()`。
4. `dequeue()` 从队列取出 M1，调用 `dispatch(M1)`，M1 开始压缩。
5. M1 压缩完成 → `sendFrame(M1)` → `dequeue()` 取出 M2。
6. M2 压缩完成 → `sendFrame(M2)` → `dequeue()` 取出 close 帧。
7. close 帧的 `dispatch` 因 `compress=false` 直接 `sendFrame(closeFrame)`。
8. close 帧写入 socket 后，回调设置 `_closeFrameSent = true`。

**为何如此设计**：`Sender`
的队列是统一的 FIFO 结构，close 帧不享有特权。这保证了 `close()` 之前所有
`send()`
的数据在 WebSocket 帧层面不会被 close 帧插队。设计意图是让调用方可以"发完最后一笔消息再关闭"而不必手动等待回调。

| 对象           | 回调结果        | 数据是否写入本地 socket | 对端是否收到          | close 事件码 |
| -------------- | --------------- | ----------------------- | --------------------- | ------------ |
| M0（正在压缩） | `cb(null)` 成功 | 是                      | 是（在 close 帧之前） | —            |
| M1（排队中）   | `cb(null)` 成功 | 是                      | 是                    | —            |
| M2（排队中）   | `cb(null)` 成功 | 是                      | 是                    | —            |
| close 帧       | 内部回调成功    | 是                      | 是                    | 1000         |
| close 事件     | —               | —                       | —                     | **1000**     |

**台账判定**：M0/M1/M2 的 `cb(null)`
可以记为"本机已写入 socket"（边界 B），**绝不能记为"对端已送达"**。close 事件码 1000 仅表示双向 close 握手完成，不证明对端应用层已处理这些消息。

### 5.2 时序二：本端强制终止（`ws.terminate()`）

**触发条件**：M0 正在压缩，M1 已排队，此时调用 `ws.terminate()`。

**代码路径**：

1. `WebSocket.terminate()` 将 `readyState` 设为 `CLOSING`，直接调用
   `socket.destroy()`。**不经过 `Sender.close()`，不发送 close 帧**。
2. socket 的 `'close'` 事件触发 `socketOnClose()`：设置
   `readyState = CLOSING`，清除关闭定时器，读取 socket 接收缓冲区残留数据写入 Receiver，调用
   `receiver.end()`。
3. 与此同时，zlib 的压缩操作仍在 libuv 线程池中异步执行。
4. 压缩完成回调在
   [sender.js:513-521](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L513-L521)
   检查 `this._socket.destroyed`：
   - 为 `true` → 构造 `"The socket was closed while data was being compressed"`
     错误
   - 调用 `callCallbacks(this, err, cb)`：先以错误调用 M0 的回调，再**遍历
     `_queue` 中所有待发操作**，逐个以同一错误调用其回调
   - **不调用 `dequeue()`**，队列中的 M1 不会被执行
5. `emitClose()` 调用
   `perMessageDeflate.cleanup()`（[websocket.js:273-274](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L273-L274)）：若 deflate 流仍有挂起回调（取决于时序），以
   `"The deflate stream was closed while data was being processed"` 错误调用它。

**为何存在两种错误消息**：这取决于 zlib 回调和 `cleanup()` 谁先执行。若 zlib
flush 回调在 `cleanup()` 关闭流之前完成，走 `socket.destroyed`
检查路径，消息为 "closed while data was being compressed"。若 `cleanup()` 先调用
`deflate.close()`，则 deflate 流的挂起回调收到 "deflate stream was closed while
data was being processed"。两者都表示消息未写入 socket。

| 对象           | 回调结果         | 数据是否写入本地 socket | 对端是否收到 | close 事件码 |
| -------------- | ---------------- | ----------------------- | ------------ | ------------ |
| M0（正在压缩） | `cb(Error)` 失败 | **否**                  | 否           | —            |
| M1（排队中）   | `cb(Error)` 失败 | **否**                  | 否           | —            |
| close 帧       | **未发送**       | 否                      | 否           | —            |
| close 事件     | —                | —                       | —            | **1006**     |

**台账判定**：M0/M1 的回调收到 Error
→ 消息未离开本机，**应标记为"发送失败，需要重发"**，不能记为任何形式的"已发送"。close 码 1006（abnormal
closure）确认没有进行 WebSocket 关闭握手。对端收到的也是 1006（TCP
RST 或 FIN 无 close 帧）。

### 5.3 时序三：对端在压缩尚未结束时开始关闭

**触发条件**：本端 M0 正在压缩，对端此时发送 close 帧。

**代码路径**：

1. 对端 close 帧到达本端 socket，`Receiver` 解析到 opcode 0x08，emit
   `'conclude'` 事件。
2. `receiverOnConclude()`（[websocket.js:1168-1182](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1168-L1182)）：
   - 设置 `_closeFrameReceived = true`，记录对端 close code 和 reason
   - **移除 socket 的 `'data'` 监听器**（停止读取新数据），resume socket
   - 调用 `this.close(code, reason)` 回应 close 帧
3. 此时本端 `readyState` 仍是 `OPEN`，`WebSocket.close()` 将其设为
   `CLOSING`，调用 `Sender.close()`。
4. `Sender.close()` 发现 `_state === DEFLATING`，将回应 close 帧推入
   `_queue`（排在 M0 之后）。
5. M0 压缩完成，回调检查
   `socket.destroyed === false`（socket 只是半关闭了读端，写端仍然可用）→
   `sendFrame(M0)` → `dequeue()` 发送 close 帧。
6. close 帧回调设置 `_closeFrameSent = true`，因 `_closeFrameReceived`
   已为 true，调用 `socket.end()`。
7. 对端收到 M0 数据帧和本端 close 帧后，关闭连接。

**为何出站数据不受影响**：对端关闭的是它的发送方向（FIN），本端 socket 的写端仍然可写。WebSocket 关闭握手要求双方都发送 close 帧，但不要求先停止发送数据。`receiverOnConclude`
只移除了 `'data'` 监听器（不再处理入站数据），并未销毁 socket，因此
`socket.destroyed` 仍为 `false`，压缩回调走成功路径。

| 对象              | 回调结果        | 数据是否写入本地 socket | 对端是否收到 | close 事件码                   |
| ----------------- | --------------- | ----------------------- | ------------ | ------------------------------ |
| M0（正在压缩）    | `cb(null)` 成功 | 是                      | **是**       | —                              |
| 对端 close 帧     | 本端收到        | —                       | —            | —                              |
| 本端回应 close 帧 | 内部回调成功    | 是                      | 是           | —                              |
| close 事件        | —               | —                       | —            | **对端发来的 code**（如 1000） |

**台账判定**：M0 的 `cb(null)`
仍表示边界 B（已写入本地 socket）。此时 close 码是对端传来的码，不是本端主动关闭的码。如果对端在 close
reason 中表示"即将断开"，M0 仍有时间在 TCP 写通道关闭前发出，但**仍然不保证对端应用层已经处理**——对端可能在收到 M0 之前就已经关闭了应用层。

### 5.4 边界 B 之后的 `terminate()`：回调成功但连接异常

还有一种容易误判的时序：M0 压缩完成，`sendFrame()` 调用了
`socket.write()`，用户回调 `cb(null)` 执行（边界 B），但**在同一个回调中立即调用
`terminate()`**。

- 用户回调 `cb(null)` 确实表示数据已写入内核 TCP 发送缓冲区。
- 但 `terminate()` 调用 `socket.destroy()`，可能导致：
  - 内核发送缓冲区中的数据被 RST 包替代（取决于 TCP 状态和 linger 设置），对端可能收不到。
  - close 事件码为 1006。
- 此时台账上已经记录了"边界 B 成功"，但对端实际可能未收到。

**这证明了边界 B 不是"对端已送达"的充分条件**。即使回调成功，后续的
`terminate()`
或网络故障仍可能导致数据丢失。close 码 1006 是一个危险信号，说明连接异常终止，此前所有"边界 B 成功"的消息都应被视为"未确认送达"。

### 5.5 结果矩阵总表

| 关闭时序                | 正在压缩的消息        | 排队中的消息          | close 后新 send              | close 事件码 | 对端能收到出站数据？  |
| ----------------------- | --------------------- | --------------------- | ---------------------------- | ------------ | --------------------- |
| 本端 `close()`          | cb 成功，写入 socket  | cb 成功，写入 socket  | 被拒绝，cb 收到 Error        | 1000         | 能，FIFO 顺序         |
| 本端 `terminate()`      | cb 收到 Error，未写入 | cb 收到 Error，未写入 | 被拒绝（readyState=CLOSING） | 1006         | 否                    |
| 对端先发 close          | cb 成功，写入 socket  | cb 成功，写入 socket  | 被拒绝                       | 对端 code    | 能（在 close 帧之前） |
| 边界 B 后 `terminate()` | cb 已成功             | 取决于排队状态        | 被拒绝                       | 1006         | 不确定                |

---

## 6. 投递台账判定规则

### 6.1 可以记入台账的状态

| 台账字段             | 判定条件                     | 含义                                          | 对应边界 |
| -------------------- | ---------------------------- | --------------------------------------------- | -------- |
| `accepted_by_sender` | `ws.send()` 同步返回未抛异常 | 消息已被 WebSocket 发送层接纳，将按 FIFO 处理 | 边界 A   |
| `written_to_socket`  | `send` 回调 `cb(null)` 执行  | 帧数据已通过 `socket.write()` 交给本机 TCP 栈 | 边界 B   |
| `peer_received`      | 对端应用层 ACK 消息到达      | 对端应用层已处理（需自建 ACK 机制）           | 边界 C   |

### 6.2 绝不能记入台账的状态

- **绝不能**因为 `cb(null)` 就将消息标记为 `peer_received`
  或"对端已送达"。`cb(null)` 只代表边界 B。
- **绝不能**因为 close 事件码为 1000 就认为之前所有消息都已被对端应用层处理。1000 只表示 WebSocket
  close 帧握手完成。
- **绝不能**因为 `bufferedAmount === 0` 就认为对端已收到。`bufferedAmount`
  只反映本机发送队列和内核缓冲区，不反映网络传输或对端状态。
- **绝不能**因为 `readyState === CLOSING` 就认为已排队的消息会被丢弃。`close()`
  会等待队列排空，只有 `terminate()` 才会立即丢弃。

### 6.3 回调错误与台账动作

| 回调错误消息                                                     | 原因                               | 台账动作                                     |
| ---------------------------------------------------------------- | ---------------------------------- | -------------------------------------------- |
| `"The socket was closed while data was being compressed"`        | 压缩完成时发现 socket 已被 destroy | 标记为**发送失败**，消息未离开本机，可重发   |
| `"The deflate stream was closed while data was being processed"` | `cleanup()` 关闭了 deflate 流      | 标记为**发送失败**，消息未离开本机，可重发   |
| `"WebSocket is not open: readyState 2 (CLOSING)"`                | `close()` 后调用 `send()`          | 消息**从未进入发送队列**，不应计入 in-flight |
| `"WebSocket is not open: readyState 3 (CLOSED)"`                 | `close` 事件后调用 `send()`        | 消息**从未进入发送队列**                     |

### 6.4 close 事件码与台账对账

| close 码                       | 含义                                                     | 对台账的影响                                                         |
| ------------------------------ | -------------------------------------------------------- | -------------------------------------------------------------------- |
| 1000                           | 正常关闭，双向 close 握手完成                            | 边界 B 的消息大概率已发出，但仍不保证对端应用层处理；需 ACK 确认     |
| 1006                           | 异常关闭，无 close 帧（通常是 `terminate()` 或网络断开） | 所有未到边界 C 的消息应标记为"未确认"；回调收到 Error 的标记为"失败" |
| 1005                           | 对端关闭但无状态码（空 close 帧）                        | 同 1000，但无对端原因                                                |
| 其他（1001/1008/1009/1011 等） | 对端带原因关闭                                           | 检查 reason 文本判断是否影响消息投递；出站消息仍会在 close 帧前发送  |

### 6.5 回调执行顺序保证

在优雅关闭（`close()`）场景下，所有在 `close()` 之前调用的 `send()`
的回调，**按 FIFO 顺序全部执行完毕后**，close 事件才会触发。这由以下机制保证：

1. 每帧的 `socket.write()` 回调按写入顺序触发（Node.js stream 保证）。
2. `dequeue()` 在上一帧的 `sendFrame()` 之后同步调用，开始下一帧的压缩/发送。
3. close 帧排在队列末尾，其写入回调在所有数据帧回调之后。
4. close 事件在 socket `'close'` 事件后触发，晚于所有 write 回调。

因此台账可以安全地在每个 `cb(null)` 中逐条更新状态，在 close 事件中做最终对账。

---

## 7. 评审结论：默认行为、配置影响与信号判读

### 7.1 客户端与服务端的默认协商行为

| 角色                               | `perMessageDeflate` 默认值 | 默认行为                                                                                                       | 源码位置                                                                                                                                                                                                                                                                                                       |
| ---------------------------------- | -------------------------- | -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 客户端（`new WebSocket(url)`）     | `true`                     | 主动在 Upgrade 请求中携带 `Sec-WebSocket-Extensions: permessage-deflate; client_max_window_bits`，请求启用压缩 | [websocket.js:677](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L677), [websocket.js:769-778](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L769-L778)                                                                                                                              |
| 服务端（`new WebSocket.Server()`） | `false`                    | **不主动启用压缩**；只有显式设置 `perMessageDeflate: true` 或配置对象时，才解析客户端的扩展头并协商            | [websocket-server.js:76](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket-server.js#L76), [websocket-server.js:133](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket-server.js#L133), [websocket-server.js:298-321](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket-server.js#L298-L321) |

**默认协商结果**：

- 网关作为**服务端**时，若未显式配置
  `perMessageDeflate: true`，即使客户端请求压缩，服务端也不会接受，所有消息均以未压缩帧传输。
- 网关作为**客户端**（回源或连接上游）时，默认请求压缩；若上游服务端接受，则出站消息默认压缩（`WebSocket.send()`
  的 `opts.compress` 默认为 `true`，见
  [websocket.js:475](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L475)）。
- 默认协商参数：`client_max_window_bits`（客户端通告支持），不启用
  `*_no_context_takeover`，即**上下文接管默认开启**，压缩字典在消息之间复用。
- `threshold`（默认 1024 字节）仅在 `*_no_context_takeover`
  协商成功时生效。默认配置下上下文接管开启，**所有消息（包括小消息）都会压缩**，不受 threshold 影响（见
  [sender.js:372-383](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L372-L383)）。

### 7.2 影响资源与排队表现的配置

| 配置项                                                      | 默认值               | 影响维度                                                                                                | 高扇出场景注意事项                                                                                                                                                                                        | 源码位置                                                                                                                                                                                                                       |
| ----------------------------------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `concurrencyLimit`                                          | `10`                 | **进程级全局** zlib 并发数。所有连接的压缩/解压操作共享同一个 `Limiter`，超出部分在 Limiter 队列中等待  | 高扇出时，即使单连接的 Sender 队列为空，压缩操作也可能因全局 zlib 线程池拥堵而排队。这会放大"正在压缩"的时间窗口，增加关闭时数据滞留的概率。该值在首次创建 `PerMessageDeflate` 实例时确定，后续实例不覆盖 | [permessage-deflate.js:65-71](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L65-L71), [limiter.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/limiter.js)                                             |
| `server_no_context_takeover` / `client_no_context_takeover` | `false`              | 每条消息后重置 deflate/inflate 字典。开启后压缩率略降，但内存占用不随消息历史增长，且 threshold 生效    | 高扇出服务端建议开启 `server_no_context_takeover`，避免每条连接的 deflate 流持有不断增长的压缩上下文内存。代价是压缩率降低和 CPU 开销略增                                                                 | [permessage-deflate.js:90-95](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L90-L95), [permessage-deflate.js:454-456](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L454-L456)     |
| `threshold`                                                 | `1024`               | 仅在 `*_no_context_takeover` 启用时生效，小于此字节数的消息不压缩（RSV1=0）                             | 对快照行情（通常远大于 1KB）无影响；可减少小消息的 CPU 开销                                                                                                                                               | [permessage-deflate.js:56-57](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L56-L57), [sender.js:381](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L381)                                      |
| `zlibDeflateOptions` / `zlibInflateOptions`                 | `{}`                 | 透传给 `zlib.createDeflateRaw()` / `zlib.createInflateRaw()`，可设置 `level`、`memLevel`、`strategy` 等 | 降低 `level` 可减少 CPU 开销但压缩率下降；降低 `memLevel` 可减少内存但影响压缩率和性能                                                                                                                    | [permessage-deflate.js:414-417](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L414-L417), [permessage-deflate.js:349-352](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L349-L352) |
| `closeTimeout`                                              | `30000`（30 秒）     | `close()` 后等待关闭握手完成的超时；超时强制 `socket.destroy()`                                         | 高扇出优雅下线时，30 秒可能过长。可调小以加速连接回收，但会增加内核发送缓冲区中未发出数据丢失的风险                                                                                                       | [constants.js:10](file:///e:/newGsb/questions/GSB-005/Steve/lib/constants.js#L10), [websocket.js:1309-1314](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1309-L1314)                                            |
| `maxPayload`                                                | `104857600`（100MB） | 接收侧解压后消息的最大字节数，超出则以 1009 关闭连接                                                    | 快照推送方向为出站时影响不大；若接收客户端的大消息，需注意此限制                                                                                                                                          | [websocket.js:675](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L675), [receiver.js:439-453](file:///e:/newGsb/questions/GSB-005/Steve/lib/receiver.js#L439-L453)                                                |

**关键资源特征**：

- 每条启用压缩的连接持有独立的 `_deflate` 和 `_inflate`
  zlib 流（[permessage-deflate.js:60-61](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L60-L61)）。上下文接管开启时，这些流的内存随消息历史增长。
- 压缩操作是异步的，通过 libuv 线程池执行。Node.js 默认线程池大小为 4，`concurrencyLimit`
  默认 10 意味着最多 10 个压缩/解压操作在线程池队列中等待。
- Sender 队列（`_queue`）是单连接级别的 FIFO 数组，无大小上限。高扇出时若单连接发送速度超过网络传输速度，队列会无限增长（`_bufferedBytes`
  持续累加）。

### 7.3 只能说明本机进度、不能代表交付完成的信号

| 信号                      | 本机含义                                            | 不能推断的内容                                                        | 证据来源                                                                                         |
| ------------------------- | --------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `ws.send()` 同步返回      | 消息已被 Sender 接收（边界 A）                      | 消息尚未压缩、未写入 socket、未经过网络                               | [sender.js:411-413](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L411-L413)           |
| `send` 回调 `cb(null)`    | 帧已通过 `socket.write()` 交给本机 TCP 栈（边界 B） | 对端未 ACK、未解析帧、未解压、未触发 `'message'`                      | [sender.js:563-572](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L563-L572)           |
| `ws.bufferedAmount === 0` | 本机 Sender 队列和内核发送缓冲区均为空              | 不代表对端已收到或处理；数据可能在网络中或对端内核缓冲区              | [websocket.js:120-124](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L120-L124)     |
| `ws.readyState === OPEN`  | 本端 WebSocket 状态机处于打开                       | 不代表对端仍然存活或网络可达                                          | [websocket.js:67](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L67)                |
| close 事件码 `1000`       | 双向 close 帧握手完成                               | 不保证 close 帧之前的所有数据帧已被对端应用层处理                     | [websocket.js:302-340](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L302-L340)     |
| `'open'` 事件触发         | WebSocket 握手完成，Sender/Receiver 已创建          | 不代表对端应用层已准备好接收数据                                      | [websocket.js:257-258](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L257-L258)     |
| `'pong'` 事件             | 收到对端的 pong 帧                                  | 仅证明对端 TCP/WebSocket 协议栈存活，不证明对端应用层处理了之前的消息 | [websocket.js:1261-1263](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1261-L1263) |

**监控台账应区分的状态**：

- `accepted`（边界 A）：`send()` 返回成功，可以计入"已提交发送层"。
- `flushed`（边界 B）：`cb(null)` 执行，可以计入"已写入本机 socket"。
- `confirmed`（边界 C）：收到对端应用层 ACK，可以计入"对端已确认"——**此状态无法从 ws 库获得，必须在应用层实现**。
- `failed`：`cb(Error)` 执行，可以计入"发送失败，需重发"。

### 7.4 最终建议

#### 建议一：高扇出网关的压缩配置

**适用条件**：网关作为 WebSocket 服务端，单进程维护数千条长连接，推送以大快照（>1KB）为主。

**建议配置**：

```js
const wss = new WebSocket.Server({
  perMessageDeflate: {
    serverNoContextTakeover: true,
    concurrencyLimit: 10,
    threshold: 1024
  },
  closeTimeout: 5000
  // ... 其他选项
});
```

**理由**：

- `serverNoContextTakeover: true`：限制每条连接的 deflate 流内存不随消息历史增长，对高扇出场景的内存可控性至关重要。上下文接管带来的压缩率提升对大快照收益有限，而内存风险在数千连接下会放大。
- `threshold: 1024`：配合 noContextTakeover，小消息不压缩，减少 zlib 线程池压力。
- `closeTimeout: 5000`：优雅下线时 5 秒足够已排队数据写入 socket，避免 30 秒过长导致连接堆积。但需确认业务能接受 5 秒后未发出数据丢失的风险。
- `concurrencyLimit`：默认 10 在大多数场景下足够；若 CPU 核心数较多且快照压缩成为瓶颈，可适当调高，但需监控 zlib 线程池延迟。

**不建议**：在高扇出服务端使用默认的上下文接管（即不设置
`serverNoContextTakeover`），因为每条连接的压缩字典会持续增长。

#### 建议二：台账状态机

**适用条件**：需要追踪每笔消息的投递状态，用于关闭时判断是否需要重发或对账。

**建议**：使用三级状态机，不跨越边界推断：

| 状态          | 进入条件                                    | 退出条件                                                        | 可用于                                                         |
| ------------- | ------------------------------------------- | --------------------------------------------------------------- | -------------------------------------------------------------- |
| `accepted`    | `ws.send()` 同步返回                        | `cb(null)` → `flushed`；`cb(Error)` → `failed`                  | 判断消息已提交，但不能用于判断是否需要重发（关闭时仍可能成功） |
| `flushed`     | `cb(null)`                                  | 收到对端 ACK → `confirmed`；连接异常关闭（1006）→ `unconfirmed` | 判断消息已离开本机，但不能标记为"已送达"                       |
| `confirmed`   | 对端应用层 ACK 到达                         | —                                                               | 唯一可标记为"对端已送达"的状态                                 |
| `failed`      | `cb(Error)` 或连接 1006 且未到 `flushed`    | —                                                               | 消息未离开本机，应重发                                         |
| `unconfirmed` | 已 `flushed` 但连接异常关闭（1006）且无 ACK | —                                                               | 消息可能已到达对端也可能丢失，需业务层对账                     |

**关键规则**：

- `close` 码 1000 + `flushed` 状态：消息大概率已发出，但在收到 ACK 前不应标记为
  `confirmed`。
- `close` 码 1006 + `flushed` 状态：标记为
  `unconfirmed`，不自动重发（可能导致重复），等待业务层对账。
- `close` 码 1006 + `accepted` 状态（回调未执行）：标记为 `failed`，可安全重发。

#### 建议三：优雅关闭流程

**适用条件**：网关需要滚动重启或主动断开连接，希望尽量减少消息丢失。

**建议流程**：

1. 停止向该连接调用新的 `ws.send()`。
2. 等待所有已 `accepted` 消息的回调执行（到达 `flushed` 或 `failed`）。可通过
   `ws.bufferedAmount` 和回调计数判断。
3. 对于 `failed` 的消息，触发业务层重发逻辑。
4. 调用 `ws.close(code)`，等待 `'close'` 事件。
5. 若 `closeTimeout` 超时触发 `socket.destroy()`，所有未到 `flushed`
   的消息标记为 `failed`。

**不建议**在未等待回调的情况下直接 `ws.close()` 后假设数据已发出。虽然 `close()`
会排队等待数据写入，但写入成功只代表边界 B，且 `closeTimeout`
超时仍可能丢弃数据。

### 7.5 剩余风险

以下风险无法通过配置或台账完全消除，属于 WebSocket/TCP 协议和本库设计的固有约束：

1. **边界 B 之后的网络丢失**：`cb(null)`
   后数据仍在内核发送缓冲区或网络中，TCP 重传失败、对端 RST、进程崩溃等都可能导致数据丢失。本库不提供应用层 ACK，无法检测此类丢失。
   - 证据：测试 "callback success at boundary B does not survive a subsequent
     terminate" 验证了 `cb(null)` 后 `terminate()` 仍可导致 close 码 1006。

2. **对端应用层未处理**：即使对端 TCP 栈 ACK 了数据，对端的 WebSocket
   Receiver 可能还在解压队列中，或应用层 `'message'`
   回调尚未执行。对端在此期间关闭连接，数据不会触发 `'message'`。
   - 证据：[websocket.js:1177](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1177)
     在收到 close 帧后立即移除 `'data'`
     监听器，虽然已缓冲的数据会被 flush（[websocket.js:1339-1348](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1339-L1348)），但对端应用层是否处理取决于其消费速度。

3. **全局 zlib 排队导致的关闭窗口放大**：`concurrencyLimit`
   是进程级全局限制。高扇出时，一笔大快照的压缩可能被 zlib 线程池延迟，在此期间连接被关闭的概率增加。Sender 队列中的消息在
   `terminate()` 时会全部失败。
   - 证据：[permessage-deflate.js:65-71](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L65-L71)
     确认 `zlibLimiter` 是模块级变量；测试 timing
     2 验证了排队消息在 terminate 时全部收到错误。

4. **`closeTimeout` 超时丢数据**：`close()` 启动的定时器超时后调用
   `socket.destroy()`，此时内核发送缓冲区中尚未被 TCP 栈发出的数据会被丢弃，且这些数据已经过了边界 B（`cb(null)`
   已执行）。台账会显示 `flushed` 但数据实际丢失。
   - 证据：[websocket.js:1309-1314](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1309-L1314)
     确认超时直接 `socket.destroy()`。

5. **无背压的无限队列**：Sender 的 `_queue` 数组无大小上限。若应用层 `send()`
   速度持续超过网络传输速度，队列会无限增长，`bufferedAmount`
   持续上升，最终导致内存溢出。本库不提供队列水位限制或 `'drain'` 事件。
   - 证据：[sender.js:551-554](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L551-L554)
     的 `enqueue()` 仅做 push，无大小检查。

### 7.6 本地证据索引

所有结论均由以下本地证据支撑：

| 结论                                    | 证据类型                               | 位置                                                                                                                                                                                                            |
| --------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| close 帧排在压缩消息之后（FIFO）        | 测试 scenario B + timing 1             | [test/close-during-compression.test.js:92-144](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js#L92-L144)                                                                        |
| terminate 丢弃压缩中和排队的消息        | 测试 scenario C + timing 2             | [test/close-during-compression.test.js:146-192](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js#L146-L192)                                                                      |
| 对端关闭不影响出站压缩数据              | 测试 scenario H + timing 3             | [test/close-during-compression.test.js:394-434](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js#L394-L434)                                                                      |
| cb(null) 不等于对端收到                 | 测试 scenario E + boundary B+terminate | [test/close-during-compression.test.js:249-291](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js#L249-L291)                                                                      |
| close() 后 send() 被拒绝                | 测试 scenario F + late send            | [test/close-during-compression.test.js:293-336](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js#L293-L336)                                                                      |
| 回调 FIFO 顺序，close 事件在最后        | 测试 FIFO order                        | [test/close-during-compression.test.js:735-785](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js#L735-L785)                                                                      |
| 排队中的 ping 也被 terminate 丢弃       | 测试 queued ping                       | [test/close-during-compression.test.js:787-839](file:///e:/newGsb/questions/GSB-005/Steve/test/close-during-compression.test.js#L787-L839)                                                                      |
| 客户端默认请求压缩                      | 源码                                   | [websocket.js:677](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L677)                                                                                                                             |
| 服务端默认不启用压缩                    | 源码                                   | [websocket-server.js:76](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket-server.js#L76)                                                                                                                 |
| zlibLimiter 是全局单例                  | 源码                                   | [permessage-deflate.js:24](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L24), [permessage-deflate.js:65-71](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L65-L71) |
| threshold 仅在 noContextTakeover 时生效 | 源码                                   | [sender.js:372-383](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L372-L383)                                                                                                                          |
| closeTimeout 超时 destroy socket        | 源码                                   | [websocket.js:1309-1314](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1309-L1314)                                                                                                                |

运行全部证据：`npx mocha test/close-during-compression.test.js --timeout 10000`（15 个用例全部通过）。

---

## 8. 代码引用索引

| 机制                                | 文件                                                                                         | 关键行                                                                                                                                                         |
| ----------------------------------- | -------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Sender 状态机（DEFAULT/DEFLATING）  | [sender.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js)                         | [L22-L24](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L22-L24), [L503-L529](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L503-L529)     |
| 发送队列（enqueue/dequeue）         | [sender.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js)                         | [L536-L554](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L536-L554)                                                                                 |
| close 帧入队逻辑                    | [sender.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js)                         | [L224-L228](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L224-L228)                                                                                 |
| sendFrame 写入 socket               | [sender.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js)                         | [L563-L572](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L563-L572)                                                                                 |
| 压缩期间 socket 销毁的错误处理      | [sender.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js)                         | [L513-L521](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L513-L521), [L585-L594](file:///e:/newGsb/questions/GSB-005/Steve/lib/sender.js#L585-L594) |
| WebSocket.close() 状态机            | [websocket.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js)                   | [L302-L340](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L302-L340)                                                                              |
| sendAfterClose（close 后 send）     | [websocket.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js)                   | [L1138-L1159](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1138-L1159)                                                                          |
| terminate()                         | [websocket.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js)                   | [L492-L504](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L492-L504)                                                                              |
| 接收 close 帧（receiverOnConclude） | [websocket.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js)                   | [L1168-L1182](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1168-L1182)                                                                          |
| emitClose 与 cleanup                | [websocket.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js)                   | [L266-L280](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L266-L280)                                                                              |
| PerMessageDeflate.compress          | [permessage-deflate.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js) | [L322-L329](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L322-L329)                                                                     |
| PerMessageDeflate.cleanup           | [permessage-deflate.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js) | [L130-L150](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L130-L150)                                                                     |
| zlib 并发限制 Limiter               | [permessage-deflate.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js) | [L65-L71](file:///e:/newGsb/questions/GSB-005/Steve/lib/permessage-deflate.js#L65-L71), [limiter.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/limiter.js) |
| bufferedAmount 计算                 | [websocket.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js)                   | [L120-L124](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L120-L124)                                                                              |
| 关闭定时器                          | [websocket.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js)                   | [L1309-L1314](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L1309-L1314)                                                                          |
| 压缩协商（客户端 offer）            | [websocket.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js)                   | [L769-L778](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket.js#L769-L778)                                                                              |
| 压缩协商（服务端 accept）           | [websocket-server.js](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket-server.js)     | [L298-L321](file:///e:/newGsb/questions/GSB-005/Steve/lib/websocket-server.js#L298-L321)                                                                       |
