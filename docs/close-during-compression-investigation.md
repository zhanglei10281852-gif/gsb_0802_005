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
覆盖了以下 8 个场景，均可通过 `npx mocha test/close-during-compression.test.js`
运行：

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

## 5. 代码引用索引

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
