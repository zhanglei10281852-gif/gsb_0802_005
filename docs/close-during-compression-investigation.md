# 压缩大快照与优雅关闭相遇时的发送路径调查

> 范围：仅调查 `ws@8.21.1` 现有实现，不改动 `lib/`、公开 API 或协议语义。
> 结论用于接入方做交付判断（"这笔消息到底走到了哪里"）。
> 复现脚本：[test/close-during-compression-repro.js](file:///e:/newGsb/questions/GSB-005/Tony/test/close-during-compression-repro.js)
> 运行方式：`node test/close-during-compression-repro.js`（不新增任何依赖）。

## 1. 问题与结论速览

高峰期的现象是：服务端刚发出一个压缩后的大快照，随即调用 `ws.close()` 做优雅关闭，现场难以判断这笔消息走到了哪一步。

调查结论：

1. **优雅关闭（`close()`）会排队，不会抢跑、也不会丢弃正在压缩/排队的数据消息。** `close()` 把一个 close 帧作为普通发送任务放进 `Sender` 的 FIFO 队列尾部；在它之前入队的数据帧会先被压缩、组帧并写入 socket。因此一笔在 `close()` 之前调用的 `send()`，其数据帧在协议层一定先于 close 帧离开本端。
2. **从应用调用 `send()`，到对端真正 `message`，中间隔着三个本质不同的边界。** 其中只有"写入本地 socket"由 `send()` 的回调通知，而"对端实际收到"本库无法感知（见第 3 节）。
3. **压缩是异步的。** 对大消息，`send()` 返回时数据可能仍在 zlib 线程里压缩，此时既没有组帧、也没有写 socket；`close()` 此刻调用，close 帧只是排在后面等待（见场景 B）。
4. **`terminate()` 与 `close()` 完全不同。** `terminate()` 立即 `socket.destroy()`，会中断正在进行的压缩，`send()` 回调收到错误，数据不会发出（见场景 C）。这是唯一会导致"已被发送层接收但最终没写入 socket"的路径。

---

## 2. 代码路径与四个模块的关系

发送一条压缩数据消息，会依次穿过：压缩协商 → 发送队列 → socket 写入 → 关闭握手。

### 2.1 压缩协商（permessage-deflate 是否生效）

- 客户端在握手时通过 `Sec-WebSocket-Extensions` 头带上 `permessage-deflate` 要约，见 [initAsClient](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L769-L778)。
- 服务端在 [handleUpgrade](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket-server.js#L298-L321) 中解析并接受该扩展，把协商好的 `PerMessageDeflate` 实例挂到 `ws._extensions`。
- 客户端在 upgrade 回调里 `perMessageDeflate.accept(...)` 后同样挂到 `_extensions`，见 [websocket.js#L1017-L1027](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1017-L1027)。

关键点在 [WebSocket.prototype.send](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L455-L485)：

```js
if (!this._extensions[PerMessageDeflate.extensionName]) {
  opts.compress = false;
}
this._sender.send(data || EMPTY_BUFFER, opts, cb);
```

**只有扩展真正协商成功，`compress` 才可能为 true**；否则即便调用方传了 `compress: true` 也会被强制置 false，消息明文发送。判断一笔消息是否真的被压缩，要看连接上 `ws.extensions` 是否包含 `permessage-deflate`，而不是调用方的选项。

压缩本身由 [PerMessageDeflate.compress](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L322-L329) 完成，它通过全局 `zlibLimiter`（默认并发 10）把任务丢到 libuv 线程池，**异步**回调。这正是"大快照"场景里 `send()` 返回后数据仍未就绪的原因。

### 2.2 发送队列与状态机（Sender）

[Sender](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L29-L56) 内部有一个状态机和一个 FIFO 队列：

- `_state`：`DEFAULT(0)` 空闲、`DEFLATING(1)` 正在压缩、`GET_BLOB_DATA(2)` 正在读 Blob。
- `_queue`：当 `_state !== DEFAULT` 时，后续 `send/ping/pong/close` 全部 `enqueue`，见 [sender.js#L409-L413](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L409-L413) 与 [sender.js#L224-L228](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L224-L228)。
- `_bufferedBytes`：正在处理和排队中的**未压缩**字节数，计入 `ws.bufferedAmount`。

核心压缩分发逻辑在 [dispatch](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L503-L529)：

```js
this._bufferedBytes += options[kByteLength];
this._state = DEFLATING;
perMessageDeflate.compress(data, options.fin, (_, buf) => {
  if (this._socket.destroyed) {
    callCallbacks(this, err, cb);   // socket 已销毁，回调报错，不再写
    return;
  }
  this._bufferedBytes -= options[kByteLength];
  this._state = DEFAULT;
  this.sendFrame(Sender.frame(buf, options), cb);  // 组帧并写 socket
  this.dequeue();                                  // 处理队列里下一个任务
});
```

`dequeue()` 在 [sender.js#L536-L543](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L536-L543) 严格按入队顺序处理后续任务。**这保证了：数据消息、close 帧的发送顺序就是调用顺序。**

### 2.3 写入本地 socket（sendFrame 与回调）

[sendFrame](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L563-L572) 最终调用的是 Node `net.Socket` 的 `write`，并把调用方传入的 `cb` 作为 `socket.write` 的回调：

```js
sendFrame(list, cb) {
  if (list.length === 2) {
    this._socket.cork();
    this._socket.write(list[0]);
    this._socket.write(list[1], cb);   // cb 在此刻才与一次真正的 socket.write 绑定
    this._socket.uncork();
  } else {
    this._socket.write(list[0], cb);
  }
}
```

**`send(data, cb)` 的 `cb` 被触发，等价于"该帧的字节已经交给 Node 的 socket 层"，而不是"对端收到了"。** 这是 Node 流的契约：`writable.write(chunk, cb)` 的回调在数据被**写入内核发送缓冲区/离开 Node 用户态缓冲**时被调用。若发生背压，回调会延后到缓冲区排空。

### 2.4 优雅关闭握手（close）

[WebSocket.prototype.close](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L302-L340) 的关键行为：

```js
this._readyState = WebSocket.CLOSING;
this._sender.close(code, data, !this._isServer, (err) => {
  if (err) return;
  this._closeFrameSent = true;
  if (this._closeFrameReceived || ...) {
    this._socket.end();
  }
});
setCloseTimer(this);   // 默认 30s，超时则 socket.destroy()
```

- `close()` 把状态置为 `CLOSING`，然后**调用 `this._sender.close(...)`**。由 2.2 可知，如果此时 `_state` 是 `DEFLATING`，close 帧会被 `enqueue` 到数据消息之后，而不是立即发出。
- 当 close 帧真正 `sendFrame` 写完、回调无错时，才置 `_closeFrameSent = true`。
- `socket.end()`（半关闭写侧，发送 FIN）只在"close 帧已发出 **且** 已收到对端 close 帧（或接收端出错）"时才调用。这意味着 `socket.end()` 会等队列里排在 close 帧之前的数据全部写完。
- 若对端不回应 close，30 秒后 [setCloseTimer](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1309-L1314) 强制 `socket.destroy()`。

对照 [terminate()](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L492-L504)：它直接 `socket.destroy()`，不经过 Sender 队列，不等任何排队数据。

---

## 3. 三个边界：如何判断一笔消息走到了哪里

一笔数据从"应用调用 send"到"对端业务收到 message"，存在三个必须严格区分的边界：

| 边界 | 何时到达 | 代码标志 / 可观测信号 | 对交付的意义 |
|------|----------|----------------------|--------------|
| **① 已被发送层接收** | `ws.send()` 同步返回 | `Sender._state` 变为 `DEFLATING`，`_bufferedBytes` 增加，任务进入 `_queue`；`ws.bufferedAmount > 0` | 仅表示库已接管这笔数据。若此刻 `terminate()`，数据会丢失，`send` 回调收到错误。 |
| **② 已写入本地 socket** | `send(data, cb)` 的 **cb 被调用且无错** | `socket.write` 回调触发；`Sender._bufferedBytes` 减少；`ws.bufferedAmount` 下降 | 数据已离开 Node 进程、进入本机内核发送缓冲区（或已被内核确认接收）。**仍不等于对端收到。** |
| **③ 对端实际收到并解析** | 对端触发 `'message'` 事件 | 本端无法直接观测；只能靠应用层 ACK | 数据真正到达对端并通过了 WebSocket 解析/解压。 |

**② 与 ③ 之间还隔着什么：**

1. **本机内核发送缓冲区**：`socket.write` 回调只说明字节交给了内核 TCP 发送缓冲区。若对端窗口为零（慢消费者/网络拥塞），这些字节会一直停留在本机内核里。
2. **网络传输**：IP 路由、丢包重传、TCP 分段与重组，都可能让字节延迟或（在连接中断时）丢失。
3. **对端内核接收缓冲区**：字节到达对端机器，但对端应用尚未 `read()`。
4. **对端 WebSocket 解析与解压**：即便字节被读取，`Receiver` 还要完成帧重组、（压缩消息的）inflate、UTF-8/大小校验，才会 emit `'message'`。
5. **对端应用事件循环**：`'message'` 回调要等对端事件循环调度才执行。

因此：

- **`send` 回调成功 = 边界②，不能用来证明对端已经处理。**
- 若需要确认边界③，必须由**应用层在业务消息里带 ACK**（对端处理完快照后回一条确认）。WebSocket 协议层没有针对单条数据消息的送达确认，TCP 的 ACK 只到字节层，不代表对端应用已消费。
- `close` 帧被对端收到并回 close，只证明**关闭握手完成**，不证明排在它前面的某条业务消息已被对端应用处理（尽管 TCP 保证该消息字节先于 FIN/close 到达）。

### 关于 `bufferedAmount`

[bufferedAmount getter](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L120-L124)：

```js
return this._socket._writableState.length + this._sender._bufferedBytes;
```

它是 `socket` 写队列字节 + `Sender` 排队/压缩中**未压缩**字节之和。可作背压参考，但它混合了两种不同口径的计数，且为 0 只代表"没有待写字节"，不代表对端已收。

---

## 4. 复现场景与观察结果

脚本 [test/close-during-compression-repro.js](file:///e:/newGsb/questions/GSB-005/Tony/test/close-during-compression-repro.js) 只用仓库自身的 `WebSocket`/`WebSocketServer` 与 Node 内置模块，通过包装（非修改）`Sender.dispatch`/`sendFrame` 打点，启动真实本地 server/client。共 5 个场景。

### 场景 A：发完压缩大快照后立即优雅关闭（对端正常读）

发送一个 2 MiB 可压缩快照后**立刻** `close(1000)`。

观察（摘自实际运行）：

```
send() large snapshot, payloadBytes=2097152
dispatch enter opcode=2 compress=true state=0
send() returned; sender.state=1 sender.bufferedBytes=2097152 bufferedAmount=2097152
calling close() immediately after send()
close() returned; readyState=2(CLOSING) sender.state=1(DEFLATING) queueLen=1 closeFrameSent=false
... (压缩进行中，约 7ms)
-> sendFrame opcode=2 rsv1=1 frameBytes=8454     # 数据帧组帧并写 socket（2MiB 压到 8KiB）
dispatch enter opcode=8 ...                       # 随后才处理队列里的 close 帧
-> sendFrame opcode=8 rsv1=0 frameBytes=10
   socket.write CB opcode=2 err=null              # 数据帧回调（边界②）
send() CALLBACK err=null readyState=2 bufferedAmount=0
   socket.write CB opcode=8 err=null              # close 帧回调
CLIENT RECEIVED message #1 bytes=2097152          # 边界③
CLIENT RECEIVED close code=1000 reason=graceful
```

结论：
- `close()` 返回时，数据仍在压缩（`state=DEFLATING`），close 帧排在队列（`queueLen=1`，`closeFrameSent=false`）。
- 数据帧先写、close 帧后写，顺序严格保持；对端先收到 message 再收到 close。
- 这条可压缩数据压缩率极高（2 MiB → 8454 字节），所以 socket 写入几乎瞬间完成。

### 场景 B：大快照不可压缩、对端暂停读取 —— close() 时数据仍在压缩

发送 16 MiB **随机不可压缩**数据（压缩后仍约 16 MiB，压缩耗时长），对端在收到连接后暂停 socket 读取。

```
send() large incompressible snapshot, payloadBytes=16777216
dispatch enter opcode=2 compress=true state=0
calling close()
after close: sender.state=1(DEFLATING) queueLen=1 socketWritableLength=0
... 300ms later:
300ms later: sender.state=1 queueLen=1 socketWritableLength=0
             closeFrameSent=false readyState=2 sendCallbackFired=false
RESULT: data is STILL being compressed (state=DEFLATING); close frame sits
        in sender queue behind it; nothing has been framed or written yet.
... (resume 后压缩完成)
-> sendFrame opcode=2 rsv1=1 frameBytes=16782342
dispatch enter opcode=8
-> sendFrame opcode=8
CLIENT RECEIVED message #1 bytes=16777216
CLIENT RECEIVED close code=1000
```

结论（直接对应现场疑问）：
- `send()` 之后 300ms，**数据仍在 DEFLATING，既没组帧也没写 socket，`send` 回调未触发**。此时进程若崩溃，这笔数据丢失。
- `close()` 没有打断压缩，也没有抢先发送；close 帧一直排在数据后面，等压缩完成才被处理。
- 恢复读取后，数据仍先于 close 到达对端。

### 场景 C：对照 —— `terminate()` 在压缩期间

同样在压缩期间操作，但调用的是 `terminate()` 而非 `close()`：

```
send() returned; sender.state=1. calling terminate()
terminate() returned; socketDestroyed=true readyState=2
send() CALLBACK ERROR: The socket was closed while data was being compressed
CLIENT RECEIVED close code=1006
```

结论：
- `terminate()` 立即销毁 socket。压缩完成回调里检测到 `this._socket.destroyed`，通过 [callCallbacks](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L585-L594) 让 `send` 回调收到错误 `"The socket was closed while data was being compressed"`，数据帧不写、队列里其余任务回调也全部报错。
- 对端拿到的是异常关闭码 1006（没有 close 帧）。
- **这是"已被发送层接收（已过边界①）但最终没到边界②"的唯一情形。** 接入方应通过判断 `send` 回调是否收到错误来识别它。

### 场景 D：多条压缩消息排队时 close() —— 顺序保证

连发 3 条快照后 `close(1001)`：

```
after 3 sends: sender.state=1 queueLen=2 sender.bufferedBytes=1572864
calling close() ...
after close: sender.state=1 queueLen=3 (close frame is last entry)
-> sendFrame opcode=2 ... #1
-> sendFrame opcode=2 ... #2
-> sendFrame opcode=2 ... #3
-> sendFrame opcode=8 ... # close 帧最后
CLIENT RECEIVED message #1 / #2 / #3
CLIENT RECEIVED close code=1001
```

结论：close 帧是队列第 4 个任务，三条数据全部先于它发送，对端按相同顺序收到。

### 场景 E：数据已写入本地 socket（回调已触发）但对端尚未读取 —— 边界②与③的差距

关闭压缩以隔离变量，对端在 `open` 后、服务端发送前就暂停读取，发送 16 MiB 明文快照，并在数据 `send` 回调内调用 `close()`：

```
CLIENT paused client socket BEFORE server sends
SERVER send() large UNCOMPRESSED snapshot, payloadBytes=16777216
-> sendFrame opcode=2 rsv1=0 frameBytes=16777216
   socket.write CB opcode=2 err=null
data send() CALLBACK err=null ... socketDestroyed=false      # 边界②已到
called close() from within data callback
-> sendFrame opcode=8 ...
... 200ms later:
200ms later: dataCallbackFired=true socketWritableLength=0
             clientMessagesReceived=0 closeFrameSent=true readyState=2
RESULT: data was accepted by socket.write (left Node.js, in local kernel
        send buffer), but the paused peer has NOT received it.
CLIENT resuming ...
CLIENT RECEIVED message #1 bytes=16777216                    # 边界③在 200ms 后才到
CLIENT RECEIVED close code=1000
```

结论：
- `send` 回调成功（边界②）后 200ms，对端才真正收到 message（边界③）。这 200ms 里数据停留在本机/对端内核缓冲，`socketWritableLength` 在 Node 侧已经是 0（字节已离开 Node），但对端 `message` 事件尚未触发。
- 这正面说明：**回调成功不构成对端收到的证据**；在慢消费者/网络拥塞时，边界②到③之间可能有显著延迟，若此期间连接被 RST 或进程崩溃，已"写入本地 socket"的数据仍可能无法到达对端应用。

---

## 5. 给接入方的交付判断建议

针对"压缩大快照刚发出就优雅关闭"，建议接入方按下列规则判断：

1. **区分关闭方式**
   - 用的是 `ws.close()`（优雅）：排队规则保证先发的数据帧先于 close 帧离开本端，库不会主动丢弃正在压缩/排队的消息。
   - 用的是 `ws.terminate()`（强制）：不保证任何待发/压缩中消息送达，`send` 回调会收到错误，必须视为可能丢失。

2. **用 `send` 回调判断到了哪一步，而不是用它判断"对方收到"**
   - 回调未触发：消息还在发送层（压缩中或排队中，边界①之后、边界②之前）。
   - 回调无错：消息已写入本地 socket（边界②）。
   - 回调有错（例如 `"The socket was closed while data was being compressed"`）：消息未送达 socket，应按未发送处理、考虑重发。
   - 任何情况下，回调都**不**代表对端应用已处理（边界③）。

3. **需要"对方确实收到并处理了这笔快照"时，加应用层 ACK**
   - 在快照消息里带唯一 id；对端处理完成后回复 ACK。
   - 本端在收到对应 ACK 后再认为这笔快照交付完成；若要在未确认时关闭，可先等 ACK（或带超时），再 `close()`。
   - 不要用 close 帧的回包当作业务消息的送达确认。

4. **关闭时序**
   - 若希望"快照尽量送达再关"：调用 `send(snapshot, cb)` 后，在 `cb`（边界②）里或之后再 `close()`；但仍需注意边界②到③的差距，关键数据用应用层 ACK。
   - `close()` 之后 `readyState` 立即变为 `CLOSING(2)`，此时再 `send()` 会走 [sendAfterClose](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1138-L1159)，回调在下个 tick 收到 `WebSocket is not open` 错误。所以必须在 `close()` 之前完成所有 `send()` 调用。
   - 注意默认 30 秒关闭超时（`closeTimeout`）。若对端不回 close，到时会被 `socket.destroy()`，此时仍停留在内核缓冲但未被对端确认的数据可能丢失。

5. **确认压缩确实生效**
   - 检查 `ws.extensions` 是否包含 `permessage-deflate`；未协商成功时 `compress` 选项被忽略，消息明文发送，"压缩大快照"的现场判断前提可能不成立。

---

## 6. 涉及的关键代码位置

- 发送入口与压缩开关：[websocket.js send()](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L455-L485)
- 优雅关闭：[websocket.js close()](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L302-L340)
- 强制关闭：[websocket.js terminate()](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L492-L504)
- 关闭后发送处理：[sendAfterClose](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1138-L1159)
- bufferedAmount：[websocket.js#L120-L124](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L120-L124)
- Sender 队列与状态机：[sender.js 构造/字段](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L38-L56)
- 压缩分发与 socket 销毁检测：[dispatch](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L503-L529)
- 入队/出队：[enqueue/dequeue](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L536-L554)
- 写 socket 与回调绑定：[sendFrame](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L563-L572)
- 队列错误回调：[callCallbacks](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L585-L594)
- close 帧排队：[sender.js close()](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L184-L229)
- 异步压缩与并发限流：[permessage-deflate.js compress()](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L322-L329) / [_compress()](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L404-L460)
- 关闭时压缩流清理：[cleanup()](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L130-L150)
- 服务端扩展协商：[websocket-server.js handleUpgrade](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket-server.js#L298-L321)
