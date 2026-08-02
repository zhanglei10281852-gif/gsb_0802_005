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
5. **对端先关闭也分两种：** 对端优雅 `close()`（场景 F/G）不会丢弃本地正在压缩/排队的数据——收到对端 close 只是让本地回一个 close 帧（同样排队），接收端也只丢弃它自己 close 帧**之后**到达的字节；对端 `terminate()`/RST（场景 H）会销毁本地 socket，压缩中的消息回调报错、不会发出。
6. **台账可记的只有"本机已接收"和"本机已发出"两种状态；"对端已送达"只能来自应用层 ACK。** 详见 3.1 记账矩阵与 3.2 时序对照表。

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

### 3.1 投递台账记账矩阵（接入团队直接照此记账）

把发送回调接到台账时，只有下面两种状态与本库实际能观测的信号一一对应：

| 台账状态 | 何时可记 | 依据的信号 | 绝不能写成 |
|----------|----------|------------|------------|
| **本机已接收（已被发送层接管）** | `ws.send()` 同步返回后即可记 | 调用正常返回，未抛 `WebSocket is not open`；此时 `Sender._state` 通常为 `DEFLATING` 或任务已入 `_queue`，`ws.bufferedAmount` 增加 | 不能写成"已发送/已送达"。此时进程崩溃或 `terminate()` 都会丢。 |
| **本机已发出（已写入本地 socket）** | `send(data, cb)` 的 **cb 被调用且 err 为 null** 时记 | `socket.write` 回调无错；`ws.bufferedAmount` 已相应下降 | **绝不能写成"对端已送达/对端已收到"。** 回调只证明字节离开 Node 进入本机内核，对端可能还没读、没解析、没处理。 |

回调的另外两种结果：

- **cb 收到 Error**：这笔消息**未到达边界②**，应记为"发送失败/未送达"。典型错误文案：
  - `"The socket was closed while data was being compressed"`（场景 C/H：本地或对端在压缩期间销毁了 socket）。
  - `"WebSocket is not open: readyState 2/3 (CLOSING/CLOSED)"`（在 `close()` 之后才调用 `send()`，走 [sendAfterClose](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1138-L1159)）。
  - `ECONNRESET`/`EPIPE` 等系统错误（数据可能已过边界②但连接随后被重置）。
- **cb 从未被调用**：消息停留在边界①之后、边界②之前（仍在压缩或排队）。此时连接最终是优雅关闭则 cb 随后会以无错触发；是异常销毁则以错误触发。不要在 cb 触发前假设任何结果。

**台账里的"对端已送达"只能由应用层 ACK 驱动**，不能由任何 ws 回调或 `'close'` 事件驱动。下一节的时序对照表把每种关闭方式下，排队中/压缩中/已写 socket 的消息分别会怎样列清楚。

### 3.2 各关闭时序下的消息命运对照表

下表的"在压缩中"指数据已进入 `dispatch` 但 zlib 回调尚未返回；"在队列中"指排在 `_queue` 里等待；"已写 socket"指已过边界②。

| 关闭时序 | 在压缩中的数据 | 在队列中的数据 | 已写 socket 的数据 | `send` 回调结果 | 对端能否收到 message |
|----------|----------------|----------------|--------------------|-----------------|---------------------|
| **本地 `close()`（优雅，场景 A/B/D/G）** | 压缩继续，完成后照常组帧、写 socket | 按 FIFO 依次发送，close 帧排最后 | 留在内核缓冲并被 `socket.end()` 冲刷 | 全部无错（err=null） | 通常能（字节先于 close/FIN 到达）；但是否被对端应用处理属边界③，库不保证 |
| **本地 `terminate()`（场景 C）** | 立即中断；压缩回调检测到 `socket.destroyed`，**不写 socket** | 队列里其余任务被 [callCallbacks](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L585-L594) 全部以错误回调 | 可能已发部分，但 `socket.destroy()` 发 RST，在途字节可能被丢弃 | 当前消息及队列回调均收到 `"The socket was closed while data was being compressed"` | 不能保证；已发部分也可能因 RST 丢失，对端 close code=1006 |
| **对端优雅 `close()`，本地未关（场景 F）** | 压缩继续；对端 close 触发本地回 close 帧并入队 | 按 FIFO 发送，回 close 帧排最后 | 冲刷发出 | 无错 | 对端能收到 close 帧之前已到达的数据帧；之后的字节被 receiver 丢弃 |
| **双方同时优雅 `close()`（场景 G）** | 压缩继续，完成后发送 | 按 FIFO 发送 | `socket.end()` 冲刷 | 无错 | 同上，通常能收到在途数据；边界③不保证 |
| **对端 `terminate()`/RST（场景 H）** | 本地 socket 被 RST 销毁；压缩回调检测到 destroyed，**不写 socket** | 队列任务以错误回调 | 若 RST 在 write 之后到达，回调可能无错也可能报 `ECONNRESET`（依平台/时序） | 当前消息通常报错；已写部分结果不确定 | 不能保证，对端 close code=1006 |

需要强调两点（结论不随新场景改变）：

1. **回调成功永远不等于对端收到。** 即便在 F/G 这种"对端确实收到了"的回环实验里，回调成功的语义也只是边界②；跨机器高延迟、对端慢消费或 RST 时，回调成功与对端 message 之间没有因果保证。
2. **优雅关闭（无论哪一方发起）不主动丢弃发送队列里的数据；强制终止才会。** 这是代码里 `close()`→`sender.close()`→入队 与 `terminate()`→`socket.destroy()` 两条路径的本质区别。

---

## 4. 复现场景与观察结果

脚本 [test/close-during-compression-repro.js](file:///e:/newGsb/questions/GSB-005/Tony/test/close-during-compression-repro.js) 只用仓库自身的 `WebSocket`/`WebSocketServer` 与 Node 内置模块，通过包装（非修改）`Sender.dispatch`/`sendFrame` 打点，启动真实本地 server/client。共 8 个场景（A–E 覆盖本地关闭方向，F–H 覆盖对端关闭方向）。

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

### 场景 F：对端在本地压缩尚未结束时优雅关闭（本地未调用 close）

本地（服务端）发出 16 MiB 不可压缩快照后，**不调用** `close()`；对端（客户端）在压缩进行中（约 5ms 后）调用 `close(1000)`。

```
SERVER send() returned; sender.state=1 (DEFLATING). PEER will now close() ...
CLIENT calling close() while server compresses
SERVER state right before client close arrives: sender.state=1 queueLen=1
       closeFrameReceived=true                       # 对端 close 帧在压缩完成前就到了
... (压缩继续，约 560ms)
SERVER -> sendFrame opcode=2 rsv1=1 frameBytes=16782342
SERVER -> sendFrame opcode=8 ...                     # 收到对端 close 后，本地自动回 close
SERVER    socket.write CB opcode=2 err=null
SERVER data send() CALLBACK: ok; readyState=2 closeFrameSent=false closeFrameReceived=true
SERVER    socket.write CB opcode=8 err=null
SERVER 'close' EVENT code=1000 reason=peer closing
CLIENT RECEIVED message bytes=16777216               # 对端仍然收到了数据
CLIENT 'close' EVENT code=1000 reason=peer closing
RESULT: server data callback=ok; client received 1 data frame(s); both closed
```

为什么会这样（实现机制）：

1. 对端 close 帧到达后，[receiverOnConclude](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1168-L1182) 置 `_closeFrameReceived=true`，并**自动调用本地 `ws.close()`** 回一个 close 帧。
2. 此刻本地 Sender 仍处于 `DEFLATING`，所以这个回 close 帧被 `enqueue` 到正在压缩的数据帧**之后**（`queueLen=1`），不会打断压缩。
3. 压缩完成后，数据帧先 `sendFrame`，随后回 close 帧；两个写入都成功，`send` 回调无错。
4. 对端的 [Receiver](file:///e:/newGsb/questions/GSB-005/Tony/lib/receiver.js#L96-L98) 在解析完自己的 close 帧后，只**丢弃 close 帧之后**到达的字节（`_opcode === 0x08` 时直接 `cb()` 不再解析）。由于本地数据帧在网络上**先于**对端自己发出的 close 帧到达（对端 close 是在压缩期间才发出，而本地数据帧在压缩完成后才发出——在本机回环场景里二者顺序仍由 TCP 保证），所以对端照常收到并 emit `message`。

结论：**"对端先关"不等于本地排队/压缩中的数据会被丢弃。** 只要是优雅关闭、且数据字节在对端 close 帧之前进入对端内核，对端就会投递该 message。但这一点依赖网络字节序与对端读取时机，跨机器/高延迟下并非强保证（见第 4.7 节）。

### 场景 G：双方在压缩进行中同时优雅关闭

与 F 类似，但本地也在约 5ms 时调用 `close(1001)`，形成双方几乎同时关闭。

```
SERVER send() returned; sender.state=1. BOTH sides will close() ...
SERVER calling close() / CLIENT calling close()
... (压缩继续)
SERVER -> sendFrame opcode=2 ... frameBytes=16782342
SERVER -> sendFrame opcode=8 ... frameBytes=19
SERVER    socket.write CB opcode=2 err=null
SERVER data send() CALLBACK: ok
SERVER    socket.write CB opcode=8 err=null
SERVER 'close' EVENT code=1000
CLIENT RECEIVED message bytes=16777216
CLIENT 'close' EVENT code=1001 reason=server going away
RESULT: server data callback=ok; client received 1 data frame(s)
```

为什么会这样：

1. 本地 `close()` 把本地 close 帧排进队列；对端 close 帧到达后又触发一次 `close()`，但 [close()](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L310-L319) 在 `CLOSING` 状态下是幂等的：当 `_closeFrameSent && _closeFrameReceived` 时直接 `socket.end()`。
2. 关键在 close 帧写出回调：[websocket.js#L327-L337](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L327-L337)。当回 close 帧写完（`_closeFrameSent=true`）且 `_closeFrameReceived=true` 时，立即 `socket.end()`。`socket.end()` 是**半关闭写侧**，它会先把内核发送缓冲里已有的字节冲刷出去再发 FIN，因此数据帧不丢、`send` 回调无错。
3. 对端同样：先读到数据帧并 emit `message`，再读到 close 帧。

结论：双方同时优雅关闭时，**已进入发送队列/已写 socket 的数据仍被冲刷，回调成功**；但"对端应用是否处理"仍属边界③，库不保证。这进一步印证了"回调成功 ≠ 对端收到"——这里回调成功只是因为内核成功接收了字节。

### 场景 H：对端在压缩进行中强制终止（RST）

对端在压缩中调用 `terminate()`，直接 `socket.destroy()`，这会向本地发送 TCP RST。

```
SERVER send() returned; sender.state=1. PEER will terminate() (RST) ...
CLIENT calling terminate() while server compresses
SERVER data send() CALLBACK: error: The socket was closed while data was being compressed;
       socketDestroyed=true
SERVER 'close' EVENT code=1006
CLIENT 'close' EVENT code=1006
RESULT: server data callback=error; server error event=false; client received 0 data frame(s)
```

为什么会这样：

1. 对端 RST 使本地 socket 立即 `destroyed`。这里 socket 的 `'close'`/`'error'` 事件先于压缩回调触发（注意：此时 Sender 仍在 `DEFLATING`，队列里还没有本地 close 帧，因为本地没调用 close）。
2. 压缩完成回调在 [dispatch](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L513-L521) 里检测到 `this._socket.destroyed`，构造 `"The socket was closed while data was being compressed"` 错误，交给 `send` 回调，并**不调用 `sendFrame`**，数据帧彻底不发。
3. 对端没有收到任何数据帧（`client received 0`），双方 close code 都是 1006（异常关闭）。
4. 本场景里没有额外的 `'error'` 事件：是因为错误通过 `send` 回调返回，且 socket 销毁走的是 close 路径。是否会额外 emit `'error'` 取决于 RST 被 Node 报告为 socket `'error'` 还是仅 `'close'`，存在时序/平台差异（本环境 Windows + Node 22 表现为只走 close）。

结论：**对端强制终止是真正可能丢消息的情形。** 此时"已被发送层接收"（边界①）的数据既到不了边界②也到不了边界③，`send` 回调以错误返回是台账判定"未送达"的可靠信号。

> 时序提示：若数据已经 `sendFrame`/`socket.write`（已过边界②）之后 RST 才到达，回调是否报错取决于操作系统何时把 RST 通知给本次 write——可能是回调成功但随后 socket `'error'`/`'close'`(1006)，也可能是回调直接收到 `ECONNRESET`。因此**回调成功仍不能在异常关闭场景下作为送达凭证**。

### 4.7 对端关闭方向的两个事实（解释"为何这样设计"）

1. **接收端只丢弃自己 close 帧之后的字节**：[receiver.js#L97](file:///e:/newGsb/questions/GSB-005/Tony/lib/receiver.js#L96-L98)。这是为了让 close 帧之前已在途的数据帧能够被完整投递，符合 RFC 6455 "close 帧表示连接关闭起点"但不追溯丢弃已收数据的语义。
2. **发送端不因为收到对端 close 就清空发送队列**：对端 close 只是触发本地回一个 close 帧（同样排队），不会取消前面排队的数据。[socketOnEnd](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1384-L1390) 里的 `this.end()` 也只在 receiver.end() 之后半关闭写侧，会冲刷已缓冲的发送数据。

这两点合起来解释了 F/G 中"对端关了，本地数据照样发出去并被收到"。但它们都只在**优雅关闭**且**字节序有利**时成立；RST（H）或对端在数据到达前就停止读取并销毁，都会使数据丢失。

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

6. **对端发起关闭时的处理（本轮新增）**
   - 收到对端优雅 `close()` 时，本库会自动回 close 帧并继续把发送队列里的数据冲刷出去（场景 F/G）。不要因为收到了对端 close 就假设"本地刚发的快照丢了"——应仍以 `send` 回调判断边界②，以应用层 ACK 判断边界③。
   - 若 socket 报 `'error'`（如 `ECONNRESET`）或 `'close'` 的 code 为 1006，说明对端是异常断开（场景 H）。此时压缩中/队列中的消息可能未发出，已写 socket 的也可能丢失，台账中这些消息应回退为"未确认/可能丢失"，等待业务层对账或重发，而不是根据之前是否回调成功来记"已送达"。
   - 记账状态机建议：`send()` 返回 → "本机已接收"；回调无错 → "本机已发出"；回调有错或异常 close(1006) → "发送失败/未确认"；收到应用层 ACK → "对端已送达"。前三个状态都不能跃迁到最后一个。

---

## 6. 发布评审结论（高扇出网关：压缩开关、台账、监控）

本节把前两轮已验证的发送边界与关闭/压缩时序收敛成可直接评审的决策依据。所有结论都只基于现有实现，不修改库、不包装 API、不伪造确认。

### 6.1 客户端与服务端的默认协商行为

压缩是否生效取决于**两端都开启**并通过 HTTP 握手协商，任一端关闭即不压缩：

| 角色 | 配置项 | 默认值 | 行为 |
|------|--------|--------|------|
| 客户端 | `perMessageDeflate` | `true`（[initAsClient](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L667-L689)） | 默认在握手头里带 `permessage-deflate` 要约（[websocket.js#L769-L778](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L769-L778)） |
| 服务端 | `perMessageDeflate` | `false`（[WebSocketServer](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket-server.js#L69-L89)） | **默认不接受压缩**；只有显式传 `true` 或配置对象才会解析并接受客户端要约（[websocket-server.js#L298-L321](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket-server.js#L298-L321)） |

含义：

- 只有当服务端显式启用（如 `new WebSocketServer({ perMessageDeflate: true })`）时，连接才会协商出 `permessage-deflate`。本调查的所有压缩场景都据此配置。
- 协商成功后，`ws.extensions` 字符串包含 `permessage-deflate`；否则 `send()` 的 `compress` 选项被强制置 `false`（[websocket.js#L480-L484](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L480-L484)），消息明文发送。
- **监控/台账上线前应先确认线上服务端确实启用了压缩**，否则"压缩大快照"这一前提不成立，第 4 节里压缩带来的异步延迟窗口也不会出现。

### 6.2 影响资源与排队表现的配置

| 配置 | 默认值 | 对资源/排队的影响 | 高扇出注意事项 |
|------|--------|------------------|----------------|
| `perMessageDeflate.concurrencyLimit` | `10`（[permessage-deflate.js#L65-L71](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L65-L71)） | **进程级全局** zlib 任务并发上限（`zlibLimiter` 是单例，跨所有连接共享），限制同时跑在 libuv 线程池上的压缩/解压任务数 | 高扇出下，N 个连接的大快照压缩会在这个全局队列里排队；超过 10 的任务等待，直接放大场景 B/F/G 里"压缩中"的时间窗口。这是 CPU/内存与排队延迟的主要来源 |
| `perMessageDeflate.threshold` | `1024` | 仅在对应方向 `no_context_takeover` 生效时，小于该字节的消息不压缩（[sender.js#L371-L385](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L371-L385)） | 默认协商**不**带 `no_context_takeover`，因此该阈值默认不拦压缩；不要误以为"小消息不压缩" |
| `server/client_no_context_takeover` | 未设置（即允许上下文接管） | 开启后每条消息重置 deflate/inflate 上下文，压缩率略降但释放历史缓冲 | 影响内存占用与压缩率，不改变关闭/排队语义 |
| `zlibDeflateOptions` / `zlibInflateOptions` | 未设置 | 透传给 `zlib.createDeflateRaw/InflateRaw`，含 `level`/`memLevel`/`chunkSize` 等 | 可调压缩等级与内存；压缩等级越高，CPU 时间越长，异步窗口越大 |
| `closeTimeout` | `30000`（[constants.js#L10](file:///e:/newGsb/questions/GSB-005/Tony/lib/constants.js#L10)） | 优雅关闭后等对端回 close 的超时，到时 `socket.destroy()` | 对端不响应时，已写内核但未确认的数据最长挂 30s 后可能被丢弃 |
| `maxPayload` | 客户端 100 MiB，服务端 100 MiB | 接收端解压后消息大小上限，超限以 1009 关闭（[receiver.js#L439-L457](file:///e:/newGsb/questions/GSB-005/Tony/lib/receiver.js#L439-L457)、[permessage-deflate.js#L485-L506](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L485-L506)） | 防止单个大快照解压耗尽接收端内存；与发送端排队无关 |
| `maxBufferedChunks` / `maxFragments` | 262144 块 / 16384 片（客户端，[websocket.js#L673-L675](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L673-L675)），262144 块 / 16384 片（服务端，[websocket-server.js#L72-L74](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket-server.js#L72-L74)） | 接收端缓冲块/分片数量上限，超限报错关闭 | 接收侧背压保护（计的是块数不是字节数），不影响发送队列 |
| `highWaterMark`（底层 socket，由 Node/net 决定） | net.Socket 默认 16 KiB（可通过自定义 `createConnection` 调整） | 决定 `socket.write` 何时返回 false 产生背压，以及 `_writableState.length` 的规模 | 慢消费者时 Node 侧写缓冲增长程度；内核发送/接收缓冲另由 OS 控制 |

资源语义要点：

1. **压缩/解压共享全局并发**：`zlibLimiter` 不是每个连接一个，而是进程内所有 `PerMessageDeflate` 实例共享（[permessage-deflate.js#L24](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L24) 注释明确是为避免全局线程池内存碎片）。高扇出+大快照时，压缩排队是全局性的，单连接的大消息会拖慢其他连接的压缩。注意：该限流器在**首个** `PerMessageDeflate` 实例构造时按其 `concurrencyLimit` 懒初始化（[permessage-deflate.js#L65-L71](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L65-L71)），之后的实例不会重置它；因此不同连接若传了不同的 `concurrencyLimit`，实际生效的是进程里第一个实例的值。评审配置时应全进程统一设置。
2. **发送队列在每个连接的 Sender 内**：`_queue`/`_bufferedBytes` 是每连接的（[sender.js#L51-L53](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L51-L53)），但 `_bufferedBytes` 计的是**未压缩**字节数。压缩完成前，`bufferedAmount` 反映原始大小；压缩后才组帧写 socket。
3. **关闭不取消压缩任务**：无论本地还是对端优雅关闭，已经进入 zlib 的任务会跑完（场景 A/B/D/F/G 均观察到压缩继续并最终发送）。

### 6.3 只能说明本机进度、不能代表交付完成的信号

| 信号 | 能说明的本机进度 | 绝不能据此认定的事 |
|------|------------------|--------------------|
| `ws.send()` 正常返回 | 边界①：发送层已接收，任务进入压缩或队列 | 不能认定已发送、更不能认定对端收到 |
| `send(data, cb)` 回调 `err===null` | 边界②：帧已 `socket.write`、字节交给本机内核 | **不能认定对端收到/处理**（场景 E 实测回调后 200ms 对端才 message；F/G 回调成功也不构成跨机器保证） |
| `ws.bufferedAmount` 下降或归零 | 本机 Sender 队列/socket 写缓冲已排空 | 不代表对端已读；它混合 socket 与 Sender 两种口径（[websocket.js#L120-L124](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L120-L124)） |
| 本端 `'close'` 事件触发（code≠1006） | 本端关闭握手完成、socket 已关闭 | 不代表之前发的业务消息被对端应用处理 |
| `closeFrameSent=true` | close 帧已写入本机 socket | 只代表关闭帧离开本机，不代表数据帧被对端消费 |
| socket 的 `'finish'`/`end` 事件 | 本机写侧已冲刷完毕 | 仍是本机信号，不保证对端应用收到 |
| 对端 TCP ACK（内核层，应用不可见） | 字节到达对端内核 | 不代表对端 WebSocket 解析完成或业务处理 |

**唯一能代表"交付完成"的信号是应用层 ACK**：对端业务处理完该快照后回一条带消息 id 的确认。这不在 ws 库职责内，需要网关协议自行定义；本调查不提供也不建议伪造该确认。

### 6.4 是否继续启用压缩（决策建议）

以下建议基于本仓库行为，供负责人结合业务数据拍板，不是对库的修改要求：

**建议：按连接/消息类型有条件启用，而非在高扇出网关上全局无条件启用。**

适用条件（启用压缩收益明确）：

- 出向消息**可压缩率高**（如结构化行情快照、JSON、重复字段多），带宽成本是瓶颈。
- 单连接消息量大、扇出数可控，或大快照不频繁。
- 能接受压缩带来的异步窗口（场景 B：16 MiB 不可压缩数据压缩耗时数百毫秒），并已用应用层 ACK 兜底交付确认。

不建议启用 / 应关闭压缩的情形：

- 消息本身**已压缩或近随机**（如 protobuf+gzip、加密后二进制、图像）。压缩耗时和 CPU 开销几乎不减小体积（场景 B/H 中 16 MiB 随机数据压到 16.78 MiB，几乎无收益却占用全局 zlib 配额）。
- **超高扇出 + 大快照广播**：全局 zlibLimiter（并发 10）会成为所有连接共享的瓶颈，放大关闭/压缩重叠窗口和内存占用（`_bufferedBytes` 持有的是未压缩原始数据）。
- 对关闭时序敏感、要求"发完即确定送达"的场景——但注意：即便关闭压缩也无法提供对端送达保证，这必须靠应用层 ACK，而非关压缩解决。

落地手段（都是公开配置，不碰 `lib/`）：

- 服务端按业务决定 `perMessageDeflate: true | false | { ... }`。
- 对需要压缩的连接用 `perMessageDeflate: { threshold, concurrencyLimit, zlibDeflateOptions: { level } }` 调优；`concurrencyLimit` 是进程级，按机器 CPU 核数与扇出规模评估。
- 对单条消息可用 `ws.send(data, { compress: false })` 对不可压缩/低价值消息明文发送（扩展已协商时仍可逐消息关闭压缩）。
- 无论是否压缩，关键快照都带应用层 ACK 与消息 id。

### 6.5 台账与监控应该观察什么

台账状态机（与 3.1 一致，评审口径）：

```
send() 返回
  └─[本机已接收]─→ send 回调无错
                     └─[本机已发出]─→ 应用层 ACK
                                        └─[对端已送达]
send 回调有错 / close(1006) / socket error
  └─[发送失败/未确认]（需对账或重发）
```

监控指标建议（均可通过公开属性/事件获取，不包装内部 API）：

- `ws.bufferedAmount`：本机待发字节趋势，用于背压告警。注意它是未压缩口径与 socket 口径之和，只作本机进度参考。
- `send` 回调错误率，区分错误类型：
  - `"The socket was closed while data was being compressed"`（压缩期间被关闭/重置，场景 C/H 实测）。
  - `"WebSocket is not open: readyState 2/3 (CLOSING/CLOSED)"`（在 `close()` 之后才调用 `send()`，由 [sendAfterClose](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1138-L1159) 在下个 tick 回调；这是接入代码时序 bug 的信号）。
  - `ECONNRESET`/`EPIPE`（对端异常，可能已过边界②才报错）。
- `'close'` 事件的 code：1006 表示异常关闭，对应连接上"本机已发出但未确认"的消息应全部回退为未确认。
- `'error'` 事件计数。
- 应用层 ACK 延迟与未确认数：这才是交付质量指标。
- 进程级 zlib 排队无法直接从公开 API 读取，但可通过"send() 返回 → 回调无错"的耗时分布（即边界①到②的延迟）间接观测压缩+全局排队耗时。

### 6.6 剩余风险（现有实现无法消除，评审须知）

1. **无单条消息送达确认**：WebSocket/TCP 不向应用暴露"对端已处理"。回调成功只到边界②，这是设计事实，不是缺陷。
2. **边界②到③之间的丢失窗口**：回调成功后到对端 message 之前，若发生 RST、进程崩溃、机器掉电，数据可能丢失（场景 E 演示了时间差，场景 H 演示了 RST）。
3. **全局 zlib 并发瓶颈**：高扇出下压缩任务在进程级队列排队，单连接大快照影响全局，且压缩中持有未压缩原始数据，内存峰值与扇出×消息大小相关。
4. **优雅关闭不等于全部送达**：`close()` 保证数据帧先于 close 帧离开本机、内核会冲刷，但不保证对端应用在关闭前处理；`closeTimeout`（默认 30s）到时强制 destroy 也可能丢弃在途数据。
5. **平台/时序差异**：RST 是否反映为 `'error'` 事件、回调是否收到 `ECONNRESET`，随操作系统与 Node 版本不同（场景 H 在 Windows+Node 22 上只触发 close(1006)，未额外 emit error）。台账不应依赖某个特定错误文案/事件组合。
6. **对端先优雅关闭时的数据投递依赖字节序**：场景 F/G 在本机回环中对端收到了数据，但跨机器高延迟下，若对端 close 先于数据帧到达对端内核，receiver 会丢弃 close 之后的字节（[receiver.js#L97](file:///e:/newGsb/questions/GSB-005/Tony/lib/receiver.js#L97)）。这属于 TCP 字节序与对端读取时机，本库不保证、也无法保证业务消息一定先到。

### 6.7 本地证据索引

| 结论 | 证据 |
|------|------|
| 优雅 close 排队、不丢在压数据 | 场景 A/B/D；[sender.close()](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L224-L228)、[dispatch→enqueue](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L409-L413) |
| 压缩是异步的，send 返回时可能仍在 DEFLATING | 场景 B（300ms 仍 state=1）；[dispatch](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L511-L528)、[compress](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L322-L329) |
| terminate 中断压缩、回调报错、数据不发 | 场景 C/H；[websocket.terminate()](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L500-L503)、[dispatch destroyed 检测](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L514-L521) |
| 回调成功 ≠ 对端收到（边界②③差距） | 场景 E（回调后 200ms 才 message）；[sendFrame](file:///e:/newGsb/questions/GSB-005/Tony/lib/sender.js#L563-L572) |
| 对端优雅 close 不丢弃本地在压数据 | 场景 F/G；[receiverOnConclude](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1168-L1182)、[receiver _write 丢弃规则](file:///e:/newGsb/questions/GSB-005/Tony/lib/receiver.js#L96-L98) |
| 对端 RST 导致异常关闭、可能丢消息 | 场景 H（双方 1006，回调报错）；[socketOnClose](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1321-L1365) |
| close 后再 send 立即报错 | [sendAfterClose](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1138-L1159) |
| 全局 zlib 并发限制 | [permessage-deflate.js#L24](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L24)、[L65-L71](file:///e:/newGsb/questions/GSB-005/Tony/lib/permessage-deflate.js#L65-L71)、[limiter.js](file:///e:/newGsb/questions/GSB-005/Tony/lib/limiter.js) |
| 服务端默认不压缩、客户端默认要约压缩 | [websocket-server.js#L76](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket-server.js#L69-L89)、[websocket.js#L677](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L667-L689) |

复现命令：`node test/close-during-compression-repro.js`（8 个场景，零新依赖，不修改 `lib/`）。

---

## 7. 涉及的关键代码位置

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
- 收到对端 close 帧的处理：[receiverOnConclude](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1168-L1182)
- 接收端丢弃 close 帧之后的字节：[receiver.js _write()](file:///e:/newGsb/questions/GSB-005/Tony/lib/receiver.js#L96-L98)
- 接收端 control 帧解析与 conclude 触发：[receiver.js controlMessage()](file:///e:/newGsb/questions/GSB-005/Tony/lib/receiver.js#L653-L701)
- 对端 FIN 时半关闭并冲刷发送数据：[socketOnEnd](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1384-L1390)
- socket 关闭时排空缓冲并结束 receiver：[socketOnClose](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1321-L1365)
- Sender 出错时 end() 而非 destroy()：[senderOnError](file:///e:/newGsb/questions/GSB-005/Tony/lib/websocket.js#L1281-L1301)
