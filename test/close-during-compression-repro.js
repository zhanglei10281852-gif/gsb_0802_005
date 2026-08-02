'use strict';

const WebSocket = require('..');
const { WebSocketServer } = WebSocket;
const { randomFillSync } = require('crypto');

const LARGE_SIZE = 2 * 1024 * 1024;
const HUGE_SIZE = 16 * 1024 * 1024;

let scenarioIndex = 0;

function makePayload(size) {
  const buf = Buffer.allocUnsafe(size);
  for (let i = 0; i < buf.length; i++) buf[i] = (i * 31 + 7) & 0xff;
  return buf;
}

function makeIncompressiblePayload(size) {
  const buf = Buffer.allocUnsafe(size);
  randomFillSync(buf);
  return buf;
}

function ts() {
  const t = process.hrtime.bigint() / 1000000n;
  return Number(t).toString().padStart(6, ' ');
}

function log(label, msg) {
  console.log(`[${ts()} ms][${label}] ${msg}`);
}

function attachProbes(ws, label) {
  const sender = ws._sender;
  const socket = ws._socket;

  const origDispatch = sender.dispatch.bind(sender);
  sender.dispatch = function (data, compress, options, cb) {
    const opcode = options.opcode;
    const isData = opcode === 1 || opcode === 2;
    if (isData || opcode === 0x08) {
      log(
        label,
        `dispatch enter opcode=${opcode} compress=${compress} ` +
          `state=${sender._state} queueLen=${sender._queue.length} ` +
          `bufferedBytes(sender)=${sender._bufferedBytes}`
      );
    }
    return origDispatch(data, compress, options, cb);
  };

  const origSendFrame = sender.sendFrame.bind(sender);
  sender.sendFrame = function (list, cb) {
    const first = list[0];
    const opcode = first[0] & 0x0f;
    const rsv1 = first[0] & 0x40;
    let payloadBytes = 0;
    for (let i = 1; i < list.length; i++) payloadBytes += list[i].length;

    log(
      label,
      `-> sendFrame opcode=${opcode} rsv1=${rsv1 ? 1 : 0} ` +
        `frameBytes=${payloadBytes} ` +
        `socketWritableLength(before)=${socket._writableState.length}`
    );

    let wrappedCb = cb;
    if (typeof cb === 'function') {
      wrappedCb = function (err) {
        log(
          label,
          `   socket.write CB opcode=${opcode} err=${
            err ? err.message : 'null'
          } ` +
            `socketWritableLength(after)=${socket._writableState.length} ` +
            `socketDestroyed=${socket.destroyed}`
        );
        cb(err);
      };
    }

    return origSendFrame(list, wrappedCb);
  };
}

function runScenario(name, fn) {
  scenarioIndex++;
  console.log('\n==================================================');
  console.log(`Scenario ${scenarioIndex}: ${name}`);
  console.log('==================================================');
  return new Promise((resolve, reject) => {
    let settled = false;
    const done = (err) => {
      if (settled) return;
      settled = true;
      if (err) reject(err);
      else resolve();
    };
    const timeout = setTimeout(
      () => done(new Error('scenario timed out')),
      8000
    );
    const finish = (err) => {
      clearTimeout(timeout);
      done(err);
    };
    fn(finish);
  });
}

function scenarioA(finish) {
  const wss = new WebSocketServer(
    { port: 0, perMessageDeflate: { threshold: 0 } },
    () => {
      const client = new WebSocket(`ws://localhost:${wss.address().port}`, {
        perMessageDeflate: true
      });

      let serverWs;
      let receivedMessages = 0;
      let gotClose = false;

      wss.on('connection', (ws) => {
        serverWs = ws;
        attachProbes(ws, 'SERVER');

        const payload = makePayload(LARGE_SIZE);
        log('SERVER', `send() large snapshot, payloadBytes=${payload.length}`);

        ws.send(payload, { binary: true }, (err) => {
          log(
            'SERVER',
            `send() CALLBACK err=${err ? err.message : 'null'} ` +
              `readyState=${ws.readyState} bufferedAmount=${ws.bufferedAmount}`
          );
        });

        log(
          'SERVER',
          `send() returned; sender.state=${ws._sender._state} ` +
            `sender.bufferedBytes=${ws._sender._bufferedBytes} ` +
            `bufferedAmount=${ws.bufferedAmount}`
        );

        log('SERVER', 'calling close() immediately after send()');
        ws.close(1000, 'graceful');
        log(
          'SERVER',
          `close() returned; readyState=${ws.readyState} ` +
            `sender.state=${ws._sender._state} ` +
            `queueLen=${ws._sender._queue.length} ` +
            `closeFrameSent=${ws._closeFrameSent}`
        );
      });

      client.on('message', (data) => {
        receivedMessages++;
        log(
          'CLIENT',
          `RECEIVED message #${receivedMessages} bytes=${data.length}`
        );
      });
      client.on('close', (code, reason) => {
        gotClose = true;
        log(
          'CLIENT',
          `RECEIVED close code=${code} reason=${reason.toString()}`
        );
      });
      client.on('open', () => log('CLIENT', 'open'));

      const check = setInterval(() => {
        if (serverWs && gotClose && receivedMessages >= 1) {
          clearInterval(check);
          log(
            'RESULT',
            `client got message(s)=${receivedMessages} closeFrame=yes; ` +
              `server closeFrameSent=${serverWs._closeFrameSent}`
          );
          wss.close(() => {
            client.terminate();
            finish();
          });
        }
      }, 30);
    }
  );

  wss.on('error', finish);
}

function scenarioB(finish) {
  const wss = new WebSocketServer(
    { port: 0, perMessageDeflate: { threshold: 0 } },
    () => {
      const client = new WebSocket(`ws://localhost:${wss.address().port}`, {
        perMessageDeflate: true
      });

      let serverWs;
      let receivedMessages = 0;
      let gotClose = false;
      let sendCallbackFired = false;

      client.on('upgrade', () => {
        process.nextTick(() => {
          client._socket.pause();
          log('CLIENT', 'paused client socket (not reading from kernel)');
        });
      });

      wss.on('connection', (ws) => {
        serverWs = ws;
        attachProbes(ws, 'SERVER');

        const payload = makeIncompressiblePayload(HUGE_SIZE);
        log(
          'SERVER',
          `send() large incompressible snapshot, payloadBytes=${payload.length}`
        );

        ws.send(payload, { binary: true }, (err) => {
          sendCallbackFired = true;
          log(
            'SERVER',
            `send() CALLBACK err=${err ? err.message : 'null'} ` +
              `readyState=${ws.readyState} socketWritableLength=${ws._socket._writableState.length}`
          );
        });

        log('SERVER', 'calling close()');
        ws.close(1000, 'graceful');
        log(
          'SERVER',
          `after close: sender.state=${ws._sender._state} ` +
            `queueLen=${ws._sender._queue.length} ` +
            `socketWritableLength=${ws._socket._writableState.length}`
        );

        setTimeout(() => {
          log(
            'SERVER',
            `300ms later: sender.state=${ws._sender._state} ` +
              `queueLen=${ws._sender._queue.length} ` +
              `socketWritableLength=${ws._socket._writableState.length} ` +
              `closeFrameSent=${ws._closeFrameSent} readyState=${ws.readyState} ` +
              `sendCallbackFired=${sendCallbackFired}`
          );
          log(
            'RESULT',
            `300ms after send()+close(): data is STILL being compressed ` +
              `(state=DEFLATING); close frame sits in sender queue behind it; ` +
              `nothing has been framed or written to the socket yet. ` +
              `The send() callback has not fired.`
          );
          log(
            'CLIENT',
            'resuming client socket so it can read once frames are sent'
          );
          client._socket.resume();
        }, 300);
      });

      client.on('message', (data) => {
        receivedMessages++;
        log(
          'CLIENT',
          `RECEIVED message #${receivedMessages} bytes=${data.length}`
        );
      });
      client.on('close', (code) => {
        gotClose = true;
        log('CLIENT', `RECEIVED close code=${code}`);
      });

      const check = setInterval(() => {
        if (serverWs && gotClose && receivedMessages >= 1) {
          clearInterval(check);
          wss.close(() => {
            client.terminate();
            finish();
          });
        }
      }, 30);
    }
  );

  wss.on('error', finish);
}

function scenarioC(finish) {
  const wss = new WebSocketServer(
    { port: 0, perMessageDeflate: { threshold: 0 } },
    () => {
      const client = new WebSocket(`ws://localhost:${wss.address().port}`, {
        perMessageDeflate: true
      });

      let receivedMessages = 0;
      let gotClose = false;
      let sendCallbackErrored = false;

      wss.on('connection', (ws) => {
        attachProbes(ws, 'SERVER');

        const payload = makePayload(LARGE_SIZE);
        log('SERVER', `send() large snapshot, payloadBytes=${payload.length}`);

        ws.send(payload, { binary: true }, (err) => {
          if (err) {
            sendCallbackErrored = true;
            log('SERVER', `send() CALLBACK ERROR: ${err.message}`);
          } else {
            log('SERVER', 'send() CALLBACK ok');
          }
        });

        log(
          'SERVER',
          `send() returned; sender.state=${ws._sender._state}. ` +
            `calling terminate() (NOT graceful)`
        );
        ws.terminate();
        log(
          'SERVER',
          `terminate() returned; socketDestroyed=${ws._socket.destroyed} ` +
            `readyState=${ws.readyState}`
        );
      });

      client.on('message', (data) => {
        receivedMessages++;
        log('CLIENT', `RECEIVED message bytes=${data.length}`);
      });
      client.on('close', (code) => {
        gotClose = true;
        log('CLIENT', `RECEIVED close code=${code}`);
      });

      setTimeout(() => {
        log(
          'RESULT',
          `terminate: sendCallbackErrored=${sendCallbackErrored} ` +
            `clientReceivedMessages=${receivedMessages} clientGotClose=${gotClose}`
        );
        wss.close(() => {
          client.terminate();
          finish();
        });
      }, 800);
    }
  );

  wss.on('error', finish);
}

function scenarioD(finish) {
  const wss = new WebSocketServer(
    { port: 0, perMessageDeflate: { threshold: 0 } },
    () => {
      const client = new WebSocket(`ws://localhost:${wss.address().port}`, {
        perMessageDeflate: true
      });

      let serverWs;
      const received = [];
      let gotClose = false;

      wss.on('connection', (ws) => {
        serverWs = ws;
        attachProbes(ws, 'SERVER');

        const payloads = [
          makePayload(512 * 1024),
          makePayload(512 * 1024),
          makePayload(512 * 1024)
        ];

        payloads.forEach((p, i) => {
          log('SERVER', `send() message #${i + 1} bytes=${p.length}`);
          ws.send(p, { binary: true }, (err) => {
            log(
              'SERVER',
              `send() #${i + 1} CALLBACK err=${err ? err.message : 'null'}`
            );
          });
        });

        log(
          'SERVER',
          `after 3 sends: sender.state=${ws._sender._state} ` +
            `queueLen=${ws._sender._queue.length} ` +
            `sender.bufferedBytes=${ws._sender._bufferedBytes}`
        );
        log('SERVER', 'calling close() while messages are queued/deflating');
        ws.close(1001, 'going away');
        log(
          'SERVER',
          `after close: sender.state=${ws._sender._state} ` +
            `queueLen=${ws._sender._queue.length} (close frame is last entry)`
        );
      });

      client.on('message', (data) => {
        received.push(data.length);
        log(
          'CLIENT',
          `RECEIVED message #${received.length} bytes=${data.length}`
        );
      });
      client.on('close', (code) => {
        gotClose = true;
        log('CLIENT', `RECEIVED close code=${code}`);
      });

      const check = setInterval(() => {
        if (serverWs && gotClose && received.length === 3) {
          clearInterval(check);
          log(
            'RESULT',
            `client received ${received.length} data frames before close; ` +
              `order preserved (data frames precede close frame)`
          );
          wss.close(() => {
            client.terminate();
            finish();
          });
        }
      }, 30);
    }
  );

  wss.on('error', finish);
}

function scenarioE(finish) {
  const wss = new WebSocketServer({ port: 0 }, () => {
    const client = new WebSocket(`ws://localhost:${wss.address().port}`);

    let serverWs;
    let receivedMessages = 0;
    let gotClose = false;
    let dataCallbackFired = false;
    let clientPaused = false;

    wss.on('connection', (ws) => {
      serverWs = ws;
      attachProbes(ws, 'SERVER');

      const startSending = () => {
        const payload = makeIncompressiblePayload(HUGE_SIZE);
        log(
          'SERVER',
          `send() large UNCOMPRESSED snapshot, payloadBytes=${payload.length} ` +
            `(compression disabled to isolate the socket buffer boundary)`
        );

        ws.send(payload, { binary: true, compress: false }, (err) => {
          dataCallbackFired = true;
          log(
            'SERVER',
            `data send() CALLBACK err=${err ? err.message : 'null'} ` +
              `socketWritableLength=${ws._socket._writableState.length} ` +
              `socketDestroyed=${ws._socket.destroyed}`
          );
          ws.close(1000, 'graceful');
          log(
            'SERVER',
            `called close() from within data callback; ` +
              `socketWritableLength=${ws._socket._writableState.length}`
          );
        });

        setTimeout(() => {
          log(
            'SERVER',
            `200ms later: dataCallbackFired=${dataCallbackFired} ` +
              `socketWritableLength=${ws._socket._writableState.length} ` +
              `clientMessagesReceived=${receivedMessages} ` +
              `closeFrameSent=${ws._closeFrameSent} readyState=${ws.readyState}`
          );
          log(
            'RESULT',
            `data was accepted by socket.write (callback fired = data left ` +
              `Node.js and is in the local socket/kernel send buffer), but ` +
              `the paused peer has NOT received it. This is the gap between ` +
              `"written to local socket" and "peer received". The close frame ` +
              `is queued after the data.`
          );
          log('CLIENT', 'resuming client socket to drain and observe delivery');
          client._socket.resume();
        }, 200);
      };

      if (clientPaused) {
        startSending();
      } else {
        client.once('paused', startSending);
      }
    });

    client.on('open', () => {
      client._socket.pause();
      clientPaused = true;
      log(
        'CLIENT',
        'paused client socket BEFORE server sends (slow consumer / backpressure)'
      );
      client.emit('paused');
    });

    client.on('message', (data) => {
      receivedMessages++;
      log(
        'CLIENT',
        `RECEIVED message #${receivedMessages} bytes=${data.length}`
      );
    });
    client.on('close', (code) => {
      gotClose = true;
      log('CLIENT', `RECEIVED close code=${code}`);
    });

    const check = setInterval(() => {
      if (serverWs && gotClose && receivedMessages >= 1) {
        clearInterval(check);
        wss.close(() => {
          client.terminate();
          finish();
        });
      }
    }, 30);
  });

  wss.on('error', finish);
}

function scenarioF(finish) {
  const wss = new WebSocketServer(
    { port: 0, perMessageDeflate: { threshold: 0 } },
    () => {
      const client = new WebSocket(`ws://localhost:${wss.address().port}`, {
        perMessageDeflate: true
      });

      let serverWs;
      const clientReceived = [];
      let serverDataCb = null;
      let serverCloseEmitted = false;
      let clientCloseEmitted = false;

      wss.on('connection', (ws) => {
        serverWs = ws;
        attachProbes(ws, 'SERVER');
        ws.on('close', (code, reason) => {
          serverCloseEmitted = true;
          log(
            'SERVER',
            `'close' EVENT code=${code} reason=${reason.toString()}`
          );
        });

        const payload = makeIncompressiblePayload(HUGE_SIZE);
        log(
          'SERVER',
          `send() large compressed snapshot, payloadBytes=${payload.length}`
        );
        ws.send(payload, { binary: true }, (err) => {
          serverDataCb = err ? 'error: ' + err.message : 'ok';
          log(
            'SERVER',
            `data send() CALLBACK: ${serverDataCb}; readyState=${ws.readyState} ` +
              `closeFrameSent=${ws._closeFrameSent} closeFrameReceived=${ws._closeFrameReceived}`
          );
        });

        log(
          'SERVER',
          `send() returned; sender.state=${ws._sender._state} (DEFLATING). ` +
            `PEER (client) will now close() gracefully while we are still compressing.`
        );

        setTimeout(() => {
          log(
            'SERVER',
            `state right before client close arrives: sender.state=${ws._sender._state} ` +
              `queueLen=${ws._sender._queue.length} closeFrameReceived=${ws._closeFrameReceived}`
          );
        }, 10);
      });

      client.on('open', () => {
        setTimeout(() => {
          log(
            'CLIENT',
            'calling close() (peer-initiated graceful close) while server compresses'
          );
          client.close(1000, 'peer closing');
        }, 5);
      });
      client.on('message', (data) => {
        clientReceived.push(data.length);
        log('CLIENT', `RECEIVED message bytes=${data.length}`);
      });
      client.on('close', (code, reason) => {
        clientCloseEmitted = true;
        log('CLIENT', `'close' EVENT code=${code} reason=${reason.toString()}`);
      });

      const check = setInterval(() => {
        if (serverWs && serverCloseEmitted && clientCloseEmitted) {
          clearInterval(check);
          log(
            'RESULT',
            `peer graceful close during compression: server data callback=${serverDataCb}; ` +
              `client received ${clientReceived.length} data frame(s); ` +
              `both sides emitted 'close'. The queued data frame was still sent and ` +
              `delivered because close() only queues; the receiver drops bytes that ` +
              `arrive AFTER its close frame, not bytes already in flight before it.`
          );
          wss.close(() => {
            client.terminate();
            finish();
          });
        }
      }, 30);
    }
  );

  wss.on('error', finish);
}

function scenarioG(finish) {
  const wss = new WebSocketServer(
    { port: 0, perMessageDeflate: { threshold: 0 } },
    () => {
      const client = new WebSocket(`ws://localhost:${wss.address().port}`, {
        perMessageDeflate: true
      });

      let serverWs;
      let serverDataCb = null;
      let serverCloseEmitted = false;
      let clientCloseEmitted = false;
      const clientReceived = [];

      wss.on('connection', (ws) => {
        serverWs = ws;
        attachProbes(ws, 'SERVER');
        ws.on('close', (code) => {
          serverCloseEmitted = true;
          log('SERVER', `'close' EVENT code=${code}`);
        });

        const payload = makeIncompressiblePayload(HUGE_SIZE);
        log(
          'SERVER',
          `send() large compressed snapshot, payloadBytes=${payload.length}`
        );
        ws.send(payload, { binary: true }, (err) => {
          serverDataCb = err ? 'error: ' + err.message : 'ok';
          log('SERVER', `data send() CALLBACK: ${serverDataCb}`);
        });

        log(
          'SERVER',
          `send() returned; sender.state=${ws._sender._state}. ` +
            `BOTH sides will close() while server is still compressing.`
        );

        setTimeout(() => {
          log('SERVER', 'calling close() (simultaneous with peer close)');
          ws.close(1001, 'server going away');
        }, 5);
      });

      client.on('open', () => {
        setTimeout(() => {
          log('CLIENT', 'calling close() (simultaneous with server close)');
          client.close(1000, 'client going away');
        }, 5);
      });
      client.on('message', (data) => {
        clientReceived.push(data.length);
        log('CLIENT', `RECEIVED message bytes=${data.length}`);
      });
      client.on('close', (code, reason) => {
        clientCloseEmitted = true;
        log('CLIENT', `'close' EVENT code=${code} reason=${reason.toString()}`);
      });

      const check = setInterval(() => {
        if (serverWs && serverCloseEmitted && clientCloseEmitted) {
          clearInterval(check);
          log(
            'RESULT',
            `simultaneous graceful close during compression: server data callback=${serverDataCb}; ` +
              `client received ${clientReceived.length} data frame(s). When both sides have ` +
              `already sent a close frame, socket.end() fires as soon as the local close frame ` +
              `is written, but the kernel still flushes bytes that already reached it; no error ` +
              `is surfaced to the send callback. Delivery to the peer application is best-effort.`
          );
          wss.close(() => {
            client.terminate();
            finish();
          });
        }
      }, 30);
    }
  );

  wss.on('error', finish);
}

function scenarioH(finish) {
  const wss = new WebSocketServer(
    { port: 0, perMessageDeflate: { threshold: 0 } },
    () => {
      const client = new WebSocket(`ws://localhost:${wss.address().port}`, {
        perMessageDeflate: true
      });

      let serverWs;
      let serverDataCb = null;
      let serverErrorEmitted = false;
      let serverCloseEmitted = false;
      let clientCloseEmitted = false;
      const clientReceived = [];

      wss.on('connection', (ws) => {
        serverWs = ws;
        attachProbes(ws, 'SERVER');
        ws.on('error', (err) => {
          serverErrorEmitted = true;
          log('SERVER', `'error' EVENT: ${err.message}`);
        });
        ws.on('close', (code) => {
          serverCloseEmitted = true;
          log('SERVER', `'close' EVENT code=${code}`);
        });

        const payload = makeIncompressiblePayload(HUGE_SIZE);
        log(
          'SERVER',
          `send() large compressed snapshot, payloadBytes=${payload.length}`
        );
        ws.send(payload, { binary: true }, (err) => {
          serverDataCb = err ? 'error: ' + err.message : 'ok';
          log(
            'SERVER',
            `data send() CALLBACK: ${serverDataCb}; socketDestroyed=${ws._socket.destroyed}`
          );
        });

        log(
          'SERVER',
          `send() returned; sender.state=${ws._sender._state}. ` +
            `PEER will terminate() (RST) while we are still compressing.`
        );

        setTimeout(() => {
          log(
            'CLIENT',
            'calling terminate() (abrupt reset) while server compresses'
          );
          client.terminate();
        }, 5);
      });

      client.on('message', (data) => {
        clientReceived.push(data.length);
        log('CLIENT', `RECEIVED message bytes=${data.length}`);
      });
      client.on('close', (code) => {
        clientCloseEmitted = true;
        log('CLIENT', `'close' EVENT code=${code}`);
      });

      const check = setInterval(() => {
        if (serverWs && serverCloseEmitted && clientCloseEmitted) {
          clearInterval(check);
          log(
            'RESULT',
            `peer abrupt terminate() during compression: server data callback=${serverDataCb}; ` +
              `server error event=${serverErrorEmitted}; client received ${clientReceived.length} ` +
              `data frame(s). The RST destroys the local socket; the in-flight data may be lost, ` +
              `and whether the send callback gets an error depends on timing of the OS notification. ` +
              `This is the case where even "written to local socket" cannot guarantee delivery, and ` +
              `the close code is abnormal (1006).`
          );
          wss.close(() => {
            client.terminate();
            finish();
          });
        }
      }, 30);
    }
  );

  wss.on('error', finish);
}

async function main() {
  try {
    await runScenario(
      'A: graceful close() right after send() of a large compressed snapshot (client reads)',
      scenarioA
    );
    await runScenario(
      'B: same as A, but client pauses its socket - data still compressing when close() is called',
      scenarioB
    );
    await runScenario(
      'C: terminate() (non-graceful) during compression - contrast case',
      scenarioC
    );
    await runScenario(
      'D: close() while multiple compressed messages are queued - ordering guarantee',
      scenarioD
    );
    await runScenario(
      'E: data written to local socket buffer (callback fired) but peer not yet reading - backpressure boundary',
      scenarioE
    );
    await runScenario(
      'F: PEER graceful close() while local data is still compressing (local has NOT called close)',
      scenarioF
    );
    await runScenario(
      'G: BOTH sides graceful close() simultaneously while local data is still compressing',
      scenarioG
    );
    await runScenario(
      'H: PEER terminate() (abrupt RST) while local data is still compressing',
      scenarioH
    );
    console.log('\nAll scenarios completed.');
    process.exit(0);
  } catch (err) {
    console.error('Scenario failed:', err);
    process.exit(1);
  }
}

main();
