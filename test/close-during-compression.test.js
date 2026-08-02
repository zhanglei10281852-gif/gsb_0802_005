'use strict';

const assert = require('assert');

const WebSocket = require('..');

const DEFLATING = 1;

function makeLargeSnapshot(size) {
  const chunk = 'SNAPSHOT-TICK-0001|SYMBOL=600000|LAST=12.34|VOL=98765|';
  const buf = Buffer.alloc(size);
  for (let i = 0; i < size; i += chunk.length) {
    chunk.split('').forEach((ch, j) => {
      if (i + j < size) buf[i + j] = ch.charCodeAt(0);
    });
  }
  return buf;
}

describe('close during compression investigation', () => {
  it(
    'scenario A: large compressed snapshot sent immediately before graceful ' +
      'close is delivered before the close frame',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });
          const snapshot = makeLargeSnapshot(256 * 1024);
          const events = [];

          ws.on('open', () => {
            events.push({ t: 'client-open', ba: ws.bufferedAmount });

            ws.send(snapshot, { compress: true, binary: true }, (err) => {
              assert.ifError(err);
              events.push({
                t: 'send-callback',
                ba: ws.bufferedAmount,
                senderState: ws._sender._state,
                queueLen: ws._sender._queue.length
              });
            });

            events.push({
              t: 'after-send-return',
              ba: ws.bufferedAmount,
              senderState: ws._sender._state,
              queueLen: ws._sender._queue.length
            });

            ws.close(1000);

            events.push({
              t: 'after-close-call',
              readyState: ws.readyState,
              senderState: ws._sender._state,
              queueLen: ws._sender._queue.length
            });
          });

          ws.on('close', (code) => {
            assert.strictEqual(code, 1000);
            assert.ok(
              events.find((e) => e.t === 'send-callback'),
              'send callback should have fired'
            );
            wss.close(done);
          });
        }
      );

      wss.on('connection', (ws) => {
        const received = [];

        ws.on('message', (data, isBinary) => {
          assert.ok(isBinary);
          received.push(data);
        });

        ws.on('close', (code) => {
          assert.strictEqual(received.length, 1);
          assert.strictEqual(received[0].length, 256 * 1024);
          assert.strictEqual(code, 1000);
        });
      });
    }
  );

  it(
    'scenario B: close frame is enqueued behind the in-flight compressed ' +
      'message and not written until compression completes',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });
          const snapshot = makeLargeSnapshot(128 * 1024);
          const frameWriteOrder = [];

          ws.on('open', () => {
            const originalSendFrame = ws._sender.sendFrame.bind(ws._sender);
            ws._sender.sendFrame = (list, cb) => {
              const opcode = list[0][0] & 0x0f;
              const opcodeName =
                opcode === 0x08
                  ? 'close'
                  : opcode === 0x02
                    ? 'binary'
                    : `0x${opcode.toString(16)}`;
              frameWriteOrder.push(opcodeName);
              originalSendFrame(list, cb);
            };

            ws.send(snapshot, { compress: true, binary: true }, (err) => {
              assert.ifError(err);
            });

            assert.strictEqual(ws._sender._state, DEFLATING);
            assert.strictEqual(ws._sender._queue.length, 0);

            ws.close(1000);

            assert.strictEqual(ws._sender._state, DEFLATING);
            assert.strictEqual(ws._sender._queue.length, 1);
            assert.strictEqual(ws.readyState, WebSocket.CLOSING);
          });

          ws.on('close', () => {
            assert.deepStrictEqual(frameWriteOrder, ['binary', 'close']);
            wss.close(done);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('message', () => {});
      });
    }
  );

  it(
    'scenario C: terminate() during compression fails the send callback and ' +
      'discards queued messages',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: { threshold: 0 }, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });
          const callbacks = [];

          ws.on('open', () => {
            ws.send('first', { compress: true }, (err) => {
              callbacks.push({ name: 'first', err });
            });
            ws.send('second', { compress: true }, (err) => {
              callbacks.push({ name: 'second', err });
            });

            assert.strictEqual(ws._sender._state, DEFLATING);

            ws.terminate();
          });

          ws.on('close', (code) => {
            assert.strictEqual(code, 1006);
            assert.strictEqual(callbacks.length, 2);
            for (const cb of callbacks) {
              assert.ok(cb.err instanceof Error);
              assert.ok(
                cb.err.message ===
                  'The socket was closed while data was being compressed' ||
                  cb.err.message ===
                    'The deflate stream was closed while data was being processed'
              );
            }
            wss.close(done);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('message', () => {});
      });
    }
  );

  it(
    'scenario D: multiple compressed messages queued then graceful close are ' +
      'all delivered in order',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });
          const messages = [];
          for (let i = 0; i < 5; i++) {
            messages.push(Buffer.from(`snapshot-${i}-` + 'x'.repeat(4096)));
          }
          const sendCallbacks = [];

          ws.on('open', () => {
            messages.forEach((msg, i) => {
              ws.send(msg, { compress: true, binary: true }, (err) => {
                assert.ifError(err);
                sendCallbacks.push(i);
              });
            });
            ws.close(1000);
          });

          ws.on('close', () => {
            assert.deepStrictEqual(sendCallbacks, [0, 1, 2, 3, 4]);
            wss.close(done);
          });
        }
      );

      wss.on('connection', (ws) => {
        const received = [];

        ws.on('message', (data, isBinary) => {
          assert.ok(isBinary);
          received.push(data);
        });

        ws.on('close', (code) => {
          assert.strictEqual(code, 1000);
          assert.strictEqual(received.length, 5);
          for (let i = 0; i < 5; i++) {
            assert.strictEqual(
              received[i].toString(),
              `snapshot-${i}-` + 'x'.repeat(4096)
            );
          }
        });
      });
    }
  );

  it(
    'scenario E: send callback fires when data is written to local socket, ' +
      'not when peer receives it',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });
          const snapshot = makeLargeSnapshot(64 * 1024);
          const timeline = [];

          ws.on('open', () => {
            ws.send(snapshot, { compress: true, binary: true }, (err) => {
              assert.ifError(err);
              timeline.push('send-callback (local socket.write cb)');
            });
            ws.close(1000);
          });

          ws.on('close', () => {
            assert.ok(
              timeline.includes('send-callback (local socket.write cb)')
            );
            wss.close(done);
          });
        }
      );

      wss.on('connection', (ws) => {
        let peerReceived = false;

        ws.on('message', () => {
          peerReceived = true;
        });

        ws.on('close', () => {
          assert.ok(peerReceived, 'peer should have received the message');
        });
      });
    }
  );

  it(
    'scenario F: send() called after close() is rejected without reaching the ' +
      'sender queue, while already-queued compressed data still flushes',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });
          const snapshot = makeLargeSnapshot(32 * 1024);
          const lateCallback = { err: null, called: false };

          ws.on('open', () => {
            ws.send(snapshot, { compress: true, binary: true }, () => {});
            ws.close(1000);

            ws.send('late-message', (err) => {
              lateCallback.called = true;
              lateCallback.err = err;
            });
          });

          ws.on('close', () => {
            assert.ok(lateCallback.called);
            assert.ok(lateCallback.err instanceof Error);
            assert.ok(
              lateCallback.err.message.includes('WebSocket is not open')
            );
            wss.close(done);
          });
        }
      );

      wss.on('connection', (ws) => {
        const received = [];
        ws.on('message', (data) => received.push(data));
        ws.on('close', () => {
          assert.strictEqual(received.length, 1);
          assert.strictEqual(received[0].length, 32 * 1024);
        });
      });
    }
  );

  it(
    'scenario G: bufferedAmount reflects sender queue plus kernel socket ' +
      'buffer during compression and close',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });
          const snapshot = makeLargeSnapshot(128 * 1024);
          const samples = [];

          ws.on('open', () => {
            ws.send(snapshot, { compress: true, binary: true }, (err) => {
              assert.ifError(err);
              samples.push({
                t: 'after-send-cb',
                ba: ws.bufferedAmount
              });
            });

            samples.push({
              t: 'after-send-return',
              ba: ws.bufferedAmount,
              senderBuffered: ws._sender._bufferedBytes
            });

            ws.close(1000);

            samples.push({
              t: 'after-close',
              ba: ws.bufferedAmount,
              readyState: ws.readyState
            });
          });

          ws.on('close', () => {
            const afterSendReturn = samples.find(
              (s) => s.t === 'after-send-return'
            );
            assert.ok(
              afterSendReturn.ba > 0 || afterSendReturn.senderBuffered > 0,
              'bufferedAmount should reflect the compressing message'
            );
            wss.close(done);
          });
        }
      );

      wss.on('connection', (ws) => {
        ws.on('message', () => {});
      });
    }
  );

  it(
    'scenario H: receiving a close frame while compressing does not abort ' +
      'outbound compressed data; data is still sent',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });
          const snapshot = makeLargeSnapshot(64 * 1024);
          let clientSenderStateAtConclude = null;

          ws.on('open', () => {
            ws._receiver.on('conclude', () => {
              clientSenderStateAtConclude = ws._sender._state;
            });

            ws.send(snapshot, { compress: true, binary: true });
          });

          ws.on('close', (code) => {
            assert.strictEqual(code, 1000);
            assert.strictEqual(clientSenderStateAtConclude, DEFLATING);
            wss.close(done);
          });
        }
      );

      wss.on('connection', (ws) => {
        const received = [];
        ws.on('message', (data) => received.push(data));
        ws.on('close', () => {
          assert.strictEqual(received.length, 1);
          assert.strictEqual(received[0].length, 64 * 1024);
        });

        ws.close(1000);
      });
    }
  );
});
