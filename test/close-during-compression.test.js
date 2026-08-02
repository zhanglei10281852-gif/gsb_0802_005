'use strict';

const assert = require('assert');

const WebSocket = require('..');

//
// Reproduction for the investigation documented in
// `docs/close-during-compression-investigation.md`.
//
// These tests do not change any library behavior. They pin down what is
// actually observable when a compressed data message and a graceful close
// happen in the same tick, and they make the two delivery boundaries explicit:
//
//   Boundary A ("accepted by the sender layer"): `ws.send()` returned without
//   throwing while `readyState === OPEN`. The payload is now owned by the
//   `Sender` (dispatched or queued) and counts towards `bufferedAmount`. No
//   framing, compression, or socket write has happened yet.
//
//   Boundary B ("written to the local socket"): the per-message `send()`
//   callback fired without an error. The compressed frame was handed to
//   `socket.write()` and accepted by the OS send buffer. This is the strongest
//   guarantee the library gives; it is still not peer receipt.
//
describe('close during compression', () => {
  //
  // A large, highly compressible payload so that:
  //   - `compress: true` is actually exercised (threshold is set to 0), and
  //   - there is a real asynchronous gap between Boundary A and Boundary B
  //     during which `close()` is called.
  //
  const snapshot = Buffer.alloc(1 << 16, 0x61);

  it(
    'a compressed snapshot enqueued before a graceful close is still ' +
      'compressed, written to the socket, and delivered to the peer',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });

          ws.on('open', () => {
            let boundaryB;

            ws.send(snapshot, (err) => {
              //
              // Boundary B: the compressed frame reached `socket.write()`. With
              // a graceful `close()` the socket is not destroyed while the
              // snapshot is being compressed, so there is no error here.
              //
              boundaryB = {
                err,
                readyState: ws.readyState,
                socketDestroyed: ws._socket.destroyed
              };
            });

            //
            // Boundary A: `send()` has returned while still OPEN. The snapshot
            // is owned by the `Sender` and is being compressed asynchronously,
            // so it is still buffered and nothing has been written yet.
            //
            assert.strictEqual(ws.readyState, WebSocket.OPEN);
            assert.ok(ws.bufferedAmount > 0);
            assert.strictEqual(boundaryB, undefined);

            //
            // Graceful close. The close frame is enqueued *behind* the
            // in-flight compression, preserving ordering.
            //
            ws.close(1000, 'bye');
            assert.strictEqual(ws.readyState, WebSocket.CLOSING);

            ws.on('close', (code) => {
              assert.strictEqual(code, 1000);
              assert.ok(boundaryB, 'the send callback must have fired');
              assert.ifError(boundaryB.err);
              assert.strictEqual(boundaryB.readyState, WebSocket.CLOSING);
              assert.strictEqual(boundaryB.socketDestroyed, false);
            });
          });
        }
      );

      wss.on('connection', (ws) => {
        const chunks = [];

        ws.on('message', (data, isBinary) => {
          assert.ok(isBinary);
          chunks.push(data);
        });

        ws.on('close', (code) => {
          const received = Buffer.concat(chunks);

          //
          // Peer receipt: the snapshot arrived intact even though the client
          // closed immediately after enqueueing it.
          //
          assert.strictEqual(code, 1000);
          assert.strictEqual(received.length, snapshot.length);
          assert.ok(received.equals(snapshot));
          wss.close(done);
        });
      });
    }
  );

  it(
    'if the connection is torn down while the snapshot is still being ' +
      'compressed, the send callback reports it never reached the socket',
    (done) => {
      const wss = new WebSocket.Server(
        { perMessageDeflate: true, port: 0 },
        () => {
          const ws = new WebSocket(`ws://localhost:${wss.address().port}`, {
            perMessageDeflate: { threshold: 0 }
          });

          ws.on('open', () => {
            ws.send(snapshot, (err) => {
              //
              // Boundary B was NOT reached: the socket was torn down before the
              // compressed frame could be written, so the bytes never left the
              // library. `readyState` is already CLOSING.
              //
              assert.strictEqual(ws.readyState, WebSocket.CLOSING);
              assert.ok(err instanceof Error);
              assert.strictEqual(
                err.message,
                'The socket was closed while data was being compressed'
              );
            });

            //
            // A graceful `close()` is still requested by the application, but
            // the peer disappears first (see the server side below).
            //
            ws.close(1000, 'bye');

            ws.on('close', () => wss.close(done));
          });
        }
      );

      wss.on('connection', (ws) => {
        //
        // Simulate the peer going away right after the upgrade, while the
        // client is still compressing the snapshot.
        //
        ws._socket.end();
      });
    }
  );
});
