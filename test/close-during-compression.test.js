'use strict';

const assert = require('assert');
const crypto = require('crypto');

const WebSocket = require('..');

//
// Reproduction scenarios for the investigation documented in
// `docs/close-during-compression-investigation.md`.
//
// Each scenario uses a real local server and client with permessage-deflate
// negotiated. The payload is a 1 MiB incompressible buffer so that
// compression actually runs and the deflate window (tens of milliseconds)
// is much wider than the localhost round trip (sub-millisecond), making the
// interleavings under test deterministic in practice.
//
const PAYLOAD = crypto.randomBytes(1024 * 1024);

describe('close-during-compression investigation', () => {
  it('delivers a message that is still being compressed when close() is called', (done) => {
    const events = [];
    let sendCallbackCalled = false;

    const wss = new WebSocket.Server(
      { perMessageDeflate: { threshold: 0 }, port: 0 },
      () => {
        const ws = new WebSocket(`ws://localhost:${wss.address().port}`);

        ws.on('message', (data, isBinary) => {
          events.push('message');
          assert.ok(isBinary);
          assert.deepStrictEqual(data, PAYLOAD);
        });

        ws.on('close', (code) => {
          events.push('close');

          // The peer received the whole message before the close frame, and
          // the close handshake completed with the negotiated code.
          assert.strictEqual(code, 1000);
          assert.deepStrictEqual(events, ['message', 'close']);
          assert.ok(sendCallbackCalled);

          wss.close(done);
        });
      }
    );

    wss.on('connection', (ws) => {
      ws.send(PAYLOAD, (err) => {
        // The message was written to the local socket before the close
        // frame.
        assert.ifError(err);
        sendCallbackCalled = true;
      });

      //
      // Called synchronously after `send()`. Compression is always
      // asynchronous, so the close frame is guaranteed to be enqueued behind
      // the in-flight message.
      //
      ws.close(1000);
    });
  });

  it('rejects send() calls made after close() without queueing them', (done) => {
    const received = [];

    const wss = new WebSocket.Server(
      { perMessageDeflate: { threshold: 0 }, port: 0 },
      () => {
        const ws = new WebSocket(`ws://localhost:${wss.address().port}`);

        ws.on('message', (data) => received.push(data));

        ws.on('close', (code) => {
          assert.strictEqual(code, 1000);

          // Only the message sent before `close()` was delivered.
          assert.strictEqual(received.length, 1);
          assert.deepStrictEqual(received[0], PAYLOAD);

          wss.close(done);
        });
      }
    );

    wss.on('connection', (ws) => {
      ws.send(PAYLOAD);
      ws.close(1000);

      ws.send('too late', (err) => {
        // The message never entered the send queue.
        assert.ok(err instanceof Error);
        assert.strictEqual(
          err.message,
          'WebSocket is not open: readyState 2 (CLOSING)'
        );
      });
    });
  });

  it('delivers the message to a peer that already initiated the close handshake', (done) => {
    const events = [];

    const wss = new WebSocket.Server(
      { perMessageDeflate: { threshold: 0 }, port: 0 },
      () => {
        const ws = new WebSocket(`ws://localhost:${wss.address().port}`);

        ws.on('open', () => {
          //
          // Initiate the graceful close immediately. The close frame reaches
          // the server while the snapshot is still being compressed.
          //
          ws.close(1000);
        });

        ws.on('message', (data) => {
          events.push('message');
          assert.deepStrictEqual(data, PAYLOAD);
        });

        ws.on('close', (code) => {
          events.push('close');

          //
          // Even though this peer sent the first close frame, it still
          // received the in-flight message before the handshake completed.
          //
          assert.strictEqual(code, 1000);
          assert.deepStrictEqual(events, ['message', 'close']);

          wss.close(done);
        });
      }
    );

    wss.on('connection', (ws) => {
      ws.send(PAYLOAD, (err) => assert.ifError(err));
    });
  });

  it('fails all pending send callbacks if the socket is destroyed during compression', (done) => {
    const errors = [];

    const wss = new WebSocket.Server(
      { perMessageDeflate: { threshold: 0 }, port: 0 },
      () => {
        const ws = new WebSocket(`ws://localhost:${wss.address().port}`);

        ws.on('open', () => ws.terminate());
        ws.on('message', () => done(new Error('unexpected message')));
      }
    );

    wss.on('connection', (ws) => {
      ws.on('error', () => {});

      ws.send(PAYLOAD, (err) => errors.push(err));
      ws.send(PAYLOAD, (err) => errors.push(err));

      ws.on('close', (code) => {
        // Abnormal closure: no close frame was exchanged.
        assert.strictEqual(code, 1006);

        //
        // The message being compressed and the one still queued are both
        // lost, and both callbacks report it.
        //
        assert.strictEqual(errors.length, 2);

        for (const err of errors) {
          assert.ok(err instanceof Error);
          assert.strictEqual(
            err.message,
            'The socket was closed while data was being compressed'
          );
        }

        wss.close(done);
      });
    });
  });

  it('runs the send callback before the peer has necessarily processed the message', (done) => {
    let client;
    let messageReceived = false;

    const wss = new WebSocket.Server(
      { perMessageDeflate: { threshold: 0 }, port: 0 },
      () => {
        client = new WebSocket(`ws://localhost:${wss.address().port}`);

        client.on('open', () => {
          //
          // Stop reading, so nothing is delivered to the `'message'`
          // listeners until `resume()` is called below.
          //
          client.pause();
        });

        client.on('message', (data) => {
          messageReceived = true;
          assert.deepStrictEqual(data, PAYLOAD);
        });

        client.on('close', (code) => {
          assert.strictEqual(code, 1000);
          assert.ok(messageReceived);

          wss.close(done);
        });
      }
    );

    wss.on('connection', (ws) => {
      ws.send(PAYLOAD, (err) => {
        assert.ifError(err);

        //
        // The message has been written to the local socket, but the peer is
        // paused and has not processed it yet: "written" does not mean
        // "received".
        //
        assert.ok(!messageReceived);

        // Unblock the peer so the close handshake can complete.
        setTimeout(() => client.resume(), 50);
      });

      ws.close(1000);
    });
  });
});
