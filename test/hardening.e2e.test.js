import assert from 'node:assert/strict';
import { request as httpRequest, createServer } from 'node:http';
import { connect } from 'node:net';
import { after, before, test } from 'node:test';
import { log } from 'crawlee';
import { loadConfig } from '../src/config.js';
import { createFetchService } from '../src/fetch-service.js';

log.setLevel(log.LEVELS.OFF);

let upstream;
let upstreamPort;

before(async () => {
    upstream = createServer((request, response) => {
        if (request.url === '/doctype') {
            response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            response.end('<!DOCTYPE html><!-- c --><html><head><title>d</title></head><body><p>ok</p></body></html>');
            return;
        }

        if (request.url === '/dom-bomb') {
            // Small document that renders into a DOM far above the limit.
            response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            response.end('<!doctype html><body><script>document.body.textContent = "x".repeat(200000);</script></body>');
            return;
        }

        if (request.url === '/delay') {
            setTimeout(() => {
                response.writeHead(200, { 'Content-Type': 'text/html' });
                response.end('<p>finished</p>');
            }, 1_500);
            return;
        }

        response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        response.end('<!doctype html><title>small</title><p>ok</p>');
    });
    upstreamPort = await listenOnRandomPort(upstream);
});

after(async () => {
    await new Promise((resolve) => upstream.close(resolve));
});

async function startService(t, environment = {}) {
    const config = loadConfig({
        HOST: '127.0.0.1',
        PORT: String(await reservePort()),
        ...environment,
    });
    const service = createFetchService(config);
    await service.start();
    t.after(() => service.shutdown('test', { drain: false }));
    const url = (pathname) => `http://127.0.0.1:${config.port}${pathname}`;
    const fetchTarget = (pathname) => fetch(url(`/fetch?${new URLSearchParams({
        url: `http://127.0.0.1:${upstreamPort}${pathname}`,
    })}`));
    return { config, service, url, fetchTarget };
}

test('keep-alive timeout is configurable and outlives typical proxies by default', { timeout: 20_000 }, async (t) => {
    const { service } = await startService(t);
    assert.equal(service.server.keepAliveTimeout, 65_000);

    const short = await startService(t, { KEEP_ALIVE_TIMEOUT_MS: '300' });
    assert.equal(short.service.server.keepAliveTimeout, 300);

    // An idle keep-alive connection is closed by the server after the timeout.
    const socket = connect(short.config.port, '127.0.0.1');
    socket.resume();
    const closed = new Promise((resolve) => socket.once('close', resolve));
    socket.write('GET /health/live HTTP/1.1\r\nHost: x\r\n\r\n');
    const startedAt = Date.now();
    await closed;
    assert.ok(Date.now() - startedAt < 3_000);
});

test('malformed requests get a proper status and the socket is released', { timeout: 20_000 }, async (t) => {
    const { config } = await startService(t);

    const garbage = await rawExchange(config.port, 'NOT HTTP AT ALL\r\n\r\n');
    assert.match(garbage.data, /^HTTP\/1\.1 400 Bad Request/);
    assert.ok(garbage.closed);

    const hugeHeader = `GET / HTTP/1.1\r\nHost: x\r\nX-Big: ${'a'.repeat(20_000)}\r\n\r\n`;
    const overflow = await rawExchange(config.port, hugeHeader);
    assert.match(overflow.data, /^HTTP\/1\.1 431 /);
    assert.ok(overflow.closed);
});

test('HTML is serialized once with doctype and comments preserved', { timeout: 20_000 }, async (t) => {
    const { fetchTarget } = await startService(t);
    const response = await fetchTarget('/doctype');
    const body = await response.text();

    assert.equal(response.status, 200);
    assert.match(body, /^<!DOCTYPE html><!-- c --><html>/);
    assert.match(body, /<p>ok<\/p>/);
});

test('a rendered DOM above the limit is rejected before transfer', { timeout: 20_000 }, async (t) => {
    const { fetchTarget } = await startService(t, { MAX_RESPONSE_BYTES: '50000' });
    const response = await fetchTarget('/dom-bomb');
    const body = await response.json();

    assert.equal(response.status, 502);
    assert.equal(body.code, 'UPSTREAM_RESPONSE_TOO_LARGE');
});

test('liveness fails while submitted jobs are stalled past their deadline', { timeout: 20_000 }, async (t) => {
    const { service, url, fetchTarget } = await startService(t, {
        API_TIMEOUT_MS: '800',
        JOB_STALL_GRACE_MS: '300',
    });
    await waitFor(() => service.crawler.autoscaledPool);
    const pool = service.crawler.autoscaledPool;
    await pool.pause();

    try {
        // The crawler never picks the job up; the API still answers at its deadline.
        const response = await fetchTarget('/small');
        assert.equal(response.status, 504);

        await waitFor(async () => (await fetch(url('/health/live'))).status === 503, 5_000);
        const live = await (await fetch(url('/health/live'))).json();
        assert.equal(live.live, false);
        assert.equal(live.stalledJobs, 1);
        assert.equal((await fetch(url('/health/ready'))).status, 503);
    } finally {
        // Never leave the pool paused, or shutdown would wait on it forever.
        pool.resume();
    }

    // Once the crawler releases the job, the instance recovers.
    await waitFor(async () => (await fetch(url('/health/live'))).status === 200);
});

test('SIGTERM with a shutdown delay fails readiness but keeps serving first', { timeout: 20_000 }, async (t) => {
    const { service, url, fetchTarget } = await startService(t, {
        SHUTDOWN_DELAY_MS: '1000',
    });
    // Warm the browser so the in-delay request is fast.
    assert.equal((await fetchTarget('/small')).status, 200);

    const inFlight = getWithHeaders(url(`/fetch?${new URLSearchParams({
        url: `http://127.0.0.1:${upstreamPort}/delay`,
    })}`));
    await delay(100);

    const shutdownStartedAt = Date.now();
    const shutdown = service.shutdown('SIGTERM');

    assert.equal((await fetch(url('/health/ready'))).status, 503);
    assert.equal((await fetch(url('/health/live'))).status, 200);
    const duringDelay = await fetchTarget('/small');
    assert.equal(duringDelay.status, 200, 'requests are still served during the delay');

    // The in-flight request finishes during drain and tells the client to reconnect elsewhere.
    const finished = await inFlight;
    assert.equal(finished.status, 200);
    assert.equal(finished.headers.connection, 'close');
    assert.match(finished.body, /finished/);

    await shutdown;
    assert.ok(Date.now() - shutdownStartedAt >= 1_000);
    await assert.rejects(fetch(url('/health/live')));
});

function rawExchange(port, payload) {
    return new Promise((resolve, reject) => {
        const socket = connect(port, '127.0.0.1');
        let data = '';
        const timer = setTimeout(() => {
            socket.destroy();
            resolve({ data, closed: false });
        }, 3_000);
        socket.on('data', (chunk) => { data += chunk; });
        socket.on('error', () => {});
        socket.on('close', () => {
            clearTimeout(timer);
            resolve({ data, closed: true });
        });
        socket.once('connect', () => socket.write(payload));
        socket.once('error', reject);
    });
}

function getWithHeaders(target) {
    return new Promise((resolve, reject) => {
        httpRequest(target, (response) => {
            let body = '';
            response.setEncoding('utf8');
            response.on('data', (chunk) => { body += chunk; });
            response.on('end', () => resolve({
                status: response.statusCode,
                headers: response.headers,
                body,
            }));
        }).on('error', reject).end();
    });
}

async function waitFor(predicate, timeoutMs = 10_000) {
    const deadline = Date.now() + timeoutMs;
    while (!await predicate()) {
        if (Date.now() > deadline) throw new Error('Condition not met in time');
        await delay(25);
    }
}

function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function reservePort() {
    const server = createServer();
    const port = await listenOnRandomPort(server);
    await new Promise((resolve) => server.close(resolve));
    return port;
}

function listenOnRandomPort(server) {
    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => resolve(server.address().port));
    });
}
