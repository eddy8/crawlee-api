import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { gzipSync } from 'node:zlib';
import { after, before, test } from 'node:test';
import { log } from 'crawlee';
import { loadConfig } from '../src/config.js';
import { createFetchService } from '../src/fetch-service.js';

let upstream;
let upstreamUrl;
let service;
let baseUrl;
let streamAborts = 0;
let delayedStarts = 0;
let delayedAborts = 0;
let setupPromise;

before(() => {
    setupPromise = startFixtures();
    return setupPromise;
});

async function startFixtures() {
    log.setLevel(log.LEVELS.ERROR);
    upstream = createServer((request, response) => {
        const pathname = new URL(request.url, 'http://localhost').pathname;
        response.setHeader('Content-Type', 'text/html; charset=utf-8');
        if (pathname === '/normal-status') {
            response.end('<title>Dashboard</title><body data-state="ready"><p id="status">Online</p>');
        } else if (pathname === '/challenge') {
            response.writeHead(403, { 'X-Gatekeeper-Challenge': 'required' });
            response.end('<title>正在验证浏览器</title><body data-state="checking"><div id="status">Checking</div><script>setTimeout(() => { document.cookie="verified=yes; path=/"; location.replace("/final"); }, 100)</script>');
        } else if (pathname === '/final') {
            response.writeHead(201, { 'Content-Type': 'application/json' });
            response.end(JSON.stringify({ cookie: request.headers.cookie || '' }));
        } else if (pathname === '/gzip') {
            response.writeHead(200, { 'Content-Type': 'text/plain', 'Content-Encoding': 'gzip' });
            response.end(gzipSync('界'.repeat(1000)));
        } else if (pathname === '/stream') {
            response.writeHead(200, { 'Content-Type': 'text/plain' });
            response.write('x'.repeat(4096));
            const timer = setInterval(() => response.write('x'.repeat(4096)), 50);
            response.once('close', () => { clearInterval(timer); streamAborts += 1; });
        } else if (pathname === '/delayed') {
            delayedStarts += 1;
            const timer = setTimeout(() => response.end('<p>finished</p>'), 300);
            response.once('close', () => {
                clearTimeout(timer);
                if (!response.writableEnded) delayedAborts += 1;
            });
        } else {
            response.end('<p>ok</p>');
        }
    });
    await new Promise((resolve) => upstream.listen(0, '127.0.0.1', resolve));
    upstreamUrl = `http://127.0.0.1:${upstream.address().port}`;
    service = createFetchService({
        ...loadConfig({}), host: '127.0.0.1', port: 0,
        maxConcurrency: 1, maxPendingRequests: 4,
        apiTimeoutMs: 4000, gatekeeperTimeoutMs: 300,
        maxResponseBytes: 1024, shutdownTimeoutMs: 2500,
    });
    await service.start();
    baseUrl = `http://127.0.0.1:${service.server.address().port}`;
}

after(async () => {
    await setupPromise;
    await service?.shutdown('test cleanup', { drain: false });
    upstream?.closeAllConnections();
    if (upstream?.listening) await new Promise((resolve) => upstream.close(resolve));
});

const fetchTarget = (path) => fetch(`${baseUrl}/fetch?${new URLSearchParams({ url: upstreamUrl + path })}`);
const metrics = async () => (await fetch(`${baseUrl}/metrics`)).json();

test('ordinary pages with a status element are not treated as challenges', async () => {
    const response = await fetchTarget('/normal-status');
    assert.equal(response.status, 200, await response.text());
});

test('challenge redirects return the final status, URL, body and cookies', async () => {
    const response = await fetchTarget('/challenge');
    assert.equal(response.status, 201, await response.clone().text());
    assert.equal(response.headers.get('x-upstream-status'), '201');
    assert.equal(decodeURIComponent(response.headers.get('x-final-url')), upstreamUrl + '/final');
    assert.match((await response.json()).cookie, /verified=yes/);
});

test('enforces the size limit on decompressed response bytes', async () => {
    const response = await fetchTarget('/gzip');
    assert.equal(response.status, 502);
    assert.equal((await response.json()).code, 'UPSTREAM_RESPONSE_TOO_LARGE');
});

test('aborts oversized chunked responses before the API deadline', async () => {
    const previous = streamAborts;
    const response = await fetchTarget('/stream');
    assert.equal(response.status, 502, await response.clone().text());
    assert.equal((await response.json()).code, 'UPSTREAM_RESPONSE_TOO_LARGE');
    await waitFor(() => streamAborts > previous);
    await waitFor(async () => (await metrics()).jobs.total === 0);
});

test('client disconnect releases its slot and stops upstream work', async () => {
    const previous = delayedAborts;
    const url = `${baseUrl}/fetch?${new URLSearchParams({ url: upstreamUrl + '/delayed' })}`;
    const started = delayedStarts;
    const client = httpRequest(url);
    client.on('error', () => {});
    client.end();
    await waitFor(() => delayedStarts > started);
    client.destroy();
    await waitFor(async () => (await metrics()).jobs.total === 0);
    assert.ok((await metrics()).requests.cancelled >= 1);
    await waitFor(() => delayedAborts > previous);
    assert.equal((await fetchTarget('/small')).status, 200);
});

test('releases handled queue storage after repeated HTTP requests', async () => {
    for (let index = 0; index < 12; index += 1) {
        const response = await fetchTarget(`/small?n=${index}`);
        assert.equal(response.status, 200);
        await response.text();
    }
    await waitFor(async () => service.crawler.autoscaledPool.currentConcurrency === 0);
    const info = await service.crawler.requestQueue.getInfo();
    assert.equal(info.totalRequestCount, 0, 'Completed requests must not accumulate in memory');
});

test('shutdown drains both running and already accepted queued requests', async () => {
    const started = delayedStarts;
    const first = fetchTarget('/delayed');
    await waitFor(() => delayedStarts > started);
    const second = fetchTarget('/small');
    await waitFor(async () => (await metrics()).jobs.queued === 1);
    const stopping = service.shutdown('drain test');
    const responses = await Promise.all([first, second]);
    for (const response of responses) assert.equal(response.status, 200, await response.text());
    await stopping;
});

async function waitFor(predicate, timeoutMs = 6000) {
    const deadline = Date.now() + timeoutMs;
    while (!await predicate()) {
        assert.ok(Date.now() < deadline, 'Condition did not become true before the deadline');
        await new Promise((resolve) => setTimeout(resolve, 20));
    }
}
