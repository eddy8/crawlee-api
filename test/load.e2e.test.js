import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { test } from 'node:test';
import { log } from 'crawlee';
import { loadConfig } from '../src/config.js';
import { createFetchService } from '../src/fetch-service.js';

test('sustained bursts stay within capacity and release queue and browser resources', { timeout: 60_000 }, async (t) => {
    log.setLevel(log.LEVELS.OFF);
    let upstreamActive = 0;
    let peakUpstreamActive = 0;
    const upstream = createServer((request, response) => {
        if (request.url === '/favicon.ico') { response.writeHead(204); response.end(); return; }
        upstreamActive += 1;
        peakUpstreamActive = Math.max(peakUpstreamActive, upstreamActive);
        const timer = setTimeout(() => {
            response.writeHead(200, {
                'Content-Type': 'text/html; charset=utf-8',
                'Cache-Control': 'no-store',
            });
            response.end('<title>load fixture</title><p>complete</p>');
        }, 80);
        response.once('close', () => { clearTimeout(timer); upstreamActive -= 1; });
    });
    await new Promise((resolve) => upstream.listen(0, '127.0.0.1', resolve));
    const service = createFetchService({
        ...loadConfig({}), host: '127.0.0.1', port: 0,
        maxConcurrency: 3, maxPendingRequests: 12,
        apiTimeoutMs: 15_000, shutdownTimeoutMs: 5000,
    });
    try {
        await service.start();
        const baseUrl = `http://127.0.0.1:${service.server.address().port}`;
        const latencies = [];
        const startedAt = performance.now();
        for (let batch = 0; batch < 5; batch += 1) {
            await Promise.all(Array.from({ length: 12 }, async (_, index) => {
                const query = new URLSearchParams({
                    url: `http://127.0.0.1:${upstream.address().port}/page?job=${batch * 12 + index}`,
                });
                const started = performance.now();
                const response = await fetch(`${baseUrl}/fetch?${query}`);
                assert.equal(response.status, 200);
                assert.match(await response.text(), /<p>complete<\/p>/u);
                latencies.push(performance.now() - started);
            }));
            await waitFor(async () => service.crawler.autoscaledPool.currentConcurrency === 0);
            const queueInfo = await service.crawler.requestQueue.getInfo();
            assert.equal(queueInfo.totalRequestCount, 0);
            assert.equal(service.crawler.requestQueue.requestCache.length(), 0);
            assert.equal(service.crawler.requestQueue.requestSeenCache.size, 0);
        }
        const metrics = await (await fetch(`${baseUrl}/metrics`)).json();
        assert.equal(metrics.requests.succeeded, 60);
        assert.equal(metrics.requests.failed, 0);
        assert.equal(metrics.jobs.total, 0);
        assert.equal(metrics.jobs.crawlerSlotsInUse, 0);
        assert.ok(peakUpstreamActive <= 3);
        for (const controller of service.crawler.browserPool.activeBrowserControllers) {
            assert.equal(controller.activePages, 0);
        }
        latencies.sort((a, b) => a - b);
        t.diagnostic(JSON.stringify({
            requests: latencies.length,
            browserConcurrencyLimit: 3,
            peakUpstreamActive,
            durationMs: Math.round(performance.now() - startedAt),
            p50Ms: Math.round(latencies[29]),
            p95Ms: Math.round(latencies[56]),
            retainedQueueRequests: 0,
        }));
    } finally {
        await service.shutdown('load test cleanup', { drain: false });
        upstream.closeAllConnections();
        await new Promise((resolve) => upstream.close(resolve));
    }
});

async function waitFor(predicate) {
    const deadline = Date.now() + 5000;
    while (!await predicate()) {
        assert.ok(Date.now() < deadline, 'Browser slots did not drain');
        await new Promise((resolve) => setTimeout(resolve, 20));
    }
}
