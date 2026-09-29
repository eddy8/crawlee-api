import assert from 'node:assert/strict';
import { test } from 'node:test';
import { loadConfig } from '../src/config.js';
import { createFetchService } from '../src/fetch-service.js';

test('configures the crawler for low-latency API use', async (t) => {
    const { crawler } = createFetchService(
        loadConfig({ BROWSER_IDLE_TIMEOUT_SECS: '900' }),
    );
    t.after(() => crawler.browserPool.destroy());

    // New work must not wait for Crawlee's default 0.5 s pool tick.
    assert.equal(crawler.autoscaledPoolOptions.maybeRunIntervalSecs, 0.05);

    // Idle browsers stay warm instead of being retired after 10 s.
    assert.equal(
        crawler.browserPool.browserRetireInterval._idleTimeout,
        900_000,
    );

    // Memory overload threshold is not Crawlee's 0.25 default, but an
    // explicit environment override still wins.
    const expectedRatio = process.env.CRAWLEE_AVAILABLE_MEMORY_RATIO
        ? Number(process.env.CRAWLEE_AVAILABLE_MEMORY_RATIO)
        : 0.8;
    assert.equal(crawler.config.get('availableMemoryRatio'), expectedRatio);
});
