import assert from 'node:assert/strict';
import { test } from 'node:test';
import { loadConfig } from '../src/config.js';

test('uses defaults only when configuration values are absent', () => {
    assert.equal(loadConfig({}).maxConcurrency, 3);
    assert.equal(loadConfig({ MAX_CONCURRENCY: '6' }).maxPendingRequests, 24);
});

test('rejects malformed numbers instead of partially parsing or falling back', () => {
    for (const value of ['2jobs', '1.5', '0', '-1', 'NaN', '9007199254740992']) {
        assert.throws(() => loadConfig({ MAX_CONCURRENCY: value }), /MAX_CONCURRENCY/);
    }
});

test('rejects invalid TCP ports and timer overflow', () => {
    assert.throws(() => loadConfig({ PORT: '65536' }), /PORT/);
    assert.throws(() => loadConfig({ API_TIMEOUT_MS: '2147483648' }), /API_TIMEOUT_MS/);
    assert.throws(() => loadConfig({ SHUTDOWN_TIMEOUT_MS: '2147483648' }), /SHUTDOWN_TIMEOUT_MS/);
});

test('validates HEADLESS explicitly', () => {
    assert.equal(loadConfig({ HEADLESS: 'false' }).headless, false);
    assert.equal(loadConfig({ HEADLESS: 'true' }).headless, true);
    assert.throws(() => loadConfig({ HEADLESS: 'flase' }), /HEADLESS/);
});
