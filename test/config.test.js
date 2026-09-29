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

test('keeps idle browsers warm by default and validates the idle timeout', () => {
    assert.equal(loadConfig({}).browserIdleTimeoutSecs, 3_600);
    assert.equal(loadConfig({ BROWSER_IDLE_TIMEOUT_SECS: '120' }).browserIdleTimeoutSecs, 120);
    assert.throws(() => loadConfig({ BROWSER_IDLE_TIMEOUT_SECS: '0' }), /BROWSER_IDLE_TIMEOUT_SECS/);
    assert.throws(() => loadConfig({ BROWSER_IDLE_TIMEOUT_SECS: '2147484' }), /BROWSER_IDLE_TIMEOUT_SECS/);
});

test('reads shutdown delay, keep-alive and stall settings', () => {
    const defaults = loadConfig({});
    assert.equal(defaults.shutdownDelayMs, 0);
    assert.equal(defaults.keepAliveTimeoutMs, 65_000);
    assert.equal(defaults.jobStallGraceMs, 90_000);
    assert.equal(loadConfig({ API_TIMEOUT_MS: '5000' }).jobStallGraceMs, 65_000);
    assert.equal(loadConfig({ SHUTDOWN_DELAY_MS: '0' }).shutdownDelayMs, 0);
    assert.equal(loadConfig({ SHUTDOWN_DELAY_MS: '5000' }).shutdownDelayMs, 5_000);
    assert.throws(() => loadConfig({ SHUTDOWN_DELAY_MS: '-1' }), /SHUTDOWN_DELAY_MS/);
    assert.throws(() => loadConfig({ KEEP_ALIVE_TIMEOUT_MS: '0' }), /KEEP_ALIVE_TIMEOUT_MS/);
    assert.throws(() => loadConfig({ JOB_STALL_GRACE_MS: '1.5' }), /JOB_STALL_GRACE_MS/);
});
