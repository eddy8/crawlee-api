import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { after, before, test } from 'node:test';

const projectDirectory = new URL('../', import.meta.url);

let application;
let applicationOutput = '';
let applicationPort;
let upstream;
let upstreamPort;
let slowRequestsAborted = 0;
let slowRequestsStarted = 0;
let delayRequestsStarted = 0;

before(async () => {
    upstream = createServer((request, response) => {
        if (request.url === '/small') {
            response.writeHead(200, {
                'Content-Type': 'text/html; charset=utf-8',
            });
            response.end('<!doctype html><title>small</title><p>ok</p>');
            return;
        }

        if (request.url === '/large') {
            response.writeHead(200, {
                'Content-Type': 'text/plain; charset=utf-8',
                'Content-Length': '2048',
            });
            response.end('x'.repeat(2048));
            return;
        }

        if (request.url?.startsWith('/fail')) {
            request.socket.destroy();
            return;
        }

        if (request.url === '/slow') {
            slowRequestsStarted += 1;

            const timer = setTimeout(() => {
                response.end('too late');
            }, 10_000);

            response.once('close', () => {
                clearTimeout(timer);

                if (!response.writableEnded) {
                    slowRequestsAborted += 1;
                }
            });
            return;
        }

        if (request.url === '/delay') {
            delayRequestsStarted += 1;

            setTimeout(() => {
                response.writeHead(200, {
                    'Content-Type': 'text/plain; charset=utf-8',
                });
                response.end('finished');
            }, 500);
            return;
        }

        response.writeHead(404);
        response.end('not found');
    });

    upstreamPort = await listenOnRandomPort(upstream);
    applicationPort = await reservePort();

    application = spawn(
        process.execPath,
        ['main.js'],
        {
            cwd: projectDirectory,
            env: {
                ...process.env,
                HOST: '127.0.0.1',
                PORT: String(applicationPort),
                API_TIMEOUT_MS: '1500',
                GATEKEEPER_TIMEOUT_MS: '500',
                MAX_CONCURRENCY: '1',
                MAX_PENDING_REQUESTS: '2',
                MAX_RESPONSE_BYTES: '1024',
                SHUTDOWN_TIMEOUT_MS: '2000',
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        },
    );

    application.stdout.on('data', (chunk) => {
        applicationOutput += chunk;
    });
    application.stderr.on('data', (chunk) => {
        applicationOutput += chunk;
    });

    await waitForApplication();
});

after(async () => {
    if (application && application.exitCode === null) {
        application.kill('SIGKILL');
        await waitForExit(application);
    }

    if (upstream) {
        await closeServer(upstream);
    }
});

test('reports live and ready runtime state', async () => {
    const liveResponse = await fetch(applicationUrl('/health/live'));
    assert.equal(liveResponse.status, 200);
    assert.equal((await liveResponse.json()).state, 'running');

    const readyResponse = await fetch(applicationUrl('/health/ready'));
    assert.equal(readyResponse.status, 200);
    const readyBody = await readyResponse.json();
    assert.equal(readyBody.ready, true);
    assert.equal(readyBody.pendingRequests, 0);
});

test('fetches a rendered page and returns a request id', async () => {
    const response = await fetchTarget('/small');

    assert.equal(response.status, 200);
    assert.match(response.headers.get('x-request-id'), /^[0-9a-f-]{36}$/);
    assert.match(await response.text(), /<p>ok<\/p>/);
});

test('rejects an oversized upstream response', async () => {
    const response = await fetchTarget('/large');
    const body = await response.json();

    assert.equal(response.status, 502);
    assert.equal(body.code, 'UPSTREAM_RESPONSE_TOO_LARGE');
});

test('redacts URL query secrets from crawler logs', async () => {
    const secret = 'super-secret-query-value';
    const response = await fetchTarget(`/fail?token=${secret}`);

    assert.equal(response.status, 502);
    assert.doesNotMatch(applicationOutput, new RegExp(secret));
});

test('cancels browser work when the API deadline expires', async () => {
    const abortedBeforeRequest = slowRequestsAborted;
    const responsePromise = fetchTarget('/slow');
    await waitFor(() => slowRequestsStarted > 0);
    const response = await responsePromise;

    assert.equal(response.status, 504);
    assert.equal((await response.json()).code, 'REQUEST_TIMEOUT');

    await withTimeout(
        waitFor(() => slowRequestsAborted > abortedBeforeRequest),
        2_000,
        'The upstream browser request was not aborted',
    );
});

test('rejects excess work and exposes saturation metrics', async () => {
    const first = fetchTarget('/slow');
    await waitForPendingCount(1);

    const second = fetchTarget('/slow');
    await waitForPendingCount(2);

    const readyResponse = await fetch(applicationUrl('/health/ready'));
    assert.equal(readyResponse.status, 503);

    const overloadedResponse = await fetchTarget('/small');
    assert.equal(overloadedResponse.status, 503);
    assert.equal(
        (await overloadedResponse.json()).code,
        'SERVICE_OVERLOADED',
    );
    assert.equal(overloadedResponse.headers.get('retry-after'), '1');

    const [firstResponse, secondResponse] = await Promise.all([
        first,
        second,
    ]);
    assert.equal(firstResponse.status, 504);
    assert.equal(secondResponse.status, 504);

    const metricsResponse = await fetch(applicationUrl('/metrics'));
    const metrics = await metricsResponse.json();
    assert.ok(metrics.requests.timedOut >= 3);
    assert.ok(metrics.requests.overloaded >= 1);
    assert.ok(metrics.requests.responseTooLarge >= 1);
});

test('drains an in-flight request before shutting down', async () => {
    const startedBeforeRequest = delayRequestsStarted;
    const fetchPromise = fetchTarget('/delay');
    await withTimeout(
        waitFor(() => delayRequestsStarted > startedBeforeRequest),
        2_000,
        'The delayed request did not start',
    );

    application.kill('SIGTERM');

    const response = await fetchPromise;
    assert.equal(response.status, 200, applicationOutput);
    assert.equal(await response.text(), 'finished');

    const exit = await waitForExit(application);
    assert.equal(exit.code, 0, applicationOutput);
});

test('reports a controlled error when the listen port is unavailable', async () => {
    const occupiedServer = createServer();
    const occupiedPort = await listenOnRandomPort(occupiedServer);
    const child = spawn(
        process.execPath,
        ['main.js'],
        {
            cwd: projectDirectory,
            env: {
                ...process.env,
                HOST: '127.0.0.1',
                PORT: String(occupiedPort),
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        },
    );
    let output = '';
    child.stdout.on('data', (chunk) => {
        output += chunk;
    });
    child.stderr.on('data', (chunk) => {
        output += chunk;
    });

    const exit = await withTimeout(
        waitForExit(child),
        5_000,
        'The process did not exit after a listen failure',
    );
    await closeServer(occupiedServer);

    assert.equal(exit.code, 1);
    assert.match(output, /Unable to start HTTP server/);
    assert.doesNotMatch(output, /Unhandled 'error' event/);
});

test('forces shutdown after the configured drain deadline', async () => {
    const port = await reservePort();
    const child = spawn(
        process.execPath,
        ['main.js'],
        {
            cwd: projectDirectory,
            env: {
                ...process.env,
                HOST: '127.0.0.1',
                PORT: String(port),
                API_TIMEOUT_MS: '10000',
                MAX_CONCURRENCY: '1',
                MAX_PENDING_REQUESTS: '1',
                SHUTDOWN_TIMEOUT_MS: '500',
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        },
    );
    let output = '';
    child.stdout.on('data', (chunk) => {
        output += chunk;
    });
    child.stderr.on('data', (chunk) => {
        output += chunk;
    });

    try {
        await withTimeout(
            waitFor(async () => {
                try {
                    const response = await fetch(
                        `http://127.0.0.1:${port}/health/live`,
                    );
                    return response.ok;
                } catch {
                    return false;
                }
            }),
            10_000,
            `Deadline test service did not start:\n${output}`,
        );

        const startedBeforeRequest = slowRequestsStarted;
        const query = new URLSearchParams({
            url: upstreamUrl('/slow'),
        });
        const fetchPromise = fetch(
            `http://127.0.0.1:${port}/fetch?${query}`,
        );
        await withTimeout(
            waitFor(() => slowRequestsStarted > startedBeforeRequest),
            3_000,
            'The deadline test request did not start',
        );

        const shutdownStartedAt = Date.now();
        child.kill('SIGTERM');

        const response = await fetchPromise;
        const body = await response.json();
        const exit = await withTimeout(
            waitForExit(child),
            2_000,
            `The deadline test service did not exit:\n${output}`,
        );

        assert.equal(response.status, 503);
        assert.equal(body.code, 'SERVICE_SHUTTING_DOWN');
        assert.equal(exit.code, 0, output);
        assert.ok(Date.now() - shutdownStartedAt < 2_000);
    } finally {
        if (child.exitCode === null) {
            child.kill('SIGKILL');
            await waitForExit(child);
        }
    }
});

function applicationUrl(pathname) {
    return `http://127.0.0.1:${applicationPort}${pathname}`;
}

function upstreamUrl(pathname) {
    return `http://127.0.0.1:${upstreamPort}${pathname}`;
}

function fetchTarget(pathname) {
    const query = new URLSearchParams({
        url: upstreamUrl(pathname),
    });

    return fetch(applicationUrl(`/fetch?${query}`));
}

async function waitForApplication() {
    await withTimeout(
        waitFor(async () => {
            if (application.exitCode !== null) {
                throw new Error(
                    `Application exited during startup:\n${applicationOutput}`,
                );
            }

            try {
                const response = await fetch(applicationUrl('/health/live'));
                return response.ok;
            } catch {
                return false;
            }
        }),
        15_000,
        `Application did not start:\n${applicationOutput}`,
    );
}

async function waitForPendingCount(expected) {
    await withTimeout(
        waitFor(async () => {
            const response = await fetch(applicationUrl('/metrics'));
            const metrics = await response.json();
            return metrics.jobs.total === expected;
        }),
        2_000,
        `Pending request count did not reach ${expected}`,
    );
}

async function waitFor(predicate) {
    while (!await predicate()) {
        await new Promise((resolve) => setTimeout(resolve, 25));
    }
}

async function reservePort() {
    const server = createServer();
    const port = await listenOnRandomPort(server);
    await closeServer(server);
    return port;
}

function listenOnRandomPort(server) {
    return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => {
            server.off('error', reject);
            resolve(server.address().port);
        });
    });
}

function closeServer(server) {
    return new Promise((resolve, reject) => {
        server.close((error) => {
            if (error) {
                reject(error);
                return;
            }

            resolve();
        });
        server.closeAllConnections?.();
    });
}

function waitForExit(child) {
    if (child.exitCode !== null) {
        return Promise.resolve({
            code: child.exitCode,
            signal: child.signalCode,
        });
    }

    return new Promise((resolve) => {
        child.once('exit', (code, signal) => {
            resolve({ code, signal });
        });
    });
}

function withTimeout(promise, timeoutMs, message) {
    let timer;

    return Promise.race([
        promise,
        new Promise((_, reject) => {
            timer = setTimeout(() => {
                reject(new Error(message));
            }, timeoutMs);
        }),
    ]).finally(() => {
        clearTimeout(timer);
    });
}
