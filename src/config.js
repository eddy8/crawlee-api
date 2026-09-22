const MEBIBYTE = 1024 * 1024;

export function loadConfig(environment = process.env) {
    const maxConcurrency = readPositiveInteger(
        environment,
        'MAX_CONCURRENCY',
        3,
    );

    return {
        host: environment.HOST || '0.0.0.0',
        port: readPositiveInteger(environment, 'PORT', 3000),
        apiTimeoutMs: readPositiveInteger(
            environment,
            'API_TIMEOUT_MS',
            30_000,
        ),
        gatekeeperTimeoutMs: readPositiveInteger(
            environment,
            'GATEKEEPER_TIMEOUT_MS',
            15_000,
        ),
        maxConcurrency,
        maxPendingRequests: readPositiveInteger(
            environment,
            'MAX_PENDING_REQUESTS',
            maxConcurrency * 4,
        ),
        maxResponseBytes: readPositiveInteger(
            environment,
            'MAX_RESPONSE_BYTES',
            5 * MEBIBYTE,
        ),
        shutdownTimeoutMs: readPositiveInteger(
            environment,
            'SHUTDOWN_TIMEOUT_MS',
            30_000,
        ),
        retryAfterSeconds: readPositiveInteger(
            environment,
            'RETRY_AFTER_SECONDS',
            1,
        ),
        headless: environment.HEADLESS !== 'false',
        proxyUrls: (environment.PROXY_URLS || '')
            .split(',')
            .map((value) => value.trim())
            .filter(Boolean),
    };
}

function readPositiveInteger(environment, name, fallback) {
    const value = Number.parseInt(environment[name] || '', 10);

    return Number.isInteger(value) && value > 0
        ? value
        : fallback;
}
