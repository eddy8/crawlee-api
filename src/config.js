const MEBIBYTE = 1024 * 1024;
// Leave room for the service's additional navigation and shutdown grace time.
const MAX_TIMEOUT_MS = 2 ** 31 - 1 - 10_000;

export function loadConfig(environment = process.env) {
    const maxConcurrency = readPositiveInteger(
        environment,
        'MAX_CONCURRENCY',
        3,
    );

    return {
        host: environment.HOST || '0.0.0.0',
        port: readPositiveInteger(environment, 'PORT', 3000, 65_535),
        apiTimeoutMs: readPositiveInteger(
            environment,
            'API_TIMEOUT_MS',
            30_000,
            MAX_TIMEOUT_MS,
        ),
        gatekeeperTimeoutMs: readPositiveInteger(
            environment,
            'GATEKEEPER_TIMEOUT_MS',
            15_000,
            MAX_TIMEOUT_MS,
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
            MAX_TIMEOUT_MS,
        ),
        retryAfterSeconds: readPositiveInteger(
            environment,
            'RETRY_AFTER_SECONDS',
            1,
        ),
        headless: readBoolean(environment, 'HEADLESS', true),
        proxyUrls: (environment.PROXY_URLS || '')
            .split(',')
            .map((value) => value.trim())
            .filter(Boolean),
    };
}

function readPositiveInteger(environment, name, fallback, maximum = Number.MAX_SAFE_INTEGER) {
    const raw = String(environment[name] ?? '').trim();
    const value = raw === '' ? fallback : Number(raw);
    if (
        (raw !== '' && !/^\d+$/u.test(raw))
        || !Number.isSafeInteger(value)
        || value <= 0
        || value > maximum
    ) {
        throw new Error(`${name} must be an integer between 1 and ${maximum}`);
    }
    return value;
}

function readBoolean(environment, name, fallback) {
    const value = String(environment[name] ?? '').trim().toLowerCase();
    if (value === '') return fallback;
    if (value === 'true') return true;
    if (value === 'false') return false;
    throw new Error(`${name} must be true or false`);
}
