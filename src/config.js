const MEBIBYTE = 1024 * 1024;
// Leave room for the service's additional navigation and shutdown grace time.
const MAX_TIMEOUT_MS = 2 ** 31 - 1 - 10_000;
const MAX_TIMER_SECONDS = Math.floor((2 ** 31 - 1) / 1000);

export function loadConfig(environment = process.env) {
    const maxConcurrency = readPositiveInteger(
        environment,
        'MAX_CONCURRENCY',
        3,
    );

    const apiTimeoutMs = readPositiveInteger(
        environment,
        'API_TIMEOUT_MS',
        30_000,
        MAX_TIMEOUT_MS,
    );

    return {
        host: environment.HOST || '0.0.0.0',
        port: readPositiveInteger(environment, 'PORT', 3000, 65_535),
        apiTimeoutMs,
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
        // Keep serving while load balancers remove this instance after SIGTERM.
        shutdownDelayMs: readInteger(
            environment,
            'SHUTDOWN_DELAY_MS',
            0,
            0,
            MAX_TIMEOUT_MS,
        ),
        // Must exceed the idle timeout of any proxy or load balancer in front.
        keepAliveTimeoutMs: readPositiveInteger(
            environment,
            'KEEP_ALIVE_TIMEOUT_MS',
            65_000,
            MAX_TIMEOUT_MS,
        ),
        // How long a job may outlive its API deadline before the crawler is
        // considered wedged and liveness fails.
        jobStallGraceMs: readPositiveInteger(
            environment,
            'JOB_STALL_GRACE_MS',
            Math.min(apiTimeoutMs + 60_000, MAX_TIMEOUT_MS),
            MAX_TIMEOUT_MS,
        ),
        retryAfterSeconds: readPositiveInteger(
            environment,
            'RETRY_AFTER_SECONDS',
            1,
        ),
        browserIdleTimeoutSecs: readPositiveInteger(
            environment,
            'BROWSER_IDLE_TIMEOUT_SECS',
            3_600,
            MAX_TIMER_SECONDS,
        ),
        headless: readBoolean(environment, 'HEADLESS', true),
        proxyUrls: (environment.PROXY_URLS || '')
            .split(',')
            .map((value) => value.trim())
            .filter(Boolean),
    };
}

function readPositiveInteger(environment, name, fallback, maximum = Number.MAX_SAFE_INTEGER) {
    return readInteger(environment, name, fallback, 1, maximum);
}

function readInteger(environment, name, fallback, minimum, maximum = Number.MAX_SAFE_INTEGER) {
    const raw = String(environment[name] ?? '').trim();
    const value = raw === '' ? fallback : Number(raw);
    if (
        (raw !== '' && !/^\d+$/u.test(raw))
        || !Number.isSafeInteger(value)
        || value < minimum
        || value > maximum
    ) {
        throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
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
