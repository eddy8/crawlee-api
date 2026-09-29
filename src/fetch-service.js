import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import {
    Configuration,
    ProxyConfiguration,
    PuppeteerCrawler,
    log as defaultLogger,
} from 'crawlee';
import { openTransientQueue } from './transient-queue.js';
import { monitorDocumentSize } from './response-limit.js';

const LATENCY_SAMPLE_LIMIT = 200;
// AutoscaledPool is not woken by addRequests(); an idle slot only picks up new
// work on this tick. Crawlee's 0.5 s default adds ~250 ms mean latency per API call.
const CRAWLER_TASK_POLL_INTERVAL_SECS = 0.05;
// Crawlee's default (0.25 of total memory, including Chromium child processes)
// marks a browser service as overloaded early and silently lowers concurrency
// below the service's own slot accounting. The limit is cgroup-aware; operators
// can override it with CRAWLEE_MEMORY_MBYTES or CRAWLEE_AVAILABLE_MEMORY_RATIO.
const DEFAULT_AVAILABLE_MEMORY_RATIO = 0.8;

class RequestCancelledError extends Error {
    constructor(message = 'Crawler request was cancelled') {
        super(message);
        this.name = 'RequestCancelledError';
    }
}

class RequestDeadlineExceededError extends Error {
    constructor() {
        super('Crawler request exceeded its deadline');
        this.name = 'RequestDeadlineExceededError';
    }
}

class ResponseTooLargeError extends Error {
    constructor(actualBytes, maxBytes) {
        super(
            actualBytes === null
                ? `Upstream response exceeds ${maxBytes} bytes`
                : `Upstream response is ${actualBytes} bytes; limit is ${maxBytes} bytes`,
        );
        this.name = 'ResponseTooLargeError';
        this.actualBytes = actualBytes;
        this.maxBytes = maxBytes;
    }
}

export function createFetchService(
    config,
    { logger = defaultLogger } = {},
) {
    const jobs = new Map();
    const waitingJobIds = new Set();
    const drainWaiters = new Set();
    const metrics = createMetrics();

    let activeCrawlerJobs = 0;
    let crawlerCompletion;
    let crawlerState = 'starting';
    let isPumping = false;
    let isStarted = false;
    let isListening = false;
    let runtimeError = null;
    let shutdownPromise;
    let shuttingDown = false;
    // Set as soon as shutdown is requested; readiness fails while /fetch is
    // still served during SHUTDOWN_DELAY_MS.
    let shutdownAnnounced = false;
    let draining = false;
    let stallWatchdog = null;
    let stallReported = false;
    const crawlerConfiguration = new Configuration({
        persistStorage: false,
        availableMemoryRatio: DEFAULT_AVAILABLE_MEMORY_RATIO,
    });

    const proxyConfiguration = config.proxyUrls.length > 0
        ? new ProxyConfiguration({ proxyUrls: config.proxyUrls })
        : undefined;
    const crawlerLogger = createRedactingLogger(
        logger.child?.({ prefix: 'PuppeteerCrawler' }) || logger,
    );

    const crawler = new PuppeteerCrawler(
        {
            log: crawlerLogger,
            keepAlive: true,
            maxConcurrency: config.maxConcurrency,
            autoscaledPoolOptions: {
                desiredConcurrency: Math.min(config.maxConcurrency, config.maxPendingRequests),
                maybeRunIntervalSecs: CRAWLER_TASK_POLL_INTERVAL_SECS,
            },
            browserPoolOptions: {
                /*
                 * browser-pool retires a browser after 10 s without new pages,
                 * so the next API call after a short lull pays a Chromium cold
                 * start. Keep the warm browser; retireBrowserAfterPageCount
                 * still recycles it periodically.
                 */
                retireInactiveBrowserAfterSecs: config.browserIdleTimeoutSecs,
            },
            maxRequestRetries: 1,
            maxSessionRotations: 2,
            proxyConfiguration,
            useSessionPool: true,
            persistCookiesPerSession: true,
            sessionPoolOptions: {
                /*
                 * The target's first response can legitimately be a 403
                 * JavaScript challenge, so only these statuses retire a
                 * session automatically.
                 */
                blockedStatusCodes: [401, 429],
            },
            navigationTimeoutSecs: Math.max(
                1,
                Math.ceil(config.apiTimeoutMs / 1000),
            ),
            requestHandlerTimeoutSecs: Math.max(
                1,
                Math.ceil(config.apiTimeoutMs / 1000) + 5,
            ),
            preNavigationHooks: [trackNavigation],
            launchContext: {
                launchOptions: {
                    headless: config.headless,
                    handleSIGINT: false,
                    handleSIGTERM: false,
                    handleSIGHUP: false,
                    args: [
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                    ],
                },
            },
            requestHandler: handleCrawlerRequest,
            failedRequestHandler: handleFailedCrawlerRequest,
        },
        crawlerConfiguration,
    );

    const server = createServer((request, response) => {
        handleHttpRequest(request, response).catch((error) => {
            logger.error('Unhandled HTTP request error', {
                error: describeError(error),
            });

            sendJsonSafely(response, 500, {
                success: false,
                code: 'INTERNAL_ERROR',
                error: 'Internal server error',
            });
        });
    });

    server.requestTimeout = config.apiTimeoutMs + 5_000;
    server.headersTimeout = Math.min(
        server.requestTimeout,
        15_000,
    );
    // Must outlive the idle timeout of upstream proxies/load balancers
    // (ALB and nginx default to 60 s), otherwise reused connections race
    // with this server closing them and surface as sporadic 502s.
    server.keepAliveTimeout = config.keepAliveTimeoutMs;
    server.timeout = config.apiTimeoutMs + 5_000;
    server.maxRequestsPerSocket = 100;

    server.on('clientError', (error, socket) => {
        const code = error?.code;

        // With a clientError listener installed Node no longer cleans the
        // socket up itself, so every branch must end in destroy().
        if (
            code === 'ECONNRESET'
            || code === 'EPIPE'
            || !socket.writable
        ) {
            logger.debug('HTTP client connection closed abruptly', {
                code,
            });
            socket.destroy();
            return;
        }

        const status = selectClientErrorStatus(code);
        logger.warning('HTTP client error', {
            code,
            status,
            error: describeError(error),
        });
        socket.end(
            `HTTP/1.1 ${status}\r\n`
            + 'Connection: close\r\n'
            + 'Content-Length: 0\r\n'
            + '\r\n',
            () => socket.destroy(),
        );
    });

    server.on('error', (error) => {
        if (!isListening || shuttingDown) {
            return;
        }

        runtimeError = error;
        logger.error('HTTP server failed', {
            error: describeError(error),
        });
        void shutdown('HTTP server failure', { drain: false });
    });

    async function start() {
        if (isStarted) {
            throw new Error('Fetch service has already been started');
        }

        isStarted = true;

        try {
            crawler.requestQueue = await openTransientQueue(crawlerConfiguration);
            await listen(server, config.port, config.host);
        } catch (error) {
            crawlerState = 'failed';

            throw new Error(
                `Unable to start HTTP server on ${config.host}:${config.port}: ${error.message}`,
                { cause: error },
            );
        }

        isListening = true;
        crawlerState = 'running';
        crawlerCompletion = crawler.run().then(
            () => ({ error: null }),
            (error) => ({ error }),
        );

        stallWatchdog = setInterval(checkForStalledJobs, 5_000);
        stallWatchdog.unref();

        logger.info(
            `Fetch service listening on http://${config.host}:${config.port}`,
        );
        logger.info('Runtime configuration', {
            apiTimeoutMs: config.apiTimeoutMs,
            maxConcurrency: config.maxConcurrency,
            maxPendingRequests: config.maxPendingRequests,
            maxResponseBytes: config.maxResponseBytes,
            browserIdleTimeoutSecs: config.browserIdleTimeoutSecs,
            keepAliveTimeoutMs: config.keepAliveTimeoutMs,
            shutdownDelayMs: config.shutdownDelayMs,
            jobStallGraceMs: config.jobStallGraceMs,
            memoryLimit: crawlerConfiguration.get('memoryMbytes')
                ? `${crawlerConfiguration.get('memoryMbytes')} MB`
                : `${crawlerConfiguration.get('availableMemoryRatio')} of available memory`,
            proxyEnabled: Boolean(proxyConfiguration),
            shutdownTimeoutMs: config.shutdownTimeoutMs,
        });

        scheduleQueuePump();
    }

    async function waitUntilStopped() {
        if (!crawlerCompletion) {
            throw new Error('Fetch service has not been started');
        }

        const { error } = await crawlerCompletion;

        if (error) {
            crawlerState = 'failed';
            throw error;
        }

        crawlerState = 'stopped';

        if (runtimeError) {
            throw runtimeError;
        }

        if (!shuttingDown) {
            throw new Error('Crawler stopped unexpectedly');
        }
    }

    function shutdown(signal, { drain = true } = {}) {
        if (shutdownPromise) {
            return shutdownPromise;
        }

        shutdownPromise = performShutdown(signal, drain);
        return shutdownPromise;
    }

    async function performShutdown(signal, drain) {
        shutdownAnnounced = true;

        /*
         * Orchestrators keep routing to an instance for a few seconds after
         * SIGTERM while endpoint removal propagates. Fail readiness first and
         * keep serving, then stop accepting. SIGINT is excluded because
         * Crawlee aborts its pool on SIGINT by itself.
         */
        if (
            drain
            && signal === 'SIGTERM'
            && config.shutdownDelayMs > 0
            && crawlerState === 'running'
        ) {
            logger.info(`Received ${signal}; failing readiness before shutdown`, {
                delayMs: config.shutdownDelayMs,
            });
            await delay(config.shutdownDelayMs);
        }

        shuttingDown = true;
        crawlerState = 'stopping';
        draining = drain;
        clearInterval(stallWatchdog);

        logger.info(`Received ${signal}; shutting down`, {
            drain,
            jobs: jobs.size,
        });

        const serverClosed = closeServer(server);
        isListening = false;
        server.closeIdleConnections?.();

        let drained = jobs.size === 0;

        if (drain && !drained) {
            scheduleQueuePump();
            drained = await waitForJobsToDrain(
                config.shutdownTimeoutMs,
            );
        }

        draining = false;

        if (!drained) {
            logger.warning('Shutdown drain deadline reached', {
                remainingJobs: jobs.size,
            });
            cancelAllJobsForShutdown();
        }

        if (crawlerCompletion) {
            crawler.stop('Fetch service crawler is stopping.');
            const crawlerResult = await crawlerCompletion;

            if (crawlerResult.error) {
                logger.error('Unable to stop crawler cleanly', {
                    error: describeError(crawlerResult.error),
                });
            }
        }

        for (const job of [...jobs.values()]) {
            settleJob(job);
        }

        server.closeAllConnections?.();
        await Promise.race([
            serverClosed,
            delay(1_000),
        ]);

        crawlerState = 'stopped';
    }

    async function handleHttpRequest(request, response) {
        let requestUrl;
        try {
            // The bind address can be IPv6; it is not a URL parsing base.
            requestUrl = new URL(request.url || '/', 'http://localhost');
        } catch {
            response.setHeader('Connection', 'close');
            sendJsonSafely(response, 400, { error: 'Invalid request URL' });
            return;
        }

        if (shuttingDown) {
            // Let keep-alive clients reconnect to an instance that stays up.
            response.setHeader('Connection', 'close');
        }

        if (
            requestUrl.pathname === '/health'
            || requestUrl.pathname === '/health/ready'
        ) {
            const snapshot = createRuntimeSnapshot();
            sendJsonSafely(
                response,
                snapshot.ready ? 200 : 503,
                snapshot,
            );
            return;
        }

        if (requestUrl.pathname === '/health/live') {
            const stalled = checkForStalledJobs();
            const live = crawlerState !== 'failed' && stalled.count === 0;
            sendJsonSafely(response, live ? 200 : 503, {
                live,
                state: crawlerState,
                stalledJobs: stalled.count,
                oldestStalledJobAgeMs: stalled.oldestAgeMs,
                uptimeSeconds: getUptimeSeconds(),
                timestamp: new Date().toISOString(),
            });
            return;
        }

        if (requestUrl.pathname === '/metrics') {
            sendJsonSafely(response, 200, createMetricsSnapshot());
            return;
        }

        if (requestUrl.pathname !== '/fetch') {
            sendJsonSafely(response, 404, {
                error: 'Not found',
            });
            return;
        }

        if (request.method !== 'GET') {
            response.setHeader('Allow', 'GET');
            sendJsonSafely(response, 405, {
                error: 'Method not allowed',
            });
            return;
        }

        if (shuttingDown || crawlerState !== 'running') {
            metrics.requests.unavailable += 1;
            sendJsonSafely(
                response,
                503,
                {
                    success: false,
                    code: 'SERVICE_UNAVAILABLE',
                    error: 'Crawler service is not ready',
                },
                { 'Retry-After': String(config.retryAfterSeconds) },
            );
            return;
        }

        if (jobs.size >= config.maxPendingRequests) {
            metrics.requests.overloaded += 1;
            sendJsonSafely(
                response,
                503,
                {
                    success: false,
                    code: 'SERVICE_OVERLOADED',
                    error: 'Crawler capacity is currently full',
                },
                { 'Retry-After': String(config.retryAfterSeconds) },
            );
            return;
        }

        const rawTargetUrl = requestUrl.searchParams.get('url');

        if (!rawTargetUrl) {
            sendJsonSafely(response, 400, {
                error: 'Missing "url" query parameter',
            });
            return;
        }

        let targetUrl;

        try {
            targetUrl = validateTargetUrl(rawTargetUrl);
        } catch (error) {
            sendJsonSafely(response, 400, {
                error: error.message,
            });
            return;
        }

        enqueueJob(request, response, targetUrl);
    }

    function enqueueJob(httpRequest, httpResponse, targetUrl) {
        const now = Date.now();
        const id = randomUUID();
        const job = {
            id,
            targetUrl: targetUrl.href,
            safeTarget: formatUrlForLogs(targetUrl),
            acceptedAt: now,
            deadlineAt: now + config.apiTimeoutMs,
            startedAt: null,
            finishedAt: null,
            retryCount: 0,
            state: 'queued',
            submitted: false,
            settled: false,
            cancelled: false,
            httpDone: false,
            httpRequest,
            httpResponse,
            crawlerRequest: null,
            page: null,
            responseListener: null,
            stopSizeMonitor: null,
            latestNavigationResponse: null,
            timer: null,
            removeDisconnectListeners: null,
        };

        const clientDisconnected = () => {
            if (!job.httpDone && !httpResponse.writableEnded) {
                abandonClientRequest(job);
            }
        };

        httpRequest.once('aborted', clientDisconnected);
        httpResponse.once('close', clientDisconnected);
        job.removeDisconnectListeners = () => {
            httpRequest.off('aborted', clientDisconnected);
            httpResponse.off('close', clientDisconnected);
        };
        job.timer = setTimeout(() => {
            expireJob(job);
        }, config.apiTimeoutMs);

        jobs.set(id, job);
        waitingJobIds.add(id);
        metrics.requests.accepted += 1;

        logger.info('Fetch request accepted', {
            requestId: id,
            target: job.safeTarget,
            jobs: jobs.size,
        });

        scheduleQueuePump();
    }

    function expireJob(job) {
        if (job.settled || job.httpDone) {
            return;
        }

        job.cancelled = true;
        finishHttpResponse(
            job,
            504,
            {
                'Content-Type': 'application/json; charset=utf-8',
            },
            JSON.stringify({
                success: false,
                requestId: job.id,
                code: 'REQUEST_TIMEOUT',
                error: 'Crawler request timed out',
                timestamp: new Date().toISOString(),
            }),
            'timed_out',
        );
        cancelCrawlerWork(job);

        if (!job.submitted) {
            settleJob(job);
        }
    }

    function abandonClientRequest(job) {
        if (job.settled || job.httpDone) {
            return;
        }

        job.cancelled = true;
        job.httpDone = true;
        clearTimeout(job.timer);
        job.removeDisconnectListeners?.();
        metrics.requests.cancelled += 1;
        recordJobLatency(job);

        logger.info('Fetch client disconnected', {
            requestId: job.id,
            target: job.safeTarget,
            totalMs: Date.now() - job.acceptedAt,
        });

        cancelCrawlerWork(job);

        if (!job.submitted) {
            settleJob(job);
        }
    }

    function cancelCrawlerWork(job) {
        job.cancelled = true;

        if (job.crawlerRequest) {
            job.crawlerRequest.noRetry = true;
        }

        if (job.page && !job.page.isClosed()) {
            void job.page.close({ runBeforeUnload: false }).catch((error) => {
                logger.debug('Unable to close cancelled browser page', {
                    requestId: job.id,
                    error: describeError(error),
                });
            });
        }
    }

    function scheduleQueuePump() {
        queueMicrotask(() => {
            void pumpQueue();
        });
    }

    function canPumpQueue() {
        return crawlerState === 'running' || (crawlerState === 'stopping' && draining);
    }

    async function pumpQueue() {
        if (
            isPumping
            || !canPumpQueue()
        ) {
            return;
        }

        isPumping = true;

        try {
            while (
                activeCrawlerJobs < config.maxConcurrency
                && waitingJobIds.size > 0
                && canPumpQueue()
            ) {
                const id = waitingJobIds.values().next().value;
                waitingJobIds.delete(id);
                const job = jobs.get(id);

                if (!job || job.settled) {
                    continue;
                }

                if (job.cancelled) {
                    settleJob(job);
                    continue;
                }

                job.submitted = true;
                job.state = 'submitted';
                activeCrawlerJobs += 1;

                try {
                    const result = await crawler.addRequests([
                        {
                            url: job.targetUrl,
                            uniqueKey: job.id,
                        },
                    ]);

                    if (
                        result.addedRequests.length === 0
                        || result.addedRequests[0].wasAlreadyPresent
                    ) {
                        throw new Error(
                            'Crawler did not accept the queued request',
                        );
                    }
                } catch (error) {
                    logger.error('Unable to add request to crawler', {
                        requestId: job.id,
                        target: job.safeTarget,
                        error: describeError(error),
                    });

                    finishHttpResponse(
                        job,
                        503,
                        {
                            'Content-Type':
                                'application/json; charset=utf-8',
                            'Retry-After': String(
                                config.retryAfterSeconds,
                            ),
                        },
                        JSON.stringify({
                            success: false,
                            requestId: job.id,
                            code: 'CRAWLER_UNAVAILABLE',
                            error: 'Crawler could not accept the request',
                        }),
                        'failed',
                    );
                    settleJob(job);
                }
            }
        } finally {
            isPumping = false;

            if (
                activeCrawlerJobs < config.maxConcurrency
                && waitingJobIds.size > 0
                && canPumpQueue()
            ) {
                scheduleQueuePump();
            }
        }
    }

    async function trackNavigation(
        { page, request },
        gotoOptions,
    ) {
        const job = jobs.get(request.uniqueKey);

        if (!job || job.cancelled) {
            request.noRetry = true;
            throw new RequestCancelledError();
        }

        detachPageTracking(job);
        job.crawlerRequest = request;
        job.page = page;
        job.state = 'running';
        job.startedAt ??= Date.now();
        job.retryCount = request.retryCount || 0;
        job.latestNavigationResponse = null;

        const responseListener = (pageResponse) => {
            try {
                const browserRequest = pageResponse.request();

                if (
                    browserRequest.isNavigationRequest()
                    && browserRequest.frame() === page.mainFrame()
                ) {
                    job.latestNavigationResponse = pageResponse;
                }
            } catch (error) {
                logger.debug('Failed to inspect browser response', {
                    requestId: job.id,
                    error: describeError(error),
                });
            }
        };

        job.responseListener = responseListener;
        page.on('response', responseListener);
        job.stopSizeMonitor = await monitorDocumentSize(
            page,
            config.maxResponseBytes,
            () => {
                if (job.httpDone || job.settled) return;
                finishHttpResponse(job, 502, {
                    'Content-Type': 'application/json; charset=utf-8',
                }, JSON.stringify({
                    success: false,
                    requestId: job.id,
                    code: 'UPSTREAM_RESPONSE_TOO_LARGE',
                    error: `Upstream response exceeds the ${config.maxResponseBytes}-byte limit`,
                }), 'response_too_large');
                cancelCrawlerWork(job);
            },
        );
        ensureJobIsActive(job);
        gotoOptions.waitUntil = 'domcontentloaded';
        gotoOptions.timeout = getRemainingTime(job);
    }

    async function handleCrawlerRequest({
        request,
        page,
        response,
        session,
        browserController,
    }) {
        const job = jobs.get(request.uniqueKey);

        if (!job || job.cancelled) {
            request.noRetry = true;

            if (job) {
                settleJob(job);
            }
            return;
        }

        job.retryCount = request.retryCount || 0;

        try {
            const passedChallenge = await waitForGatekeeper(
                page,
                response,
                job,
            );

            if (passedChallenge && session && browserController) {
                const cookies = await browserController.getCookies(page);
                ensureJobIsActive(job);
                session.setCookies(cookies, page.url());
            }

            await page.waitForFunction(
                () => document.readyState !== 'loading',
                {
                    timeout: Math.min(
                        10_000,
                        getRemainingTime(job),
                    ),
                    polling: 100,
                },
            );
            ensureJobIsActive(job);

            const finalResponse =
                job.latestNavigationResponse || response;
            const upstreamStatus = normalizeStatusCode(
                finalResponse?.status?.(),
                200,
            );
            const upstreamHeaders = finalResponse?.headers?.() || {};
            const upstreamContentType =
                upstreamHeaders['content-type'] || '';
            const body = await readFinalResponseBody(
                page,
                finalResponse,
                upstreamContentType,
                config.maxResponseBytes,
            );
            ensureJobIsActive(job);

            finishHttpResponse(
                job,
                upstreamStatus,
                {
                    'Content-Type': selectOutputContentType(
                        upstreamContentType,
                    ),
                    'X-Upstream-Status': String(upstreamStatus),
                    ...createFinalUrlHeaders(page.url()),
                },
                body,
                'succeeded',
            );
            settleJob(job);
        } catch (error) {
            if (
                error instanceof RequestCancelledError
                || error instanceof RequestDeadlineExceededError
                || error instanceof ResponseTooLargeError
            ) {
                request.noRetry = true;
            }

            throw error;
        }
    }

    async function handleFailedCrawlerRequest({ request }, error) {
        const job = jobs.get(request.uniqueKey);

        if (!job) {
            return;
        }

        if (
            job.cancelled
            || error instanceof RequestCancelledError
        ) {
            settleJob(job);
            return;
        }

        if (error instanceof RequestDeadlineExceededError) {
            expireJob(job);
            settleJob(job);
            return;
        }

        const responseTooLarge = error instanceof ResponseTooLargeError;

        logger.error('Crawler request failed', {
            requestId: job.id,
            target: job.safeTarget,
            retries: request.retryCount || 0,
            error: describeError(error),
        });

        finishHttpResponse(
            job,
            502,
            {
                'Content-Type': 'application/json; charset=utf-8',
            },
            JSON.stringify({
                success: false,
                requestId: job.id,
                code: responseTooLarge
                    ? 'UPSTREAM_RESPONSE_TOO_LARGE'
                    : 'CRAWLER_REQUEST_FAILED',
                error: responseTooLarge
                    ? `Upstream response exceeds the ${config.maxResponseBytes}-byte limit`
                    : 'Unable to fetch the upstream URL',
                timestamp: new Date().toISOString(),
            }),
            responseTooLarge ? 'response_too_large' : 'failed',
        );
        settleJob(job);
    }

    async function waitForGatekeeper(page, initialResponse, job) {
        const responseHeaders = initialResponse?.headers?.() || {};
        const pageState = await readGatekeeperState(page);
        ensureJobIsActive(job);

        const isChallengePage =
            responseHeaders['x-gatekeeper-challenge'] === 'required'
            || pageState.title === '正在验证浏览器';

        if (!isChallengePage) {
            return false;
        }

        logger.info('Browser challenge detected; waiting for completion', {
            requestId: job.id,
            target: job.safeTarget,
            state: pageState.bodyState,
        });

        try {
            await page.waitForFunction(
                () => {
                    const challengeStatus = document.querySelector(
                        'body[data-state] #status',
                    );

                    return (
                        document.title !== '正在验证浏览器'
                        && !challengeStatus
                    );
                },
                {
                    timeout: Math.min(
                        config.gatekeeperTimeoutMs,
                        getRemainingTime(job),
                    ),
                    polling: 100,
                },
            );
            ensureJobIsActive(job);

            await page.waitForFunction(
                () => document.readyState !== 'loading',
                {
                    timeout: Math.min(
                        10_000,
                        getRemainingTime(job),
                    ),
                    polling: 100,
                },
            );
            ensureJobIsActive(job);

            logger.info('Browser challenge completed', {
                requestId: job.id,
                target: job.safeTarget,
                title: await page.title(),
            });

            return true;
        } catch (error) {
            if (job.cancelled) {
                throw new RequestCancelledError();
            }

            if (error instanceof RequestDeadlineExceededError) {
                throw error;
            }

            const failedState = await readGatekeeperState(page);
            const details = [
                failedState.statusText,
                failedState.messageText,
                failedState.bodyState
                    ? `state=${failedState.bodyState}`
                    : null,
            ]
                .filter(Boolean)
                .join('; ');

            throw new Error(
                details
                    ? `目标站浏览器验证未完成：${details}`
                    : '目标站浏览器验证未在规定时间内完成',
                { cause: error },
            );
        }
    }

    function finishHttpResponse(
        job,
        statusCode,
        headers,
        body,
        outcome,
    ) {
        if (job.httpDone) {
            return false;
        }

        job.httpDone = true;
        clearTimeout(job.timer);
        job.removeDisconnectListeners?.();

        const bodyBytes = Buffer.byteLength(body || '');
        const responseHeaders = {
            'Cache-Control': 'private, no-store',
            'X-Request-ID': job.id,
            ...(shuttingDown ? { Connection: 'close' } : {}),
            ...headers,
        };

        const responseWritten = writeResponseSafely(
            job.httpResponse,
            normalizeStatusCode(statusCode, 500),
            responseHeaders,
            body,
            logger,
        );

        const recordedOutcome = responseWritten
            ? outcome
            : 'delivery_failed';

        recordOutcome(recordedOutcome, bodyBytes);
        recordJobLatency(job);

        logger.info('Fetch request completed', {
            requestId: job.id,
            target: job.safeTarget,
            outcome: recordedOutcome,
            statusCode,
            responseBytes: bodyBytes,
            queueMs: job.startedAt
                ? job.startedAt - job.acceptedAt
                : null,
            totalMs: Date.now() - job.acceptedAt,
            retries: job.retryCount,
        });

        return true;
    }

    function settleJob(job) {
        if (job.settled) {
            return;
        }

        job.settled = true;
        job.finishedAt = Date.now();
        clearTimeout(job.timer);
        job.removeDisconnectListeners?.();
        detachPageTracking(job);
        jobs.delete(job.id);
        waitingJobIds.delete(job.id);

        if (job.submitted) {
            activeCrawlerJobs = Math.max(0, activeCrawlerJobs - 1);
        }

        if (jobs.size === 0) {
            for (const resolve of drainWaiters) {
                resolve(true);
            }
            drainWaiters.clear();
        }

        scheduleQueuePump();
    }

    function detachPageTracking(job) {
        job.stopSizeMonitor?.();
        job.stopSizeMonitor = null;
        if (job.page && job.responseListener) {
            job.page.off('response', job.responseListener);
        }

        job.responseListener = null;
        job.latestNavigationResponse = null;
    }

    function cancelAllJobsForShutdown() {
        for (const job of [...jobs.values()]) {
            if (!job.httpDone) {
                finishHttpResponse(
                    job,
                    503,
                    {
                        'Content-Type':
                            'application/json; charset=utf-8',
                    },
                    JSON.stringify({
                        success: false,
                        requestId: job.id,
                        code: 'SERVICE_SHUTTING_DOWN',
                        error: 'Service is shutting down',
                    }),
                    'failed',
                );
            }

            job.cancelled = true;
            cancelCrawlerWork(job);

            if (!job.submitted) {
                settleJob(job);
            }
        }
    }

    function waitForJobsToDrain(timeoutMs) {
        if (jobs.size === 0) {
            return Promise.resolve(true);
        }

        return new Promise((resolve) => {
            const onDrained = (value) => {
                clearTimeout(timer);
                drainWaiters.delete(onDrained);
                resolve(value);
            };
            const timer = setTimeout(() => {
                drainWaiters.delete(onDrained);
                resolve(false);
            }, timeoutMs);

            drainWaiters.add(onDrained);
        });
    }

    function ensureJobIsActive(job) {
        if (job.cancelled || job.settled) {
            throw new RequestCancelledError();
        }

        if (Date.now() >= job.deadlineAt) {
            throw new RequestDeadlineExceededError();
        }
    }

    function getRemainingTime(job) {
        ensureJobIsActive(job);
        return Math.max(1, job.deadlineAt - Date.now());
    }

    function createRuntimeSnapshot() {
        const stalled = checkForStalledJobs();
        const ready = (
            crawlerState === 'running'
            && !shutdownAnnounced
            && stalled.count === 0
            && jobs.size < config.maxPendingRequests
        );

        return {
            ok: ready,
            ready,
            state: crawlerState,
            pendingRequests: jobs.size,
            jobs: createJobsSnapshot(),
            uptimeSeconds: getUptimeSeconds(),
            timestamp: new Date().toISOString(),
        };
    }

    function createMetricsSnapshot() {
        return {
            state: crawlerState,
            uptimeSeconds: getUptimeSeconds(),
            requests: { ...metrics.requests },
            jobs: createJobsSnapshot(),
            latencyMs: {
                queue: summarizeSamples(metrics.queueLatencyMs),
                total: summarizeSamples(metrics.totalLatencyMs),
            },
            responseBytes: metrics.responseBytes,
            timestamp: new Date().toISOString(),
        };
    }

    /*
     * A submitted job keeps its crawler slot until Crawlee reports back. If
     * Crawlee wedges (hung page close, stuck browser launch, stalled pool),
     * those slots never return and the service would stay "live" while
     * rejecting all work. Report such jobs so liveness can restart us.
     */
    function checkForStalledJobs(now = Date.now()) {
        let count = 0;
        let oldestAgeMs = null;

        for (const job of jobs.values()) {
            if (job.submitted && now >= job.deadlineAt + config.jobStallGraceMs) {
                count += 1;
                oldestAgeMs = Math.max(oldestAgeMs ?? 0, now - job.acceptedAt);
            }
        }

        if (count > 0 && !stallReported && !shuttingDown) {
            stallReported = true;
            logger.error('Crawler jobs are stalled past their deadline; failing liveness', {
                stalledJobs: count,
                oldestAgeMs,
                jobStallGraceMs: config.jobStallGraceMs,
            });
        } else if (count === 0 && stallReported) {
            stallReported = false;
            logger.warning('Stalled crawler jobs have been released');
        }

        return { count, oldestAgeMs };
    }

    function createJobsSnapshot() {
        let queued = 0;
        let running = 0;
        let awaitingCleanup = 0;

        for (const job of jobs.values()) {
            if (!job.submitted) {
                queued += 1;
            } else if (job.cancelled || job.httpDone) {
                awaitingCleanup += 1;
            } else {
                running += 1;
            }
        }

        return {
            total: jobs.size,
            queued,
            running,
            awaitingCleanup,
            stalled: checkForStalledJobs().count,
            capacity: config.maxPendingRequests,
            crawlerSlotsInUse: activeCrawlerJobs,
            crawlerSlotCapacity: config.maxConcurrency,
            crawlerActiveConcurrency: crawler.autoscaledPool?.currentConcurrency ?? 0,
            crawlerDesiredConcurrency: crawler.autoscaledPool?.desiredConcurrency ?? 0,
        };
    }

    function recordOutcome(outcome, bodyBytes) {
        metrics.requests.completed += 1;
        metrics.responseBytes += bodyBytes;

        if (outcome === 'succeeded') {
            metrics.requests.succeeded += 1;
            return;
        }

        if (outcome === 'timed_out') {
            metrics.requests.timedOut += 1;
            return;
        }

        metrics.requests.failed += 1;

        if (outcome === 'response_too_large') {
            metrics.requests.responseTooLarge += 1;
        }
    }

    function recordJobLatency(job) {
        const now = Date.now();
        recordSample(
            metrics.totalLatencyMs,
            now - job.acceptedAt,
        );

        if (job.startedAt) {
            recordSample(
                metrics.queueLatencyMs,
                job.startedAt - job.acceptedAt,
            );
        }
    }

    function getUptimeSeconds() {
        return Math.floor((Date.now() - metrics.startedAt) / 1000);
    }

    return {
        crawler,
        server,
        shutdown,
        start,
        waitUntilStopped,
    };
}

async function readFinalResponseBody(
    page,
    finalResponse,
    contentType,
    maxResponseBytes,
) {
    const headers = finalResponse?.headers?.() || {};
    assertContentLengthWithinLimit(
        headers['content-length'],
        maxResponseBytes,
    );

    const normalizedType = contentType.toLowerCase();

    if (
        normalizedType.includes('text/html')
        || normalizedType.includes('application/xhtml+xml')
    ) {
        // Serialize once, the same way page.content() does, and bail out in
        // the page before transferring an oversized DOM over CDP.
        const rendered = await page.evaluate((characterLimit) => {
            let content = '';

            for (const node of document.childNodes) {
                content += node === document.documentElement
                    ? node.outerHTML
                    : new XMLSerializer().serializeToString(node);

                // UTF-8 needs at least one byte per UTF-16 code unit.
                if (content.length > characterLimit) {
                    return { content: null, tooLarge: true };
                }
            }

            return { content, tooLarge: false };
        }, maxResponseBytes);

        if (rendered.tooLarge) {
            throw new ResponseTooLargeError(null, maxResponseBytes);
        }

        return assertBodyWithinLimit(rendered.content, maxResponseBytes);
    }

    if (finalResponse) {
        try {
            return assertBodyWithinLimit(
                await finalResponse.text(),
                maxResponseBytes,
            );
        } catch (error) {
            if (error instanceof ResponseTooLargeError) {
                throw error;
            }
        }
    }

    const rendered = await page.evaluate((characterLimit) => {
        const pre = document.querySelector('pre');
        const text = pre
            ? pre.textContent || ''
            : document.body?.innerText || '';

        return {
            text: text.slice(0, characterLimit + 1),
            truncated: text.length > characterLimit,
        };
    }, maxResponseBytes);

    if (rendered.truncated) {
        throw new ResponseTooLargeError(null, maxResponseBytes);
    }

    return assertBodyWithinLimit(
        rendered.text,
        maxResponseBytes,
    );
}

function assertContentLengthWithinLimit(value, maxBytes) {
    if (!value) {
        return;
    }

    const contentLength = Number.parseInt(value, 10);

    if (Number.isFinite(contentLength) && contentLength > maxBytes) {
        throw new ResponseTooLargeError(contentLength, maxBytes);
    }
}

function assertBodyWithinLimit(body, maxBytes) {
    const bodyBytes = Buffer.byteLength(body);

    if (bodyBytes > maxBytes) {
        throw new ResponseTooLargeError(bodyBytes, maxBytes);
    }

    return body;
}

function selectOutputContentType(upstreamContentType) {
    const normalizedType = upstreamContentType.toLowerCase();

    if (normalizedType.includes('json')) {
        return 'application/json; charset=utf-8';
    }

    if (normalizedType.startsWith('text/plain')) {
        return 'text/plain; charset=utf-8';
    }

    if (
        normalizedType.includes('xml')
        && !normalizedType.includes('html')
    ) {
        return 'application/xml; charset=utf-8';
    }

    return 'text/html; charset=utf-8';
}

function selectClientErrorStatus(code) {
    if (code === 'ERR_HTTP_REQUEST_TIMEOUT') {
        return '408 Request Timeout';
    }

    if (code === 'HPE_HEADER_OVERFLOW') {
        return '431 Request Header Fields Too Large';
    }

    return '400 Bad Request';
}

function createFinalUrlHeaders(finalUrl) {
    const encodedUrl = encodeURIComponent(finalUrl);

    if (Buffer.byteLength(encodedUrl) > 4_096) {
        return {
            'X-Final-URL-Omitted': 'too-long',
        };
    }

    return {
        'X-Final-URL': encodedUrl,
    };
}

async function readGatekeeperState(page) {
    try {
        return await page.evaluate(() => ({
            title: document.title,
            bodyState: document.body?.getAttribute('data-state') || null,
            hasStatusElement: Boolean(document.getElementById('status')),
            statusText:
                document.getElementById('status')?.textContent?.trim()
                || null,
            messageText:
                document.getElementById('msg')?.textContent?.trim()
                || null,
        }));
    } catch {
        return {
            title: '',
            bodyState: null,
            hasStatusElement: false,
            statusText: null,
            messageText: null,
        };
    }
}

function validateTargetUrl(value) {
    let targetUrl;

    try {
        targetUrl = new URL(value);
    } catch {
        throw new Error('Invalid target URL');
    }

    if (
        targetUrl.protocol !== 'http:'
        && targetUrl.protocol !== 'https:'
    ) {
        throw new Error('Only HTTP and HTTPS URLs are supported');
    }

    if (targetUrl.username || targetUrl.password) {
        throw new Error('URLs containing credentials are not supported');
    }

    return targetUrl;
}

function sendJsonSafely(response, statusCode, value, headers = {}) {
    if (response.writableEnded || response.destroyed) {
        return false;
    }

    return writeResponseSafely(
        response,
        statusCode,
        {
            'Content-Type': 'application/json; charset=utf-8',
            'Cache-Control': 'private, no-store',
            ...headers,
        },
        JSON.stringify(value),
    );
}

function writeResponseSafely(
    response,
    statusCode,
    headers,
    body,
    logger = defaultLogger,
) {
    if (response.writableEnded || response.destroyed) {
        return false;
    }

    try {
        response.writeHead(statusCode, headers);
        response.end(body);
        return true;
    } catch (error) {
        logger.error('Unable to write HTTP response', {
            error: describeError(error),
        });
        response.destroy(error);
        return false;
    }
}

function normalizeStatusCode(value, fallback) {
    const statusCode = Number(value);

    if (
        Number.isInteger(statusCode)
        && statusCode >= 200
        && statusCode <= 599
    ) {
        return statusCode;
    }

    return fallback;
}

function formatUrlForLogs(url) {
    const suffix = url.search || url.hash ? '?[redacted]' : '';
    return `${url.origin}${url.pathname}${suffix}`;
}

function describeError(error) {
    if (!(error instanceof Error)) {
        return redactUrls(String(error || 'Unknown error'));
    }

    return {
        name: error.name,
        message: redactUrls(error.message),
    };
}

function redactUrls(value) {
    return value.replace(/https?:\/\/\S+/gu, '[redacted-url]');
}

function createRedactingLogger(logger) {
    const redactedMethods = new Set([
        'debug',
        'deprecated',
        'error',
        'info',
        'internal',
        'perf',
        'softFail',
        'warning',
        'warningOnce',
    ]);

    return new Proxy(logger, {
        get(target, property, receiver) {
            if (property === 'child') {
                return (options) => createRedactingLogger(
                    target.child(options),
                );
            }

            const value = Reflect.get(target, property, receiver);

            if (typeof value !== 'function') {
                return value;
            }

            if (property === 'exception') {
                return (error, message, data) => value.call(
                    target,
                    sanitizeError(error),
                    scrubLogValue(message),
                    scrubLogValue(data),
                );
            }

            if (redactedMethods.has(property)) {
                return (...arguments_) => value.apply(
                    target,
                    arguments_.map((argument) => (
                        scrubLogValue(argument)
                    )),
                );
            }

            return value.bind(target);
        },
    });
}

function sanitizeError(error) {
    if (!(error instanceof Error)) {
        return error;
    }

    const sanitized = new Error(redactUrls(error.message));
    sanitized.name = error.name;

    if (error.stack) {
        sanitized.stack = redactUrls(error.stack);
    }

    return sanitized;
}

function scrubLogValue(value, seen = new WeakSet(), depth = 0) {
    if (typeof value === 'string') {
        return redactUrls(value);
    }

    if (
        value === null
        || value === undefined
        || typeof value !== 'object'
    ) {
        return value;
    }

    if (value instanceof Error) {
        return sanitizeError(value);
    }

    if (seen.has(value) || depth >= 5) {
        return '[nested value omitted]';
    }

    seen.add(value);

    if (Array.isArray(value)) {
        return value.map((item) => (
            scrubLogValue(item, seen, depth + 1)
        ));
    }

    return Object.fromEntries(
        Object.entries(value).map(([key, item]) => [
            key,
            scrubLogValue(item, seen, depth + 1),
        ]),
    );
}

function createMetrics() {
    return {
        startedAt: Date.now(),
        requests: {
            accepted: 0,
            completed: 0,
            succeeded: 0,
            failed: 0,
            timedOut: 0,
            cancelled: 0,
            overloaded: 0,
            unavailable: 0,
            responseTooLarge: 0,
        },
        queueLatencyMs: [],
        totalLatencyMs: [],
        responseBytes: 0,
    };
}

function recordSample(samples, value) {
    samples.push(value);

    if (samples.length > LATENCY_SAMPLE_LIMIT) {
        samples.shift();
    }
}

function summarizeSamples(samples) {
    if (samples.length === 0) {
        return {
            count: 0,
            average: null,
            p50: null,
            p95: null,
            max: null,
        };
    }

    const sorted = [...samples].sort((left, right) => left - right);
    const total = sorted.reduce((sum, value) => sum + value, 0);

    return {
        count: sorted.length,
        average: Math.round(total / sorted.length),
        p50: percentile(sorted, 0.5),
        p95: percentile(sorted, 0.95),
        max: sorted.at(-1),
    };
}

function percentile(sorted, ratio) {
    const index = Math.min(
        sorted.length - 1,
        Math.ceil(sorted.length * ratio) - 1,
    );
    return sorted[Math.max(0, index)];
}

function listen(server, port, host) {
    return new Promise((resolve, reject) => {
        const onError = (error) => {
            server.off('listening', onListening);
            reject(error);
        };
        const onListening = () => {
            server.off('error', onError);
            resolve();
        };

        server.once('error', onError);
        server.once('listening', onListening);
        server.listen(port, host);
    });
}

function closeServer(server) {
    return new Promise((resolve) => {
        if (!server.listening) {
            resolve();
            return;
        }

        server.close(() => resolve());
    });
}

function delay(milliseconds) {
    return new Promise((resolve) => {
        setTimeout(resolve, milliseconds);
    });
}
