// fetch-service.mjs

import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import {
    Configuration,
    ProxyConfiguration,
    PuppeteerCrawler,
    log,
} from 'crawlee';

const HOST = process.env.HOST || '0.0.0.0';
const PORT = readPositiveInteger('PORT', 3000);

const API_TIMEOUT_MS = readPositiveInteger(
    'API_TIMEOUT_MS',
    30_000,
);

const GATEKEEPER_TIMEOUT_MS = readPositiveInteger(
    'GATEKEEPER_TIMEOUT_MS',
    15_000,
);

const MAX_CONCURRENCY = readPositiveInteger(
    'MAX_CONCURRENCY',
    2,
);

const HEADLESS = process.env.HEADLESS !== 'false';
const ALLOW_ALL_TARGETS = process.env.ALLOW_ALL_TARGETS === '1';

// 默认只允许当前目标站。
// 如需多个域名：ALLOWED_HOSTS=cn.wa01.com,example.com,*.example.org
const allowedHostPatterns = (
    process.env.ALLOWED_HOSTS || 'cn.wa01.com'
)
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);

const proxyUrls = (process.env.PROXY_URLS || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);

const proxyConfiguration = proxyUrls.length > 0
    ? new ProxyConfiguration({ proxyUrls })
    : undefined;

const chromeArgs = [];

//if (process.env.CHROME_NO_SANDBOX === '1') {
    chromeArgs.push(
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
    );
//}

/**
 * uniqueKey -> {
 *   response: ServerResponse,
 *   timer: Timeout
 * }
 */
const pendingRequests = new Map();

/**
 * 记录每个页面最近一次主框架导航响应。
 * 目标站完成 Gatekeeper 后会再次导航，Crawlee context 中的 response
 * 仍可能是最初的 403，因此需要单独记录最终的 200 响应。
 */
const latestNavigationResponses = new WeakMap();

const crawler = new PuppeteerCrawler(
    {
        keepAlive: true,
        maxConcurrency: MAX_CONCURRENCY,
        maxRequestRetries: 1,
        maxSessionRotations: 2,

        proxyConfiguration,

        useSessionPool: true,
        persistCookiesPerSession: true,

        sessionPoolOptions: {
            /*
             * 关键修改：
             *
             * 不能把 403 直接视为 Session 被封禁，因为目标站首次访问
             * 正常就会返回 403，并要求浏览器运行 JavaScript 挑战。
             */
            blockedStatusCodes: [401, 429],
        },

        navigationTimeoutSecs: 60,

        requestHandlerTimeoutSecs: Math.ceil(
            (GATEKEEPER_TIMEOUT_MS + 15_000) / 1000,
        ),

        preNavigationHooks: [
            async ({ page }, gotoOptions) => {
                latestNavigationResponses.delete(page);

                page.on('response', (pageResponse) => {
                    try {
                        const browserRequest = pageResponse.request();

                        if (
                            browserRequest.isNavigationRequest()
                            && browserRequest.frame() === page.mainFrame()
                        ) {
                            latestNavigationResponses.set(
                                page,
                                pageResponse,
                            );
                        }
                    } catch (error) {
                        log.debug(
                            'Failed to inspect browser response',
                            { error },
                        );
                    }
                });

                gotoOptions.waitUntil = 'domcontentloaded';

                /*
                 * 不要在这里：
                 *
                 * 1. 硬编码 User-Agent；
                 * 2. 修改 navigator.webdriver；
                 * 3. 阻止 stylesheet、script 或 worker；
                 * 4. 覆盖 Crawlee 默认生成的浏览器指纹。
                 *
                 * Gatekeeper 需要 JavaScript、Web Worker、Web Crypto 和 Cookie。
                 */
            },
        ],

        launchContext: {
            launchOptions: {
                headless: HEADLESS,
                args: chromeArgs,
            },
        },

        requestHandler: async ({
            request,
            page,
            response,
            session,
            browserController,
        }) => {
            if (!pendingRequests.has(request.uniqueKey)) {
                // HTTP 客户端可能已经断开或请求已经超时。
                return;
            }

            const passedChallenge = await waitForGatekeeper(
                page,
                response,
            );

            /*
             * Crawlee 会在初次导航后保存 Cookie，但 Gatekeeper 的通过
             * Cookie 是在后续异步验证中产生的，因此这里再同步一次。
             */
            if (
                passedChallenge
                && session
                && browserController
            ) {
                const cookies = await browserController.getCookies(page);

                session.setCookies(
                    cookies,
                    page.url(),
                );
            }

            // 确保最终页面的 DOM 已经完成解析。
            await page.waitForFunction(
                () => document.readyState !== 'loading',
                {
                    timeout: 10_000,
                    polling: 100,
                },
            );
const finalResponse =
    latestNavigationResponses.get(page)
    || response;

const upstreamStatus = normalizeStatusCode(
    finalResponse?.status?.(),
    200,
);

const upstreamHeaders =
    finalResponse?.headers?.() || {};

const upstreamContentType =
    upstreamHeaders['content-type'] || '';

const body = await readFinalResponseBody(
    page,
    finalResponse,
    upstreamContentType,
);

finishPendingRequest(
    request.uniqueKey,
    upstreamStatus,
    {
        'Content-Type': selectOutputContentType(
            upstreamContentType,
        ),
        'X-Upstream-Status': String(upstreamStatus),
        'X-Final-URL': encodeURIComponent(page.url()),
    },
    body,
);
        },

        failedRequestHandler: async (
            { request },
            error,
        ) => {
            const message = error instanceof Error
                ? error.message
                : String(error || 'Unknown crawler error');

            log.error(
                `Request failed: ${request.url}`,
                { error: message },
            );

            finishPendingRequest(
                request.uniqueKey,
                502,
                {
                    'Content-Type':
                        'application/json; charset=utf-8',
                },
                JSON.stringify({
                    success: false,
                    url: request.url,
                    error: message,
                    html: null,
                    timestamp: new Date().toISOString(),
                }),
            );
        },
    },
    new Configuration({
        persistStorage: false,
    }),
);

async function readFinalResponseBody(
    page,
    finalResponse,
    contentType,
) {
    const normalizedType =
        contentType.toLowerCase();

    // 普通网页返回执行 JavaScript 后的完整 DOM。
    if (
        normalizedType.includes('text/html')
        || normalizedType.includes(
            'application/xhtml+xml',
        )
    ) {
        return page.content();
    }

    /*
     * JSON、纯文本等接口直接读取原始响应体，
     * 避免 page.content() 将 JSON 包装成：
     *
     * <html><body><pre>...</pre></body></html>
     */
    if (finalResponse) {
        try {
            return await finalResponse.text();
        } catch (error) {
            log.warning(
                'Unable to read raw response body; using rendered text',
                {
                    error: error.message,
                    url: page.url(),
                },
            );
        }
    }

    // 某些 Chromium 版本会把 JSON 渲染到 <pre> 中。
    return page.evaluate(() => {
        const pre = document.querySelector('pre');

        if (pre) {
            return pre.textContent || '';
        }

        return document.body?.innerText || '';
    });
}

function selectOutputContentType(upstreamContentType) {
    const normalizedType =
        upstreamContentType.toLowerCase();

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
/**
 * 检测并等待 cn.wa01.com 的 Gatekeeper 验证。
 */
async function waitForGatekeeper(page, initialResponse) {
    const responseHeaders =
        initialResponse?.headers?.() || {};

    const pageState = await readGatekeeperState(page);

    const isChallengePage =
        responseHeaders['x-gatekeeper-challenge'] === 'required'
        || pageState.title === '正在验证浏览器'
        || (
            pageState.hasStatusElement
            && pageState.bodyState
        );

    if (!isChallengePage) {
        return false;
    }

    log.info('Browser challenge detected; waiting for completion', {
        url: page.url(),
        state: pageState.bodyState,
    });

    try {
        /*
         * 挑战成功后，目标脚本会调用 location.replace(returnUrl)。
         * waitForFunction 可以跨越这次页面导航继续等待。
         */
        await page.waitForFunction(
            () => {
                const challengeStatus =
                    document.querySelector(
                        'body[data-state] #status',
                    );

                return (
                    document.title !== '正在验证浏览器'
                    && !challengeStatus
                );
            },
            {
                timeout: GATEKEEPER_TIMEOUT_MS,
                polling: 100,
            },
        );

        await page.waitForFunction(
            () => document.readyState !== 'loading',
            {
                timeout: 10_000,
                polling: 100,
            },
        );

        log.info('Browser challenge completed', {
            url: page.url(),
            title: await page.title(),
        });

        return true;
    } catch (error) {
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

async function readGatekeeperState(page) {
    try {
        return await page.evaluate(() => ({
            title: document.title,
            bodyState:
                document.body?.getAttribute('data-state')
                || null,
            hasStatusElement:
                Boolean(document.getElementById('status')),
            statusText:
                document.getElementById('status')
                    ?.textContent
                    ?.trim()
                || null,
            messageText:
                document.getElementById('msg')
                    ?.textContent
                    ?.trim()
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

const server = createServer((request, response) => {
    handleHttpRequest(request, response).catch((error) => {
        log.error('Unhandled HTTP request error', { error });

        if (
            !response.writableEnded
            && !response.destroyed
        ) {
            sendJson(response, 500, {
                success: false,
                error: 'Internal server error',
            });
        }
    });
});

async function handleHttpRequest(request, response) {
    const requestUrl = new URL(
        request.url || '/',
        `http://${request.headers.host || `${HOST}:${PORT}`}`,
    );

    if (requestUrl.pathname === '/health') {
        sendJson(response, 200, {
            ok: true,
            pendingRequests: pendingRequests.size,
            timestamp: new Date().toISOString(),
        });
        return;
    }

    if (requestUrl.pathname !== '/fetch') {
        sendJson(response, 404, {
            error: 'Not found',
        });
        return;
    }

    if (request.method !== 'GET') {
        response.setHeader('Allow', 'GET');

        sendJson(response, 405, {
            error: 'Method not allowed',
        });
        return;
    }

    const rawTargetUrl =
        requestUrl.searchParams.get('url');

    if (!rawTargetUrl) {
        sendJson(response, 400, {
            error: 'Missing "url" query parameter',
        });
        return;
    }

    let targetUrl;

    try {
	    log.info(rawTargetUrl)
        targetUrl = validateTargetUrl(rawTargetUrl);
    } catch (error) {
        sendJson(response, 400, {
            error: error.message,
        });
        return;
    }

    const uniqueKey = randomUUID();

    const timer = setTimeout(() => {
        finishPendingRequest(
            uniqueKey,
            504,
            {
                'Content-Type':
                    'application/json; charset=utf-8',
            },
            JSON.stringify({
                success: false,
                url: targetUrl.href,
                error: 'Crawler request timed out',
                timestamp: new Date().toISOString(),
            }),
        );
    }, API_TIMEOUT_MS);

    pendingRequests.set(uniqueKey, {
        response,
        timer,
    });

    const dropIfClientDisconnects = () => {
        if (!response.writableEnded) {
            dropPendingRequest(uniqueKey);
        }
    };

    request.once('aborted', dropIfClientDisconnects);
    response.once('close', dropIfClientDisconnects);

    log.info('Adding URL to crawler queue', {
        url: targetUrl.href,
        requestId: uniqueKey,
    });

    try {
        await crawler.addRequests([
            {
                url: targetUrl.href,
                uniqueKey,
            },
        ]);
    } catch (error) {
        finishPendingRequest(
            uniqueKey,
            500,
            {
                'Content-Type':
                    'application/json; charset=utf-8',
            },
            JSON.stringify({
                success: false,
                url: targetUrl.href,
                error: error.message,
                timestamp: new Date().toISOString(),
            }),
        );
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
        throw new Error(
            'Only HTTP and HTTPS URLs are supported',
        );
    }

    if (targetUrl.username || targetUrl.password) {
        throw new Error(
            'URLs containing credentials are not supported',
        );
    }

    if (
        !ALLOW_ALL_TARGETS
        && !isHostnameAllowed(targetUrl.hostname)
    ) {
        throw new Error(
            `Target hostname is not allowed: ${targetUrl.hostname}`,
        );
    }

    return targetUrl;
}

function isHostnameAllowed(hostname) {
    return true; // 允许所有域名访问
    const normalizedHostname =
        hostname.toLowerCase();

    return allowedHostPatterns.some((pattern) => {
        if (!pattern.startsWith('*.')) {
            return normalizedHostname === pattern;
        }

        const baseDomain = pattern.slice(2);

        return (
            normalizedHostname === baseDomain
            || normalizedHostname.endsWith(
                `.${baseDomain}`,
            )
        );
    });
}

function finishPendingRequest(
    uniqueKey,
    statusCode,
    headers,
    body,
) {
    const pending = pendingRequests.get(uniqueKey);

    if (!pending) {
        return false;
    }

    pendingRequests.delete(uniqueKey);
    clearTimeout(pending.timer);

    const { response } = pending;

    if (
        response.writableEnded
        || response.destroyed
    ) {
        return false;
    }

    response.writeHead(
        normalizeStatusCode(statusCode, 500),
        {
            'Cache-Control': 'private, no-store',
            ...headers,
        },
    );

    response.end(body);

    return true;
}

function dropPendingRequest(uniqueKey) {
    const pending = pendingRequests.get(uniqueKey);

    if (!pending) {
        return;
    }

    clearTimeout(pending.timer);
    pendingRequests.delete(uniqueKey);
}

function sendJson(response, statusCode, value) {
    if (
        response.writableEnded
        || response.destroyed
    ) {
        return;
    }

    response.writeHead(statusCode, {
        'Content-Type':
            'application/json; charset=utf-8',
        'Cache-Control': 'private, no-store',
    });

    response.end(JSON.stringify(value));
}

function normalizeStatusCode(value, fallback) {
    const statusCode = Number(value);

    if (
        Number.isInteger(statusCode)
        && statusCode >= 100
        && statusCode <= 599
    ) {
        return statusCode;
    }

    return fallback;
}

function readPositiveInteger(name, fallback) {
    const value = Number.parseInt(
        process.env[name] || '',
        10,
    );

    return Number.isInteger(value) && value > 0
        ? value
        : fallback;
}

let shuttingDown = false;

async function shutdown(signal) {
    if (shuttingDown) {
        return;
    }

    shuttingDown = true;

    log.info(`Received ${signal}; shutting down`);

    server.close();

    for (const uniqueKey of pendingRequests.keys()) {
        finishPendingRequest(
            uniqueKey,
            503,
            {
                'Content-Type':
                    'application/json; charset=utf-8',
            },
            JSON.stringify({
                success: false,
                error: 'Service is shutting down',
            }),
        );
    }

    await crawler.teardown();
}

process.once('SIGINT', () => {
    void shutdown('SIGINT');
});

process.once('SIGTERM', () => {
    void shutdown('SIGTERM');
});

server.on('clientError', (error, socket) => {
    log.warning('HTTP client error', {
        error: error.message,
    });

    if (socket.writable) {
        socket.end(
            'HTTP/1.1 400 Bad Request\r\n'
            + 'Connection: close\r\n'
            + '\r\n',
        );
    }
});

server.listen(PORT, HOST, () => {
    log.info(`Fetch service listening on http://${HOST}:${PORT}`);
    log.info('Target access policy', {
        allowAllTargets: ALLOW_ALL_TARGETS,
        allowedHosts: ALLOW_ALL_TARGETS
            ? ['*']
            : allowedHostPatterns,
        proxyEnabled: Boolean(proxyConfiguration),
    });
});

try {
    await crawler.run();
} catch (error) {
    log.error('Crawler stopped unexpectedly', { error });

    if (!shuttingDown) {
        await shutdown('crawler failure');
    }

    process.exitCode = 1;
}
