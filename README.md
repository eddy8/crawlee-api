# Crawlee Fetch API

一个基于 Crawlee 和 Puppeteer 的 HTTP 抓取服务，可等待目标站的 JavaScript Gatekeeper 验证完成后返回最终页面。

## 运行

```shell
npm install
npm start
```

抓取页面：

```text
GET /fetch?url=https%3A%2F%2Fexample.com%2F
```

成功响应包含 `X-Request-ID`、`X-Upstream-Status` 和 `X-Final-URL` 响应头。超时、过载和上游响应过大时会返回结构化 JSON 错误。

## 运行状态

- `GET /health/live`：进程存活状态。
- `GET /health/ready`：Crawler 可用且未达容量上限时返回 200。
- `GET /health`：与 `/health/ready` 相同，用于向后兼容。
- `GET /metrics`：返回请求结果、队列深度、Crawler 槽位、延迟分位和响应字节数。

## 配置

可通过环境变量调整：

- `HOST` 和 `PORT`
- `API_TIMEOUT_MS` 和 `GATEKEEPER_TIMEOUT_MS`
- `MAX_CONCURRENCY` 和 `MAX_PENDING_REQUESTS`
- `MAX_RESPONSE_BYTES`
- `SHUTDOWN_TIMEOUT_MS`
- `RETRY_AFTER_SECONDS`
- `HEADLESS`
- `PROXY_URLS`（英文逗号分隔）

有效默认值以 [`src/config.js`](src/config.js) 为准。

## 测试

```shell
npm test
```

测试会启动本地上游服务、实际 HTTP 服务和 Chromium，覆盖正常抓取、响应大小限制、日志脱敏、超时取消、过载保护、优雅关停和端口占用。

## 部署边界

服务允许抓取任意 HTTP/HTTPS URL。如果接口可被不可信用户调用，应在网络或容器层阻断云元数据地址和内部管理网络。
