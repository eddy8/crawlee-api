# Crawlee Fetch API

一个基于 Crawlee 和 Puppeteer 的 HTTP 抓取服务，可等待目标站的 JavaScript Gatekeeper 验证完成后返回最终页面。

## 运行

要求 Node.js 22.12 或更高版本。部署时使用锁文件安装依赖：

```shell
npm ci
npm start
```

抓取页面：

```text
GET /fetch?url=https%3A%2F%2Fexample.com%2F
```

成功响应包含 `X-Request-ID`、`X-Upstream-Status` 和 `X-Final-URL` 响应头。超时、过载和上游响应过大时会返回结构化 JSON 错误。

## 运行状态

- `GET /health/live`：进程存活状态；Crawler 失败或有任务卡死超过 `JOB_STALL_GRACE_MS` 时返回 503。
- `GET /health/ready`：Crawler 可用且未达容量上限时返回 200。
- `GET /health`：与 `/health/ready` 相同，用于向后兼容。
- `GET /metrics`：返回请求结果、队列深度、Crawler 槽位、延迟分位和响应字节数。

## 配置

可通过环境变量调整：

- `HOST` 和 `PORT`
- `API_TIMEOUT_MS` 和 `GATEKEEPER_TIMEOUT_MS`
- `MAX_CONCURRENCY` 和 `MAX_PENDING_REQUESTS`
- `MAX_RESPONSE_BYTES`
- `SHUTDOWN_TIMEOUT_MS`：关停时等待已接收请求完成的期限
- `SHUTDOWN_DELAY_MS`：收到 `SIGTERM` 后先让 readiness 返回 503、继续正常服务这么久再停止接收，默认 0。Kubernetes 下建议 5000，给端点摘除留出传播时间
- `KEEP_ALIVE_TIMEOUT_MS`：HTTP keep-alive 空闲超时，默认 65000，必须大于前置负载均衡/反向代理的空闲超时（ALB、nginx 默认 60 秒）
- `JOB_STALL_GRACE_MS`：任务超过 API 期限后仍未被 Crawler 回收的容忍时长，默认 `API_TIMEOUT_MS + 60000`；超过后 `/health/live` 返回 503，便于编排系统重启卡死实例
- `RETRY_AFTER_SECONDS`
- `HEADLESS`
- `BROWSER_IDLE_TIMEOUT_SECS`：空闲浏览器保留时长，默认 3600 秒，避免短暂空闲后下一个请求冷启动 Chromium
- `CRAWLEE_MEMORY_MBYTES` 或 `CRAWLEE_AVAILABLE_MEMORY_RATIO`：Crawlee 判定内存过载并自动降并发的阈值（含 Chromium 子进程，按容器 cgroup 限额计算）。默认按可用内存的 0.8 计算；容器部署时建议显式设置为内存限额的 70%–80%
- `PROXY_URLS`（英文逗号分隔）

有效默认值以 [`src/config.js`](src/config.js) 为准。

数值配置必须为正整数，`PORT` 不得超过 65535，`HEADLESS` 接受 `true` 或 `false`；无效配置会阻止启动并报告配置名。`MAX_PENDING_REQUESTS` 包含正在执行和排队的请求。收到 `SIGTERM` 时（经过 `SHUTDOWN_DELAY_MS` 后）停止接收新任务，并在关停期限内继续处理已接收的队列；关停期间的响应带 `Connection: close`。进程最长在 `SHUTDOWN_DELAY_MS + SHUTDOWN_TIMEOUT_MS + 10 秒` 后强制退出，Kubernetes 的 `terminationGracePeriodSeconds` 应不小于该值。

`MAX_RESPONSE_BYTES` 限制主文档接收的字节数及最终返回内容，分块和压缩响应也会检查，超过限制即中止抓取。它不是浏览器进程的总内存限制，图片、脚本等子资源和页面 JavaScript 仍会消耗资源。

## 测试

```shell
npm test
```

测试会启动本地上游服务、实际 HTTP 服务和 Chromium，覆盖正常抓取、验证跳转、响应大小限制、日志脱敏、超时与客户端断开、过载保护、队列回收、并发请求、优雅关停和端口占用。并发测试输出本机延迟数据，不代表真实目标站的吞吐量。

## 部署边界

服务允许抓取任意 HTTP/HTTPS URL。如果接口可被不可信用户调用，应在网络或容器层阻断云元数据地址和内部管理网络。
