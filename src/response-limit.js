// Enforce the document limit while Chromium is receiving it, including chunked
// and compressed bodies. A post-download length check alone cannot bound memory.
export async function monitorDocumentSize(page, maxBytes, onExceeded) {
    /*
     * Prefer the page's own CDP session: Puppeteer already has the Network
     * domain enabled there, so listening costs nothing extra. A dedicated
     * session would need its own Network.enable and makes Chromium stream
     * every Network event (all subresource chunks) to Node a second time.
     * `_client()` is internal, so fall back to a dedicated session if absent.
     */
    const pageSession = typeof page._client === 'function' ? page._client() : null;
    const client = pageSession ?? await page.createCDPSession();
    const ownsSession = !pageSession;
    let requestId;
    let receivedBytes = 0;
    let exceeded = false;
    let stopped = false;
    let frameId;

    const check = (bytes) => {
        if (!stopped && !exceeded && bytes > maxBytes) {
            exceeded = true;
            onExceeded();
        }
    };
    const onRequest = (event) => {
        if (event.type === 'Document' && event.frameId === frameId) {
            requestId = event.requestId;
            receivedBytes = 0;
        }
    };
    const onResponse = (event) => {
        if (event.requestId !== requestId) return;
        for (const [name, value] of Object.entries(event.response.headers)) {
            if (name.toLowerCase() === 'content-length') check(Number(value));
        }
    };
    const onData = (event) => {
        if (event.requestId !== requestId) return;
        // dataLength is decoded bytes; encodedDataLength would miss gzip bombs.
        receivedBytes += event.dataLength;
        check(receivedBytes);
    };
    const stop = () => {
        if (stopped) return;
        stopped = true;
        client.off('Network.requestWillBeSent', onRequest);
        client.off('Network.responseReceived', onResponse);
        client.off('Network.dataReceived', onData);
        page.off('close', stop);
        if (ownsSession) {
            void client.detach().catch(() => {});
        }
    };
    try {
        frameId = page.mainFrame()?._id;
        if (!frameId) {
            const { frameTree } = await client.send('Page.getFrameTree');
            frameId = frameTree.frame.id;
        }
        client.on('Network.requestWillBeSent', onRequest);
        client.on('Network.responseReceived', onResponse);
        client.on('Network.dataReceived', onData);
        page.once('close', stop);
        if (ownsSession) {
            await client.send('Network.enable');
        }
        return stop;
    } catch (error) {
        stop();
        throw error;
    }
}
