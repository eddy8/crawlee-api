import { randomUUID } from 'node:crypto';
import { RequestQueue as CrawleeRequestQueue } from 'crawlee';

class PendingRequestCache extends Map {
    add(key, value) {
        this.set(key, value);
    }
}

// Crawlee's storage manager uses the class name to choose its storage API.
// Retain RequestQueue's name and its supported open()/retry/locking behavior.
class RequestQueue extends CrawleeRequestQueue {
    constructor(...arguments_) {
        super(...arguments_);
        // Crawlee's default dedup cache reserves one million slots. This API
        // only needs deduplication while an individual UUID job is in flight.
        this.requestSeenCache = new PendingRequestCache();
    }

    async markRequestHandled(request) {
        const result = await super.markRequestHandled(request);
        await this.client.deleteRequest(request.id);
        // The memory-storage request ID is also the unique-key cache hash.
        // API jobs have UUID unique keys, so historical deduplication is unneeded.
        this.requestCache.remove(request.id);
        this.requestSeenCache.delete(request.id);
        this.recentlyHandledRequestsCache.remove(request.id);
        return result;
    }
}

export async function openTransientQueue(config) {
    // Separate service instances must not purge or consume each other's jobs.
    return RequestQueue.open(`fetch-${randomUUID()}`, { config });
}
