import { log } from 'crawlee';
import { loadConfig } from './src/config.js';
import { createFetchService } from './src/fetch-service.js';

const config = loadConfig();
const service = createFetchService(config);

let handlingSignal = false;

function handleSignal(signal) {
    if (handlingSignal) {
        return;
    }

    handlingSignal = true;

    const forceExitTimer = setTimeout(() => {
        log.error('Service exceeded its shutdown deadline');
        process.exit(1);
    }, config.shutdownTimeoutMs + 1_000);

    void service.shutdown(signal).then(
        () => {
            clearTimeout(forceExitTimer);
            process.exit(0);
        },
        (error) => {
            clearTimeout(forceExitTimer);
            log.error('Service shutdown failed', { error });
            process.exit(1);
        },
    );
}

process.once('SIGINT', () => handleSignal('SIGINT'));
process.once('SIGTERM', () => handleSignal('SIGTERM'));

try {
    await service.start();
    await service.waitUntilStopped();
} catch (error) {
    process.exitCode = 1;
    log.error(error.message, { error });

    await service.shutdown(
        'runtime failure',
        { drain: false },
    );
}
