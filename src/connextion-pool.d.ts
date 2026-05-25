import { EventEmitter } from 'events';
import { PoolConfig, PooledConnection, ConnectionFactory, PoolStats } from './types';
export declare class ConnectionPool<T> extends EventEmitter {
    private config;
    private factory;
    private connections;
    private waitingQueue;
    private mutex;
    private cleanupInterval;
    private isDestroyed;
    constructor(config: PoolConfig, factory: ConnectionFactory<T>);
    initialize(): Promise<void>;
    acquire(): Promise<PooledConnection<T>>;
    release(connId: string): Promise<void>;
    getStats(): PoolStats;
    destroy(): Promise<void>;
    private createConnection;
    private findIdleConnection;
    private waitForConnection;
    private cleanup;
}
//# sourceMappingURL=connextion-pool.d.ts.map