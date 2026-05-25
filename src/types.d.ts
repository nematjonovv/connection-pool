export type ConnectionState = 'idle' | 'active' | 'destroyed';
export interface PoolConfig {
    minConnections: number;
    maxConnections: number;
    acquireTimeout: number;
    idleTimeout: number;
    connectionTimeout: number;
}
export interface PooledConnection<T> {
    id: string;
    raw: T;
    state: ConnectionState;
    createdAt: Date;
    lastUsedAt: Date;
    useCount: number;
}
export interface ConnectionFactory<T> {
    create: () => Promise<T>;
    destroy: (conn: T) => Promise<void>;
    validate: (conn: T) => Promise<boolean>;
}
export interface PoolStats {
    total: number;
    active: number;
    idle: number;
    waiting: number;
}
//# sourceMappingURL=types.d.ts.map