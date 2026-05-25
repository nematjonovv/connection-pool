import { EventEmitter } from 'events';
import { randomUUID } from 'crypto';
import { PoolConfig, PooledConnection, ConnectionFactory, PoolStats } from './types';
import { Mutex } from './mutex';

export class ConnectionPool<T> extends EventEmitter {
  private connections: Map<string, PooledConnection<T>> = new Map();
  private waitingQueue: Array<{
    resolve: (conn: PooledConnection<T>) => void;
    reject: (err: Error) => void;
    timer: NodeJS.Timeout;
  }> = [];
  private mutex = new Mutex();
  private cleanupInterval: NodeJS.Timeout | null = null;
  private isDestroyed = false;

  constructor(
    private config: PoolConfig,
    private factory: ConnectionFactory<T>
  ) {
    super();
  }

  async initialize(): Promise<void> {
    const promises = Array.from(
      { length: this.config.minConnections },
      () => this.createConnection()
    );
    await Promise.all(promises);

    this.cleanupInterval = setInterval(
      () => this.cleanup(),
      30_000
    );

    this.emit('initialized', this.getStats());
  }

  async acquire(): Promise<PooledConnection<T>> {
    if (this.isDestroyed) {
      throw new Error('Pool has been destroyed');
    }

    const release = await this.mutex.acquire();

    try {
      const idleConn = this.findIdleConnection();
      if (idleConn) {
        idleConn.state = 'active';
        idleConn.lastUsedAt = new Date();
        idleConn.useCount++;
        this.emit('acquire', idleConn.id);
        return idleConn;
      }

      if (this.connections.size < this.config.maxConnections) {
        const newConn = await this.createConnection();
        newConn.state = 'active';
        newConn.useCount++;
        this.emit('acquire', newConn.id);
        return newConn;
      }

      return await this.waitForConnection();
    } finally {
      release();
    }
  }

  async release(connId: string): Promise<void> {
    const release = await this.mutex.acquire();

    try {
      const conn = this.connections.get(connId);
      if (!conn || conn.state === 'destroyed') return;

      if (this.waitingQueue.length > 0) {
        const waiter = this.waitingQueue.shift()!;
        clearTimeout(waiter.timer);
        conn.state = 'active';
        conn.lastUsedAt = new Date();
        conn.useCount++;
        waiter.resolve(conn);
        return;
      }

      conn.state = 'idle';
      conn.lastUsedAt = new Date();
      this.emit('release', connId);
    } finally {
      release();
    }
  }

  getStats(): PoolStats {
    const conns = Array.from(this.connections.values());
    return {
      total: conns.length,
      active: conns.filter(c => c.state === 'active').length,
      idle: conns.filter(c => c.state === 'idle').length,
      waiting: this.waitingQueue.length,
    };
  }

  async destroy(): Promise<void> {
    this.isDestroyed = true;
    if (this.cleanupInterval) clearInterval(this.cleanupInterval);

    for (const waiter of this.waitingQueue) {
      clearTimeout(waiter.timer);
      waiter.reject(new Error('Pool destroyed'));
    }
    this.waitingQueue = [];

    for (const conn of this.connections.values()) {
      await this.factory.destroy(conn.raw).catch(() => {});
    }
    this.connections.clear();
    this.emit('destroyed');
  }

  private async createConnection(): Promise<PooledConnection<T>> {
    const raw = await this.factory.create();
    const conn: PooledConnection<T> = {
      id: randomUUID(),
      raw,
      state: 'idle',
      createdAt: new Date(),
      lastUsedAt: new Date(),
      useCount: 0,
    };
    this.connections.set(conn.id, conn);
    this.emit('connectionCreated', conn.id);
    return conn;
  }

  private findIdleConnection(): PooledConnection<T> | null {
    for (const conn of this.connections.values()) {
      if (conn.state === 'idle') return conn;
    }
    return null;
  }

  private waitForConnection(): Promise<PooledConnection<T>> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const idx = this.waitingQueue.findIndex(w => w.timer === timer);
        if (idx !== -1) this.waitingQueue.splice(idx, 1);
        reject(new Error(
          `Acquire timeout after ${this.config.acquireTimeout}ms. Stats: ${JSON.stringify(this.getStats())}`
        ));
      }, this.config.acquireTimeout);

      this.waitingQueue.push({ resolve, reject, timer });
      this.emit('waiting', this.waitingQueue.length);
    });
  }

  private async cleanup(): Promise<void> {
    const now = Date.now();
    const release = await this.mutex.acquire();

    try {
      const toDestroy: string[] = [];

      for (const [id, conn] of this.connections) {
        const isIdle = conn.state === 'idle';
        const isExpired = now - conn.lastUsedAt.getTime() > this.config.idleTimeout;
        const aboveMin = this.connections.size - toDestroy.length > this.config.minConnections;

        if (isIdle && isExpired && aboveMin) {
          toDestroy.push(id);
        }
      }

      for (const id of toDestroy) {
        const conn = this.connections.get(id)!;
        conn.state = 'destroyed';
        this.connections.delete(id);
        await this.factory.destroy(conn.raw).catch(() => {});
        this.emit('connectionDestroyed', id);
      }
    } finally {
      release();
    }
  }
}