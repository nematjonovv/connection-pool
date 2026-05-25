import { Client } from 'pg';
import { ConnectionPool } from '../src';
import type { PoolConfig, ConnectionFactory } from '../src';

const config: PoolConfig = {
  minConnections: 2,
  maxConnections: 10,
  acquireTimeout: 5000,
  idleTimeout: 60000,
  connectionTimeout: 3000,
};

const factory: ConnectionFactory<Client> = {
  create: async () => {
    const client = new Client({
      host: 'localhost',
      port: 5432,
      database: 'testdb',
      user: 'postgres',
      password: 'postgres',
    });
    await client.connect();
    return client;
  },

  destroy: async (client) => {
    await client.end();
  },

  validate: async (client) => {
    try {
      await client.query('SELECT 1');
      return true;
    } catch {
      return false;
    }
  },
};

async function main() {
  const pool = new ConnectionPool<Client>(config, factory);

  pool.on('initialized', (stats) => console.log('Pool initialized:', stats));
  pool.on('acquire', (id) => console.log('Connection acquired:', id));
  pool.on('release', (id) => console.log('Connection released:', id));
  pool.on('connectionCreated', (id) => console.log('New connection:', id));
  pool.on('connectionDestroyed', (id) => console.log('Connection removed:', id));
  pool.on('waiting', (count) => console.log('Waiting in queue:', count));

  await pool.initialize();
  console.log('Stats:', pool.getStats());

  const conn = await pool.acquire();

  try {
    const result = await conn.raw.query('SELECT NOW() as time');
    console.log('Query result:', result.rows[0]);
  } finally {
    await pool.release(conn.id);
  }

  console.log('Stats after release:', pool.getStats());
  await pool.destroy();
  console.log('Pool destroyed');
}

main().catch(console.error);