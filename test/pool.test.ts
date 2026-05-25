import { ConnectionPool } from '../src';
import type { ConnectionFactory, PoolConfig } from '../src';

// Fake connection
interface FakeConnection {
  id: number;
  isAlive: boolean;
}

let connCounter = 0;

const factory: ConnectionFactory<FakeConnection> = {
  create: async () => {
    connCounter++;
    console.log(`[Factory] Connection created: #${connCounter}`);
    return { id: connCounter, isAlive: true };
  },

  destroy: async (conn) => {
    conn.isAlive = false;
    console.log(`[Factory] Connection destroyed: #${conn.id}`);
  },

  validate: async (conn) => {
    return conn.isAlive;
  },
};

const config: PoolConfig = {
  minConnections: 2,
  maxConnections: 5,
  acquireTimeout: 3000,
  idleTimeout: 10000,
  connectionTimeout: 3000,
};

async function test1_initialize() {
  console.log('\n--- Test 1: Initialize ---');
  const pool = new ConnectionPool<FakeConnection>(config, factory);
  await pool.initialize();
  const stats = pool.getStats();
  console.log('Stats:', stats);
  console.assert(stats.total === 2, 'Should have 2 connections');
  console.assert(stats.idle === 2, 'Should have 2 idle connections');
  await pool.destroy();
  console.log('PASSED ✅');
}

async function test2_acquire_release() {
  console.log('\n--- Test 2: Acquire & Release ---');
  const pool = new ConnectionPool<FakeConnection>(config, factory);
  await pool.initialize();

  const conn = await pool.acquire();
  console.log('Acquired connection:', conn.id, '| useCount:', conn.useCount);
  console.assert(conn.state === 'active', 'State should be active');

  let stats = pool.getStats();
  console.assert(stats.active === 1, 'Should have 1 active');

  await pool.release(conn.id);
  stats = pool.getStats();
  console.assert(stats.idle === 2, 'Should have 2 idle after release');

  await pool.destroy();
  console.log('PASSED ✅');
}

async function test3_max_connections() {
  console.log('\n--- Test 3: Max Connections ---');
  const pool = new ConnectionPool<FakeConnection>(config, factory);
  await pool.initialize();

  const conns = [];
  for (let i = 0; i < 5; i++) {
    conns.push(await pool.acquire());
  }

  const stats = pool.getStats();
  console.log('Stats:', stats);
  console.assert(stats.total === 5, 'Should have 5 total');
  console.assert(stats.active === 5, 'Should have 5 active');

  for (const conn of conns) {
    await pool.release(conn.id);
  }

  await pool.destroy();
  console.log('PASSED ✅');
}

async function test4_acquire_timeout() {
  console.log('\n--- Test 4: Acquire Timeout ---');
  const pool = new ConnectionPool<FakeConnection>(config, factory);
  await pool.initialize();

  // Barcha ulanishlarni band qilamiz
  const conns = [];
  for (let i = 0; i < 5; i++) {
    conns.push(await pool.acquire());
  }

  try {
    // 6-chi ulanish — timeout bo'lishi kerak
    await pool.acquire();
    console.log('FAILED ❌ Should have thrown timeout error');
  } catch (err: any) {
    console.log('Timeout error caught:', err.message);
    console.assert(err.message.includes('Acquire timeout'), 'Should be timeout error');
    console.log('PASSED ✅');
  }

  for (const conn of conns) {
    await pool.release(conn.id);
  }

  await pool.destroy();
}

async function test5_reuse() {
  console.log('\n--- Test 5: Automatic Reuse ---');
  const pool = new ConnectionPool<FakeConnection>(config, factory);
  await pool.initialize();

  const conn1 = await pool.acquire();
  const id1 = conn1.id;
  await pool.release(conn1.id);

  const conn2 = await pool.acquire();
  console.log('Same connection reused:', conn2.id === id1);
  console.assert(conn2.id === id1, 'Should reuse same connection');
  console.assert(conn2.useCount === 2, 'useCount should be 2');

  await pool.release(conn2.id);
  await pool.destroy();
  console.log('PASSED ✅');
}

async function runAll() {
  console.log('========= CONNECTION POOL TESTS =========');
  try {
    await test1_initialize();
    await test2_acquire_release();
    await test3_max_connections();
    await test4_acquire_timeout();
    await test5_reuse();
    console.log('\n========= ALL TESTS PASSED ✅ =========');
  } catch (err) {
    console.error('\n========= TEST FAILED ❌ =========', err);
  }
}

runAll();