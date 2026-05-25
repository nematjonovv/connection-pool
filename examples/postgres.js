"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const pg_1 = require("pg");
const src_1 = require("../src");
const config = {
    minConnections: 2,
    maxConnections: 10,
    acquireTimeout: 5000,
    idleTimeout: 60000,
    connectionTimeout: 3000,
};
const factory = {
    create: async () => {
        const client = new pg_1.Client({
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
        }
        catch {
            return false;
        }
    },
};
async function main() {
    const pool = new src_1.ConnectionPool(config, factory);
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
    }
    finally {
        await pool.release(conn.id);
    }
    console.log('Stats after release:', pool.getStats());
    await pool.destroy();
    console.log('Pool destroyed');
}
main().catch(console.error);
//# sourceMappingURL=postgres.js.map