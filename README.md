# Connection Pool

Потокобезопасная библиотека для управления пулом соединений с автоматическим переиспользованием.

## Возможности

- Generic `<T>` — работает с любым типом соединения (PostgreSQL, Redis, HTTP)
- Потокобезопасность через Mutex — предотвращает Race Condition
- Автоматическое переиспользование — соединения возвращаются и используются повторно
- Автоочистка — неактивные соединения удаляются автоматически
- Event-driven — мониторинг активности пула через события
- Гибкая настройка — min/max соединения, таймауты

## Установка

```bash
npm install
npm run build
```

## Использование

```typescript
import { ConnectionPool } from './src';
import type { PoolConfig, ConnectionFactory } from './src';

const config: PoolConfig = {
  minConnections: 2,
  maxConnections: 10,
  acquireTimeout: 5000,
  idleTimeout: 60000,
  connectionTimeout: 3000,
};

const factory: ConnectionFactory<YourConnection> = {
  create: async () => { /* создать соединение */ },
  destroy: async (conn) => { /* закрыть соединение */ },
  validate: async (conn) => { /* проверить активность */ },
};

const pool = new ConnectionPool(config, factory);
await pool.initialize();

const conn = await pool.acquire();
try {
  // использовать conn.raw
} finally {
  await pool.release(conn.id);
}

await pool.destroy();
```

## Конфигурация

| Параметр | Описание | Пример |
|---|---|---|
| `minConnections` | Минимум готовых соединений | 2 |
| `maxConnections` | Максимум одновременных соединений | 10 |
| `acquireTimeout` | Время ожидания в мс | 5000 |
| `idleTimeout` | Время жизни неактивного соединения в мс | 60000 |
| `connectionTimeout` | Время открытия нового соединения в мс | 3000 |

## События

| Событие | Данные | Описание |
|---|---|---|
| `initialized` | `PoolStats` | Пул готов к работе |
| `acquire` | `connId` | Соединение выдано |
| `release` | `connId` | Соединение возвращено |
| `connectionCreated` | `connId` | Новое соединение создано |
| `connectionDestroyed` | `connId` | Соединение удалено |
| `waiting` | `count` | Запрос ожидает в очереди |
| `destroyed` | — | Пул уничтожен |

## Статистика

```typescript
pool.getStats();
// { total: 5, active: 3, idle: 2, waiting: 0 }
```

## Пример

```bash
npm run example
```
