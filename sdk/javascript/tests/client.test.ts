import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { NapastakClient, NapastakError } from '../src/client';

const BASE = 'http://localhost:8080';

// ---------------------------------------------------------------------------
// MSW server setup
// ---------------------------------------------------------------------------

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

function makeClient(): NapastakClient {
  return new NapastakClient({ baseUrl: BASE, apiKey: 'test-key' });
}

// ---------------------------------------------------------------------------
// query returns QueryResult with correct data
// ---------------------------------------------------------------------------

describe('query', () => {
  it('returns QueryResult with correct data', async () => {
    server.use(
      http.post(`${BASE}/api/tables/query`, () =>
        HttpResponse.json({
          columns: ['id', 'name'],
          rows: [
            [1, 'alice'],
            [2, 'bob'],
          ],
        }),
      ),
    );

    const client = makeClient();
    const result = await client.query('SELECT * FROM users');

    expect(result.columns).toEqual(['id', 'name']);
    expect(result.rows).toHaveLength(2);
    expect(result.rowCount).toBe(2);
    expect(result.rows[0]).toEqual([1, 'alice']);
    expect(result.rows[1]).toEqual([2, 'bob']);
    expect(typeof result.durationMs).toBe('number');
  });
});

// ---------------------------------------------------------------------------
// ingest sends CSV correctly
// ---------------------------------------------------------------------------

describe('ingest', () => {
  it('sends CSV body and returns IngestResult', async () => {
    let receivedBody = '';
    let receivedContentType = '';

    server.use(
      http.post(`${BASE}/api/tables/users/ingest`, async ({ request }) => {
        receivedBody = await request.text();
        receivedContentType = request.headers.get('content-type') ?? '';
        return HttpResponse.json({ inserted: 3 });
      }),
    );

    const client = makeClient();
    const csv = 'id,name\n1,alice\n2,bob\n3,carol\n';
    const result = await client.ingest('users', csv);

    expect(result.inserted).toBe(3);
    expect(receivedContentType).toContain('text/csv');
    expect(receivedBody).toContain('alice');
    expect(receivedBody).toContain('bob');
  });
});

// ---------------------------------------------------------------------------
// withTransaction — commit on success
// ---------------------------------------------------------------------------

describe('withTransaction', () => {
  it('commits when the callback succeeds', async () => {
    const sqlLog: string[] = [];

    server.use(
      http.post(`${BASE}/api/tables/query`, async ({ request }) => {
        const body = (await request.json()) as { sql: string };
        sqlLog.push(body.sql);
        if (body.sql === 'BEGIN') {
          return HttpResponse.json({ txn_id: 42 });
        }
        if (body.sql === 'SELECT 1') {
          return HttpResponse.json({ columns: ['1'], rows: [[1]] });
        }
        return HttpResponse.json({ ok: true });
      }),
    );

    const client = makeClient();
    const result = await client.withTransaction(async (c) => {
      await c.query('SELECT 1');
      return 'done';
    });

    expect(result).toBe('done');
    expect(sqlLog).toEqual(['BEGIN', 'SELECT 1', 'COMMIT']);
  });

  it('rolls back when the callback throws', async () => {
    const sqlLog: string[] = [];

    server.use(
      http.post(`${BASE}/api/tables/query`, async ({ request }) => {
        const body = (await request.json()) as { sql: string };
        sqlLog.push(body.sql);
        return HttpResponse.json({ txn_id: 7 });
      }),
    );

    const client = makeClient();
    await expect(
      client.withTransaction(async () => {
        throw new Error('something went wrong');
      }),
    ).rejects.toThrow('something went wrong');

    expect(sqlLog).toEqual(['BEGIN', 'ROLLBACK']);
  });
});

// ---------------------------------------------------------------------------
// health returns status ok
// ---------------------------------------------------------------------------

describe('health', () => {
  it('returns status ok', async () => {
    server.use(
      http.get(`${BASE}/health`, () => HttpResponse.json({ status: 'ok' })),
    );

    const client = makeClient();
    const result = await client.health();

    expect(result).toEqual({ status: 'ok' });
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe('error handling', () => {
  it('throws NapastakError on 401', async () => {
    server.use(
      http.post(`${BASE}/api/tables/query`, () =>
        HttpResponse.json({ error: 'Unauthorized' }, { status: 401 }),
      ),
    );

    const client = makeClient();
    await expect(client.query('SELECT 1')).rejects.toBeInstanceOf(NapastakError);

    try {
      await client.query('SELECT 1');
    } catch (e) {
      if (e instanceof NapastakError) {
        expect(e.status).toBe(401);
      }
    }
  });

  it('throws NapastakError on 403', async () => {
    server.use(
      http.post(`${BASE}/api/tables/query`, () =>
        HttpResponse.json({ error: 'Forbidden' }, { status: 403 }),
      ),
    );

    const client = makeClient();
    let caught: NapastakError | null = null;
    try {
      await client.query('SELECT * FROM secret');
    } catch (e) {
      if (e instanceof NapastakError) caught = e;
    }
    expect(caught).not.toBeNull();
    expect(caught!.status).toBe(403);
  });
});
