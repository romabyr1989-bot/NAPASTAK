import type { NapastakConfig, IngestResult, QueryResult, TableInfo } from './types';

// ---------------------------------------------------------------------------
// Error class
// ---------------------------------------------------------------------------

export class NapastakError extends Error {
  public readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'NapastakError';
    this.status = status;
    // Restore prototype chain (needed when targeting ES5)
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildQueryResult(data: any, durationMs: number): QueryResult {
  const columns: string[] = data.columns ?? [];
  const rows: any[][] = data.rows ?? [];
  return {
    columns,
    rows,
    rowCount: rows.length,
    durationMs,
  };
}

// ---------------------------------------------------------------------------
// NapastakClient
// ---------------------------------------------------------------------------

export class NapastakClient {
  private readonly baseUrl: string;
  private readonly timeout: number;
  private token: string | undefined;
  private txnId: string | undefined;

  constructor(config: NapastakConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, '');
    this.timeout = config.timeout ?? 30_000;
    this.token = config.apiKey ?? config.token;
  }

  // ------------------------------------------------------------------
  // Auth
  // ------------------------------------------------------------------

  async login(username: string, password: string): Promise<void> {
    const resp = await this._fetch('/api/auth/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    });
    const data = await resp.json();
    const t = data.token ?? data.access_token ?? data.jwt;
    if (!t) {
      throw new NapastakError(200, 'No token returned by server');
    }
    this.token = t as string;
  }

  // ------------------------------------------------------------------
  // Query
  // ------------------------------------------------------------------

  async query(sql: string): Promise<QueryResult> {
    const start = Date.now();
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (this.txnId !== undefined) {
      headers['X-Txn-Id'] = this.txnId;
    }
    const resp = await this._fetch('/api/tables/query', {
      method: 'POST',
      headers,
      body: JSON.stringify({ sql }),
    });
    const data = await resp.json();
    return buildQueryResult(data, Date.now() - start);
  }

  // ------------------------------------------------------------------
  // Ingest
  // ------------------------------------------------------------------

  async ingest(table: string, csv: string): Promise<IngestResult> {
    const resp = await this._fetch(`/api/tables/${encodeURIComponent(table)}/ingest`, {
      method: 'POST',
      headers: { 'Content-Type': 'text/csv' },
      body: csv,
    });
    return (await resp.json()) as IngestResult;
  }

  // ------------------------------------------------------------------
  // Tables
  // ------------------------------------------------------------------

  async tables(): Promise<TableInfo[]> {
    const resp = await this._fetch('/api/tables', { method: 'GET' });
    const data = await resp.json();
    if (Array.isArray(data)) return data as TableInfo[];
    return (data.tables ?? []) as TableInfo[];
  }

  // ------------------------------------------------------------------
  // Health
  // ------------------------------------------------------------------

  async health(): Promise<any> {
    const resp = await this._fetch('/health', { method: 'GET' });
    return resp.json();
  }

  // ------------------------------------------------------------------
  // Transactions
  // ------------------------------------------------------------------

  async begin(): Promise<void> {
    const resp = await this._fetch('/api/tables/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'BEGIN' }),
    });
    const data = await resp.json();
    const id = data.txn_id ?? data.transaction_id ?? data.id;
    this.txnId = id !== undefined ? String(id) : undefined;
  }

  async commit(): Promise<void> {
    await this._sendControl('COMMIT');
    this.txnId = undefined;
  }

  async rollback(): Promise<void> {
    await this._sendControl('ROLLBACK');
    this.txnId = undefined;
  }

  private async _sendControl(sql: 'COMMIT' | 'ROLLBACK'): Promise<void> {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    if (this.txnId !== undefined) {
      headers['X-Txn-Id'] = this.txnId;
    }
    await this._fetch('/api/tables/query', {
      method: 'POST',
      headers,
      body: JSON.stringify({ sql }),
    });
  }

  /**
   * Execute *fn* inside a server-side transaction.
   * Commits on success, rolls back on any thrown error.
   */
  async withTransaction<T>(fn: (client: NapastakClient) => Promise<T>): Promise<T> {
    await this.begin();
    try {
      const result = await fn(this);
      await this.commit();
      return result;
    } catch (err) {
      try {
        await this.rollback();
      } catch {
        // Swallow rollback errors to let the original error propagate
      }
      throw err;
    }
  }

  // ------------------------------------------------------------------
  // Low-level fetch with auth + timeout + error mapping
  // ------------------------------------------------------------------

  private async _fetch(path: string, init: RequestInit): Promise<Response> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      ...(init.headers as Record<string, string> | undefined),
    };
    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);

    let resp: Response;
    try {
      resp = await fetch(url, { ...init, headers, signal: controller.signal });
    } finally {
      clearTimeout(timer);
    }

    if (!resp.ok) {
      let message = resp.statusText;
      try {
        const body = await resp.clone().json();
        message = body.error ?? body.message ?? message;
      } catch {
        // ignore JSON parse failures
      }
      throw new NapastakError(resp.status, message);
    }

    return resp;
  }
}
