export interface NapastakConfig {
  baseUrl: string;
  apiKey?: string;
  token?: string;
  timeout?: number;
}

export interface QueryResult {
  columns: string[];
  rows: any[][];
  rowCount: number;
  durationMs: number;
}

export interface TableInfo {
  name: string;
  row_count?: number;
  columns?: number;
}

export interface IngestResult {
  inserted?: number;
  rows?: number;
  table?: string;
}
