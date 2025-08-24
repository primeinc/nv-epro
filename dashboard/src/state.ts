import * as duckdb from '@duckdb/duckdb-wasm';

export interface AppState {
  db: duckdb.AsyncDuckDB;
  conn: duckdb.AsyncDuckDBConnection;
  currentView: 'dashboard' | 'vendor-detail' | 'loading' | 'error';
  selectedVendor?: string;
}

export let appState: AppState;

export function setAppState(state: AppState) {
  appState = state;
}

export function updateView(view: AppState['currentView'], selectedVendor?: string) {
  appState.currentView = view;
  if (selectedVendor !== undefined) {
    appState.selectedVendor = selectedVendor;
  }
}