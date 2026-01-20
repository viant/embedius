-- Embedius downstream SQLite schema (sqlite-vec compatible)

CREATE TABLE IF NOT EXISTS vec_dataset (
  dataset_id   TEXT PRIMARY KEY,
  description  TEXT,
  source_uri   TEXT,
  last_scn     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS vec_dataset_scn (
  dataset_id TEXT PRIMARY KEY,
  next_scn   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS vec_shadow_log (
  dataset_id   TEXT NOT NULL,
  shadow_table TEXT NOT NULL,
  scn          INTEGER NOT NULL,
  op           TEXT NOT NULL,
  document_id  TEXT NOT NULL,
  payload      BLOB NOT NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(dataset_id, shadow_table, scn)
);

CREATE TABLE IF NOT EXISTS vec_sync_state (
  dataset_id   TEXT NOT NULL,
  shadow_table TEXT NOT NULL,
  last_scn     INTEGER NOT NULL DEFAULT 0,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(dataset_id, shadow_table)
);

CREATE TABLE IF NOT EXISTS vector_storage (
  shadow_table_name TEXT NOT NULL,
  dataset_id        TEXT NOT NULL DEFAULT '',
  "index"           BLOB,
  PRIMARY KEY (shadow_table_name, dataset_id)
);

CREATE TABLE IF NOT EXISTS emb_root (
  dataset_id      TEXT PRIMARY KEY,
  source_uri      TEXT,
  description     TEXT,
  last_indexed_at TIMESTAMP,
  last_scn        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS emb_asset (
  dataset_id TEXT NOT NULL,
  asset_id   TEXT NOT NULL,
  path       TEXT NOT NULL,
  md5        TEXT NOT NULL,
  size       INTEGER NOT NULL,
  mod_time   TIMESTAMP NOT NULL,
  scn        INTEGER NOT NULL,
  archived   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (dataset_id, asset_id)
);

CREATE TABLE IF NOT EXISTS _vec_emb_docs (
  dataset_id       TEXT NOT NULL,
  id               TEXT NOT NULL,
  asset_id         TEXT NOT NULL,
  content          TEXT,
  meta             TEXT,
  embedding        BLOB,
  embedding_model  TEXT,
  scn              INTEGER NOT NULL,
  archived         INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (dataset_id, id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS emb_docs USING vec(doc_id);

CREATE INDEX IF NOT EXISTS idx_emb_asset_path
  ON emb_asset(dataset_id, path);

CREATE INDEX IF NOT EXISTS idx_emb_asset_mod
  ON emb_asset(dataset_id, mod_time);

CREATE INDEX IF NOT EXISTS idx_vec_docs_asset
  ON _vec_emb_docs(dataset_id, asset_id);

CREATE INDEX IF NOT EXISTS idx_vec_docs_scn
  ON _vec_emb_docs(dataset_id, scn);

CREATE INDEX IF NOT EXISTS idx_vec_docs_archived
  ON _vec_emb_docs(dataset_id, archived);
