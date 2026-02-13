-- Embedius upstream MySQL schema (source of truth + SCN log)

CREATE TABLE IF NOT EXISTS vec_dataset (
  dataset_id   VARCHAR(255) PRIMARY KEY,
  description  TEXT,
  source_uri   TEXT,
  last_scn     BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS vec_dataset_scn (
  dataset_id VARCHAR(255) PRIMARY KEY,
  next_scn   BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS vec_shadow_log (
  dataset_id   VARCHAR(255) NOT NULL,
  shadow_table VARCHAR(255) NOT NULL,
  scn          BIGINT NOT NULL,
  op           VARCHAR(16) NOT NULL,
  document_id  VARCHAR(512) NOT NULL,
  payload      LONGBLOB NOT NULL,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(dataset_id, shadow_table, scn)
);

CREATE TABLE IF NOT EXISTS vec_sync_state (
  dataset_id   VARCHAR(255) NOT NULL,
  shadow_table VARCHAR(255) NOT NULL,
  last_scn     BIGINT NOT NULL DEFAULT 0,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY(dataset_id, shadow_table)
);

CREATE TABLE IF NOT EXISTS shadow_vec_docs (
  dataset_id       VARCHAR(255) NOT NULL,
  id               VARCHAR(512) NOT NULL,
  content          MEDIUMTEXT,
  meta             MEDIUMTEXT,
  embedding        LONGBLOB,
  embedding_model  VARCHAR(255),
  scn              BIGINT NOT NULL DEFAULT 0,
  archived         TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY(dataset_id, id)
);

CREATE TABLE IF NOT EXISTS emb_root (
  dataset_id      VARCHAR(255) PRIMARY KEY,
  source_uri      TEXT,
  description     TEXT,
  last_indexed_at TIMESTAMP NULL,
  last_scn        BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS emb_root_config (
  dataset_id     VARCHAR(255) PRIMARY KEY,
  include_globs  TEXT,
  exclude_globs  TEXT,
  max_size_bytes BIGINT NOT NULL DEFAULT 0,
  updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS emb_asset (
  dataset_id VARCHAR(255) NOT NULL,
  asset_id   VARCHAR(512) NOT NULL,
  path       TEXT NOT NULL,
  md5        VARCHAR(64) NOT NULL,
  size       BIGINT NOT NULL,
  mod_time   TIMESTAMP NOT NULL,
  scn        BIGINT NOT NULL,
  archived   TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (dataset_id, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_emb_asset_path
  ON emb_asset(dataset_id, path(255));

CREATE INDEX IF NOT EXISTS idx_emb_asset_mod
  ON emb_asset(dataset_id, mod_time);

CREATE INDEX IF NOT EXISTS idx_shadow_vec_docs_scn
  ON shadow_vec_docs(dataset_id, scn);

CREATE INDEX IF NOT EXISTS idx_shadow_vec_docs_archived
  ON shadow_vec_docs(dataset_id, archived);
