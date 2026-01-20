-- Embedius upstream BigQuery schema (source of truth + SCN log)

CREATE TABLE IF NOT EXISTS `vec_dataset` (
  dataset_id   STRING NOT NULL,
  description  STRING,
  source_uri   STRING,
  last_scn     INT64 NOT NULL
);

CREATE TABLE IF NOT EXISTS `vec_dataset_scn` (
  dataset_id STRING NOT NULL,
  next_scn   INT64 NOT NULL
);

CREATE TABLE IF NOT EXISTS `vec_shadow_log` (
  dataset_id   STRING NOT NULL,
  shadow_table STRING NOT NULL,
  scn          INT64 NOT NULL,
  op           STRING NOT NULL,
  document_id  STRING NOT NULL,
  payload      BYTES NOT NULL,
  created_at   TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY dataset_id, shadow_table, scn;

CREATE TABLE IF NOT EXISTS `shadow_vec_docs` (
  dataset_id       STRING NOT NULL,
  id               STRING NOT NULL,
  content          STRING,
  meta             STRING,
  embedding        BYTES,
  embedding_model  STRING,
  scn              INT64 NOT NULL,
  archived         BOOL NOT NULL
)
CLUSTER BY dataset_id, scn, archived;

CREATE TABLE IF NOT EXISTS `emb_root` (
  dataset_id      STRING NOT NULL,
  source_uri      STRING,
  description     STRING,
  last_indexed_at TIMESTAMP,
  last_scn        INT64 NOT NULL
)
CLUSTER BY dataset_id;

CREATE TABLE IF NOT EXISTS `emb_asset` (
  dataset_id STRING NOT NULL,
  asset_id   STRING NOT NULL,
  path       STRING NOT NULL,
  md5        STRING NOT NULL,
  size       INT64 NOT NULL,
  mod_time   TIMESTAMP NOT NULL,
  scn        INT64 NOT NULL,
  archived   BOOL NOT NULL
)
PARTITION BY DATE(mod_time)
CLUSTER BY dataset_id, scn, archived;
