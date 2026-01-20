-- MySQL triggers for SCN logging from shadow table into vec_shadow_log.
-- Requires vec_dataset_scn and vec_shadow_log tables from schema.ddl.

DELIMITER $$

CREATE TRIGGER shadow_vec_docs_ai AFTER INSERT ON shadow_vec_docs
FOR EACH ROW
BEGIN
  INSERT INTO vec_dataset_scn(dataset_id, next_scn)
  VALUES (NEW.dataset_id, 1)
  ON DUPLICATE KEY UPDATE next_scn = next_scn + 1;

  SELECT next_scn INTO @emb_scn FROM vec_dataset_scn WHERE dataset_id = NEW.dataset_id;

  INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload)
  VALUES (
    NEW.dataset_id,
    'shadow_vec_docs',
    @emb_scn,
    'insert',
    NEW.id,
    JSON_OBJECT(
      'dataset_id', NEW.dataset_id,
      'id', NEW.id,
      'content', NEW.content,
      'meta', NEW.meta,
      'embedding', LOWER(HEX(NEW.embedding)),
      'embedding_model', NEW.embedding_model,
      'scn', NEW.scn,
      'archived', NEW.archived
    )
  );
END$$

CREATE TRIGGER shadow_vec_docs_au AFTER UPDATE ON shadow_vec_docs
FOR EACH ROW
BEGIN
  INSERT INTO vec_dataset_scn(dataset_id, next_scn)
  VALUES (NEW.dataset_id, 1)
  ON DUPLICATE KEY UPDATE next_scn = next_scn + 1;

  SELECT next_scn INTO @emb_scn FROM vec_dataset_scn WHERE dataset_id = NEW.dataset_id;

  INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload)
  VALUES (
    NEW.dataset_id,
    'shadow_vec_docs',
    @emb_scn,
    'update',
    NEW.id,
    JSON_OBJECT(
      'dataset_id', NEW.dataset_id,
      'id', NEW.id,
      'content', NEW.content,
      'meta', NEW.meta,
      'embedding', LOWER(HEX(NEW.embedding)),
      'embedding_model', NEW.embedding_model,
      'scn', NEW.scn,
      'archived', NEW.archived
    )
  );
END$$

CREATE TRIGGER shadow_vec_docs_ad AFTER DELETE ON shadow_vec_docs
FOR EACH ROW
BEGIN
  INSERT INTO vec_dataset_scn(dataset_id, next_scn)
  VALUES (OLD.dataset_id, 1)
  ON DUPLICATE KEY UPDATE next_scn = next_scn + 1;

  SELECT next_scn INTO @emb_scn FROM vec_dataset_scn WHERE dataset_id = OLD.dataset_id;

  INSERT INTO vec_shadow_log(dataset_id, shadow_table, scn, op, document_id, payload)
  VALUES (
    OLD.dataset_id,
    'shadow_vec_docs',
    @emb_scn,
    'delete',
    OLD.id,
    JSON_OBJECT(
      'dataset_id', OLD.dataset_id,
      'id', OLD.id,
      'content', OLD.content,
      'meta', OLD.meta,
      'embedding', LOWER(HEX(OLD.embedding)),
      'embedding_model', OLD.embedding_model,
      'scn', OLD.scn,
      'archived', OLD.archived
    )
  );
END$$

DELIMITER ;
