-- Служебные таблицы: журнал загрузок, водяные знаки, реестр файлов, качество.
-- схема meta и schema_migrations создаются раннером миграций

-- Каждая загрузка из источника в stg — отдельная партия с номером.
-- Слой Data Vault потом забирает партии строго по порядку номеров.
CREATE TABLE meta.batches (
    batch_id        bigserial PRIMARY KEY,
    pipeline        text        NOT NULL,
    target_table    text        NOT NULL,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    status          text        NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'loaded', 'failed', 'empty', 'rejected')),
    rows_loaded     bigint,
    watermark_from  text,
    watermark_to    text,
    source_object   text,
    error           text,
    vault_loaded_at timestamptz
);
CREATE INDEX batches_pending_ix ON meta.batches (target_table, batch_id)
    WHERE status = 'loaded' AND vault_loaded_at IS NULL;

CREATE TABLE meta.watermarks (
    pipeline    text PRIMARY KEY,
    value       text        NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Файл считается обработанным по ключу и ETag: если поставщик перезальёт
-- файл с тем же именем, но другим содержимым, он загрузится ещё раз.
CREATE TABLE meta.processed_files (
    object_key    text        NOT NULL,
    etag          text        NOT NULL,
    pipeline      text        NOT NULL,
    batch_id      bigint      REFERENCES meta.batches,
    rows_loaded   bigint,
    processed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (object_key, etag)
);

CREATE TABLE meta.dq_results (
    id           bigserial PRIMARY KEY,
    run_id       text        NOT NULL,
    check_name   text        NOT NULL,
    table_name   text        NOT NULL,
    severity     text        NOT NULL CHECK (severity IN ('warn', 'fail')),
    passed       boolean     NOT NULL,
    observed     numeric,
    threshold    numeric,
    details      text,
    checked_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dq_results_checked_ix ON meta.dq_results (checked_at DESC);
