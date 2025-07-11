/* contrib/scheduler/scheduler--1.0.sql */

-- Если кто-то попытается запустить скрипт напрямую через \i
\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

-- схема для изоляции элементов шедулера
CREATE SCHEMA IF NOT EXISTS scheduler;

-- тип команды: SQL или shell
CREATE TYPE scheduler.command_type AS ENUM ('sql', 'shell');

-- основная таблица заданий
CREATE TABLE scheduler.jobs (
    job_id        BIGSERIAL PRIMARY KEY,
    job_name      TEXT            NOT NULL UNIQUE,
    enabled       BOOLEAN         NOT NULL DEFAULT true,
    command_type  scheduler.command_type NOT NULL,
    command       TEXT            NOT NULL,
    schedule_cron TEXT            NULL,
    schedule_intv INTERVAL        NULL,
    next_run      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- функция создания задания
CREATE FUNCTION scheduler.create_job(
    p_job_name      TEXT,
    p_command_type  scheduler.command_type,
    p_command       TEXT,
    p_schedule_cron TEXT    DEFAULT NULL,
    p_schedule_intv INTERVAL DEFAULT NULL
) RETURNS BIGINT
LANGUAGE sql AS $$
    -- вставляем строку и возвращаем job_id
    INSERT INTO scheduler.jobs(job_name, command_type, command, schedule_cron, schedule_intv, next_run)
    VALUES (p_job_name, p_command_type, p_command, p_schedule_cron, p_schedule_intv,
        CASE
          WHEN p_schedule_cron IS NOT NULL THEN now()
          WHEN p_schedule_intv IS NOT NULL THEN now() + p_schedule_intv
          ELSE now()
        END
    )
    RETURNING job_id;
$$;

-- функция выключения задания
CREATE FUNCTION scheduler.disable_job(p_job_id BIGINT) RETURNS VOID
LANGUAGE sql AS $$
    UPDATE scheduler.jobs SET enabled = false WHERE job_id = p_job_id;
$$;

-- функция включения задания
CREATE FUNCTION scheduler.enable_job(p_job_id BIGINT) RETURNS VOID
LANGUAGE sql AS $$
    UPDATE scheduler.jobs SET enabled = true WHERE job_id = p_job_id;
$$;

-- функция удаления задания
CREATE FUNCTION scheduler.drop_job(p_job_id BIGINT) RETURNS VOID
LANGUAGE sql AS $$
    DELETE FROM scheduler.jobs WHERE job_id = p_job_id;
$$;
