/* contrib/scheduler/scheduler--1.0.sql */

-- Если кто-то попытается запустить скрипт напрямую, напомним, что нужно использовать CREATE EXTENSION
\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

-- Создание схемы для хранения объектов расширения
CREATE SCHEMA IF NOT EXISTS scheduler;

-- Установка search_path: сначала искать объекты в схеме scheduler, потом в public
SET search_path = scheduler, public;

-- Основная таблица со списком заданий (jobs)
CREATE TABLE scheduler.jobs (
    job_id       serial       PRIMARY KEY,                              -- Уникальный ID задания
    schedule     text,                                                  -- Формат расписания (NULL для one-shot)
    command      text         NOT NULL,                                 -- Команда или SQL-запрос для выполнения
    owner        regrole      NOT NULL DEFAULT current_user::regrole,   -- Владелец задания
    next_run     timestamptz,                                           -- Следующее время запуска
    last_run     timestamptz,                                           -- Последнее время запуска
    last_status  text                                                   -- Последний статус выполнения
);

-- Лог выполнения заданий
CREATE TABLE scheduler.job_run_log (
    run_id     bigserial     PRIMARY KEY,
    job_id     int           REFERENCES scheduler.jobs ON DELETE CASCADE,
    started_at timestamptz   NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    success    bool,
    message    text
);

-- Функция добавления нового задания (C-функция в scheduler.so)
CREATE FUNCTION scheduler.schedule(_cron text, _cmd text)
RETURNS int
LANGUAGE C STRICT PARALLEL SAFE AS 'MODULE_PATHNAME', 'scheduler_schedule';

CREATE FUNCTION scheduler.schedule_once(delay interval, cmd text)
RETURNS int
LANGUAGE C STRICT PARALLEL SAFE AS 'MODULE_PATHNAME', 'scheduler_schedule_once';

-- Функция удаления задания по ID
CREATE FUNCTION scheduler.unschedule(_job int)
RETURNS void
LANGUAGE C STRICT PARALLEL SAFE AS 'MODULE_PATHNAME', 'scheduler_unschedule';

-- Функция немедленного выполнения задания по ID
CREATE FUNCTION scheduler.run_now(_job int)
RETURNS void
LANGUAGE C STRICT PARALLEL SAFE AS 'MODULE_PATHNAME', 'scheduler_run_now';

-- Разрешение на выполнение этих функций для всех пользователей
GRANT EXECUTE ON FUNCTION scheduler.schedule(text,text)             TO public;
GRANT EXECUTE ON FUNCTION scheduler.schedule_once(interval, text)   TO public;
GRANT EXECUTE ON FUNCTION scheduler.unschedule(int)                 TO public;
GRANT EXECUTE ON FUNCTION scheduler.run_now(int)                    TO public;

-- Комментарий для всей схемы
COMMENT ON SCHEMA scheduler IS 'Light‑weight built‑in job scheduler';