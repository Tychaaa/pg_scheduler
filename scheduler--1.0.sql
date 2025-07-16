/* contrib/scheduler/scheduler--1.0.sql */

/*
 *  Загружайте расширение командой
 *      CREATE EXTENSION scheduler;
 *  Запуск этого файла напрямую не нужен.
 */
\echo Use "CREATE EXTENSION scheduler" to load this file. \quit

/*------------------------------------------------------------
 * 1. Схема и search_path
 *-----------------------------------------------------------*/
CREATE SCHEMA IF NOT EXISTS scheduler;
SET search_path = scheduler, public;

/*------------------------------------------------------------
 * 2. Таблицы
 *-----------------------------------------------------------*/
-- Таблица заданий
CREATE TABLE scheduler.jobs (
    job_id       serial       PRIMARY KEY,                              -- уникальный ID
    schedule     text,                                                  -- cron-строка; NULL = one-shot
    command      text         NOT NULL,                                 -- SQL-команда или shell-строка
    owner        regrole      NOT NULL DEFAULT current_user::regrole,   -- владелец задания
    next_run     timestamptz,                                           -- время следующего запуска
    last_run     timestamptz,                                           -- время последнего запуска
    last_status  text,                                                  -- последний статус / сообщение
    is_shell     boolean      NOT NULL DEFAULT false                    -- true → выполнять через system()
);

-- Журнал запусков
CREATE TABLE scheduler.job_run_log (
    run_id       bigserial    PRIMARY KEY,                                  -- уникальный ID запуска (журнальный)
    job_id       int          REFERENCES scheduler.jobs ON DELETE CASCADE,  -- ссылка на задание (при удалении job — удаляются и его логи)
    started_at   timestamptz  NOT NULL DEFAULT clock_timestamp(),           -- время начала выполнения
    finished_at  timestamptz,                                               -- время завершения (может быть NULL при сбое)
    success      bool,                                                      -- флаг: успешно ли выполнено
    message      text                                                       -- сообщение: OK, ошибка или статус shell-команды
);

/*------------------------------------------------------------
 * 3. C-функции-обёртки
 *-----------------------------------------------------------*/

-- Функция добавления нового задания
CREATE FUNCTION scheduler.schedule(
            _cron      text,
            _cmd       text,
            _is_shell  boolean DEFAULT false)
RETURNS int
LANGUAGE C STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'scheduler_schedule';

-- Функция добавления нового одноразового задания
CREATE FUNCTION scheduler.schedule_once(
            delay      interval,
            cmd        text,
            _is_shell  boolean DEFAULT false)
RETURNS int
LANGUAGE C STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'scheduler_schedule_once';

-- Функция удаления задания по ID
CREATE FUNCTION scheduler.unschedule(_job int)
RETURNS void
LANGUAGE C STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'scheduler_unschedule';

-- Функция немедленного выполнения задания по ID
CREATE FUNCTION scheduler.run_now(_job int)
RETURNS void
LANGUAGE C STRICT PARALLEL SAFE
AS 'MODULE_PATHNAME', 'scheduler_run_now';

/*------------------------------------------------------------
 * 4. Права
 *-----------------------------------------------------------*/
GRANT EXECUTE ON FUNCTION scheduler.schedule(text, text, boolean)           TO public;
GRANT EXECUTE ON FUNCTION scheduler.schedule_once(interval, text, boolean)  TO public;
GRANT EXECUTE ON FUNCTION scheduler.unschedule(int)                         TO public;
GRANT EXECUTE ON FUNCTION scheduler.run_now(int)                            TO public;

/*------------------------------------------------------------
 * 5. Комментарий
 *-----------------------------------------------------------*/
COMMENT ON SCHEMA scheduler IS 'Light-weight built-in job scheduler';