#include "postgres.h"             /* Основной заголовок PostgreSQL-модуля */
#include "access/xact.h"          /* Управление транзакциями */
#include "executor/spi.h"         /* SPI-интерфейс для выполнения SQL из C */
#include "fmgr.h"                 /* PG_FUNCTION_ARGS, PG_MODULE_MAGIC и др. */
#include "miscadmin.h"            /* Инфо о процессе, пользователе и т.п. */
#include "postmaster/bgworker.h"  /* Регистрация фоновых воркеров */
#include "storage/ipc.h"          /* IPC: сигналы, завершения и прерывания */
#include "storage/latch.h"        /* Latch — механизм ожидания событий */
#include "pgtime.h"               /* Работа с датой и временем (pg_localtime) */
#include "utils/wait_event.h"     /* Ожидание событий (WaitLatch) */
#include "utils/builtins.h"       /* Встроенные функции: text_to_cstring и др. */
#include "utils/memutils.h"       /* Работа с MemoryContext'ами */
#include "utils/guc.h"            /* Определение GUC-переменных */
#include "utils/timestamp.h"      /* Работа с TimestampTz */
#include <ctype.h>                /* C: isdigit, isspace и т.п. */
#include <signal.h>               /* C: обработка сигналов (SIGTERM и др.) */

#ifndef WAIT_EVENT_EXTENSION       /* Если макрос не определён — задать значение по умолчанию */
#define WAIT_EVENT_EXTENSION 0
#endif

#define UNIX_EPOCH_IN_POSTGRES 946684800  /* Сдвиг между Unix- и PostgreSQL-эпохами (в секундах) */

PG_MODULE_MAGIC;                  /* Обязательная строка для регистрации расширения */

/* ---------- конфигурация (GUC-переменные) ---------- */
static int  scheduler_tick_ms   = 60000;  /* Интервал пробуждения фонового процесса (мс) */
static int  scheduler_log_level = LOG;    /* Уровень логирования: LOG */

/* ---------- сигналы ---------- */
static volatile sig_atomic_t got_sigterm = false;  /* Получен ли сигнал SIGTERM */

/* Точка входа для фонового воркера */
PGDLLEXPORT void scheduler_launcher_main(Datum arg);

/*
 * scheduler_sigterm()
 *   Обработчик сигнала SIGTERM.
 *   Устанавливает флаг got_sigterm и пробуждает процесс через latch.
 *   Используется для корректного завершения фонового воркера.
 */
static void
scheduler_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;       /* Сохраняем errno, чтобы не потерять системную ошибку */
    got_sigterm = true;           /* Устанавливаем флаг завершения */
    if (MyLatch)                  /* Если latch инициализирован — пробуждаем процесс */
        SetLatch(MyLatch);
    errno = save_errno;           /* Восстанавливаем errno */
}

/*
 * parse_next_run()
 *   Вычисляет ближайшее время следующего запуска задания по cron-строке.
 *   Поддерживает форматы:
 *     • "* /N * * * *" — каждые N минут
 *     • "MM HH * * *"  — ежедневно в HH:MM
 *     • "MM HH * * D"  — еженедельно в HH:MM (где D = 0–6, 0 — воскресенье)
 */
static TimestampTz
parse_next_run(const char *cron)
{
    /* ---------- текущее время (локальная календарная дата) ---------- */
    TimestampTz now_ts = GetCurrentTimestamp();     /* Время "сейчас" в формате PostgreSQL */

    pg_time_t unix_sec = (pg_time_t)(now_ts / USECS_PER_SEC)
                        + UNIX_EPOCH_IN_POSTGRES;   /* Перевод в UNIX-время */

    struct tm *lt = pg_localtime(&unix_sec, session_timezone);  /* Локализуем время с учётом timezone */
    if (!lt)
        ereport(ERROR,
                (errmsg("scheduler: pg_localtime() failed")));

    /* Переносим поля struct tm в структуру pg_tm */
    struct pg_tm now_tm;
    now_tm.tm_year = lt->tm_year + 1900;  /* Год: +1900 */
    now_tm.tm_mon  = lt->tm_mon  + 1;     /* Месяц: 1-based */
    now_tm.tm_mday = lt->tm_mday;
    now_tm.tm_hour = lt->tm_hour;
    now_tm.tm_min  = lt->tm_min;
    now_tm.tm_sec  = lt->tm_sec;
    now_tm.tm_wday = lt->tm_wday;         /* День недели: 0=вс, 1=пн, ..., 6=сб */

    /* ---------- разбор cron‑строки на поля ---------- */
    char  *copy = pstrdup(cron);          /* Создаём изменяемую копию */
    char  *fld[5];                        /* Пять полей cron-строки */
    int    n = 0;

    for (char *tok = strtok(copy, " \t"); tok && n < 5;
         tok = strtok(NULL, " \t"))
        fld[n++] = tok;

    /* Проверка на корректное число полей */
    if (n != 5 || strtok(NULL, " \t") != NULL)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("scheduler: invalid cron expression \"%s\"", cron)));

    /* ---------- 1. каждые N минут ---------- */
    if (strncmp(fld[0], "*/", 2) == 0 &&
        strcmp(fld[1], "*")  == 0 &&
        strcmp(fld[2], "*")  == 0 &&
        strcmp(fld[3], "*")  == 0 &&
        strcmp(fld[4], "*")  == 0)
    {
        /* Получаем значение N из "* /N" */
        int N = pg_strtoint32(fld[0] + 2);
        if (N <= 0 || N > 59)
            ereport(ERROR,
                    (errmsg("scheduler: bad minute step \"%s\"", fld[0])));

        return now_ts + (int64) N * USECS_PER_MINUTE;   /* Через N минут */
    }

    /* ---------- разбор часов и минут ---------- */
    int minute = pg_strtoint32(fld[0]);
    int hour   = pg_strtoint32(fld[1]);
    if (minute < 0 || minute > 59 || hour < 0 || hour > 23)
        ereport(ERROR,
                (errmsg("scheduler: bad hour/minute in \"%s\"", cron)));

    /* ---------- формируем сегодняшнее HH:MM как timestamptz ---------- */
    char tsbuf[64];
    snprintf(tsbuf, sizeof(tsbuf), "%04d-%02d-%02d %02d:%02d:00",
             now_tm.tm_year, now_tm.tm_mon, now_tm.tm_mday, hour, minute);

    TimestampTz cand = DatumGetTimestampTz(
                           DirectFunctionCall3(timestamptz_in,
                                               CStringGetDatum(tsbuf),
                                               ObjectIdGetDatum(InvalidOid),
                                               Int32GetDatum(-1)));

    /* ---------- 2. ежедневно ---------- */
    if (strcmp(fld[2], "*") == 0 && strcmp(fld[3], "*") == 0 &&
        strcmp(fld[4], "*") == 0)
    {
        if (cand <= now_ts)                 /* Если время уже прошло — сдвигаем на завтра */
            cand += USECS_PER_DAY;
        return cand;
    }

    /* ---------- 3. еженедельно ---------- */
    if (strcmp(fld[2], "*") == 0 && strcmp(fld[3], "*") == 0)
    {
        int dow = pg_strtoint32(fld[4]);    /* День недели: 0 (вс) … 6 (сб) */
        if (dow < 0 || dow > 6)
            ereport(ERROR,
                    (errmsg("scheduler: bad day‑of‑week in \"%s\"", cron)));

        /* Сколько суток нужно прибавить до нужного дня недели */
        int diff = (dow - now_tm.tm_wday + 7) % 7;

        /* Если сегодня нужный день, но время уже прошло — сдвигаем на след. неделю */
        if (diff == 0 && cand <= now_ts)
            diff = 7;

        cand += (int64) diff * USECS_PER_DAY;
        return cand;
    }

    /* ---------- если формат не распознан ---------- */
    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("scheduler: unsupported cron format \"%s\"", cron)));
    return 0;
}

/*
 * _PG_init()
 *   Вызывается при инициализации расширения.
 *   Регистрирует GUC-переменные и фоновый воркер (launcher).
 */
void
_PG_init(void)
{
    /* ---------- регистрация GUC-переменных ---------- */

    /* Интервал опроса: как часто фоновый воркер будет просыпаться (в миллисекундах) */
    DefineCustomIntVariable("scheduler.tick_ms",
                            "Launcher check interval in ms.",  /* Комментарий для SHOW */
                            NULL,                              /* без короткого описания */
                            &scheduler_tick_ms,                /* переменная */
                            60000, 1000, 600000,               /* default, min, max */
                            PGC_POSTMASTER, 0,                 /* уровень: только при старте */
                            NULL, NULL, NULL);

    /* Уровень логирования результатов заданий */
    DefineCustomIntVariable("scheduler.log_level",
                            "Log level for job results.",      /* Комментарий для SHOW */
                            NULL,
                            &scheduler_log_level,
                            LOG, DEBUG1, ERROR,                /* default, min, max */
                            PGC_SIGHUP, 0,                     /* можно менять через SIGHUP */
                            NULL, NULL, NULL);

    /* ---------- регистрация фонового воркера ---------- */

    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(worker));  /* Обнуляем структуру */

    /* Флаги: доступ к shared memory и подключение к БД */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;

    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;  /* запуск после восстановления */
    worker.bgw_restart_time = 10;                              /* перезапуск через 10 сек при сбое */

    /* Названия (для логов и отладки) */
    snprintf(worker.bgw_name,          BGW_MAXLEN, "scheduler launcher");
    snprintf(worker.bgw_library_name,  BGW_MAXLEN, "scheduler");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "scheduler_launcher_main");

    worker.bgw_main_arg   = (Datum) 0;   /* без параметров */
    worker.bgw_notify_pid = 0;           /* не уведомляем о запуске */

    RegisterBackgroundWorker(&worker);   /* Регистрируем воркера */
}

/*
 * execute_job()
 *   Выполняет SQL-команду job’а в под-транзакции.
 *   Логирует результат в таблицу scheduler.job_run_log.
 *   Предполагается, что:
 *     • внешняя транзакция уже активна;
 *     • SPI_connect() уже выполнен;
 *     • функция вызывается из launcher_tick().
 */
static void
execute_job(int32 job_id, const char *command)
{
    bool        success  = false;   /* Флаг успеха выполнения команды */
    const char *msg_txt  = NULL;    /* Текст сообщения (OK или ошибка) */

    /* ---------- 1. выполняем команду в под-транзакции ---------- */
    PG_TRY();
    {
        BeginInternalSubTransaction(NULL);           /* Начинаем под-транзакцию */

        if (SPI_execute(command, false, 0) < 0)      /* Выполняем SQL */
            elog(ERROR, "scheduler: SPI_execute failed");

        success = true;
        msg_txt = "OK";                              /* Всё прошло успешно */
        ReleaseCurrentSubTransaction();              /* Завершаем под-транзакцию */
    }
    PG_CATCH();
    {
        ErrorData *edata = CopyErrorData();          /* Сохраняем сообщение об ошибке */
        msg_txt = edata->message ? edata->message : "ERROR";  /* Сообщение для лога */
        FlushErrorState();

        RollbackAndReleaseCurrentSubTransaction();   /* Откат под-транзакции */
        success = false;
    }
    PG_END_TRY();

    /* ---------- 2. записываем результат в таблицу job_run_log ---------- */
    {
        const char *sql_log =
            "INSERT INTO scheduler.job_run_log"
            "(job_id, success, message, finished_at) "
            "VALUES ($1, $2, $3, clock_timestamp())";

        Oid   at[3]  = {INT4OID, BOOLOID, TEXTOID};  /* Типы аргументов */
        Datum vl[3]  = {Int32GetDatum(job_id),       /* Значения аргументов */
                        BoolGetDatum(success),
                        CStringGetTextDatum(msg_txt)};
        char  nl[3]  = {' ',' ',' '};                /* NULL-маска (все NOT NULL) */

        if (SPI_execute_with_args(sql_log, 3, at, vl, nl, false, 0) < 0)
            elog(WARNING, "scheduler: cannot insert into job_run_log");
    }

    /* ---------- 3. обновляем поле last_status в таблице jobs ---------- */
    {
        const char *sql_upd =
            "UPDATE scheduler.jobs "
            "SET    last_status = $2 "
            "WHERE  job_id      = $1";

        Oid   at[2]  = {INT4OID, TEXTOID};
        Datum vl[2]  = {Int32GetDatum(job_id),
                        CStringGetTextDatum(msg_txt)};
        char  nl[2]  = {' ',' '};

        SPI_execute_with_args(sql_upd, 2, at, vl, nl, false, 0);
    }

    /* ---------- 4. пишем результат в server-log ---------- */
    ereport(scheduler_log_level,
            (errmsg("scheduler: job %d %s", job_id,
                    success ? "completed" : "failed")));
}

/*
 * execute_shell_job()
 *   Выполняет внешнюю shell-команду (через system()).
 *   Записывает результат в журнал scheduler.job_run_log.
 *   Предполагается, что:
 *     • внешняя транзакция и SPI_connect() уже активны;
 *     • используется внутри launcher_tick().
 */
static void
execute_shell_job(int32 job_id, const char *command)
{
    int rc = system(command);                   /* Выполняем команду через оболочку */
    bool success = (rc == 0);                   /* Успех = код возврата 0 */
    char status_buf[64];                        /* Буфер для текста статуса */

    if (WIFEXITED(rc))                          /* Завершилось корректно */
        snprintf(status_buf, sizeof(status_buf),
                 "rc=%d", WEXITSTATUS(rc));     /* Код выхода */
    else                                        /* Завершилось по сигналу */
        snprintf(status_buf, sizeof(status_buf),
                 "signal=%d", WTERMSIG(rc));    /* Номер сигнала завершения */

    /* ---------- 1. вставляем запись в журнал выполнения ---------- */
    Oid   at[3]  = {INT4OID, BOOLOID, TEXTOID};       /* Типы аргументов */
    Datum vl[3]  = {Int32GetDatum(job_id),            /* Значения аргументов */
                    BoolGetDatum(success),
                    CStringGetTextDatum(status_buf)};
    char  nl[3]  = {' ',' ',' '};                     /* NULL-маска */

    SPI_execute_with_args(
        "INSERT INTO scheduler.job_run_log "
        "(job_id, success, message, finished_at) "
        "VALUES ($1, $2, $3, clock_timestamp())",
        3, at, vl, nl, false, 0);

    /* ---------- 2. обновляем last_status в таблице jobs ---------- */
    Oid   at2[2]  = {INT4OID, TEXTOID};
    Datum vl2[2]  = {Int32GetDatum(job_id),
                     CStringGetTextDatum(status_buf)};
    char  nl2[2]  = {' ',' '};

    SPI_execute_with_args(
        "UPDATE scheduler.jobs SET last_status = $2 WHERE job_id = $1",
        2, at2, vl2, nl2, false, 0);

    /* ---------- 3. логируем в серверный журнал ---------- */
    ereport(scheduler_log_level,
            (errmsg("scheduler: job %d %s", job_id,
                    success ? "completed" : status_buf)));
}

/*
 * launcher_tick()
 *   Один проход планировщика:
 *     • выбирает все задания, просроченные по next_run;
 *     • выполняет каждое (SQL или shell);
 *     • пересчитывает next_run или удаляет one-shot;
 *     • ведёт лог и обновляет статус.
 */
static void
launcher_tick(void)
{
    /* ---------- 1. открыть транзакцию и сеанс SPI ---------- */
    StartTransactionCommand();                       /* Начинаем обычную SQL-транзакцию */

    if (SPI_connect() != SPI_OK_CONNECT)             /* Подключаемся к SPI-интерфейсу */
        elog(ERROR, "scheduler: SPI_connect failed");

    PushActiveSnapshot(GetTransactionSnapshot());    /* Устанавливаем snapshot для запросов */

    /* ---------- 2. выбрать задания, которые пора выполнить ---------- */
    const char *sql_sel =
        "SELECT job_id, command, schedule, is_shell "
        "FROM   scheduler.jobs "
        "WHERE  next_run <= clock_timestamp() "
        "FOR UPDATE SKIP LOCKED";   /* блокируем без ожидания — для многопоточности */

    if (SPI_execute(sql_sel, false, 0) != SPI_OK_SELECT)
        elog(ERROR, "scheduler: SELECT jobs failed");

    /* ---------- 3. обрабатываем каждое задание по очереди ---------- */
    for (uint64 i = 0; i < SPI_processed; i++)
    {
        HeapTuple tup  = SPI_tuptable->vals[i];         /* строка результата */
        TupleDesc desc = SPI_tuptable->tupdesc;         /* описание структуры */
        bool      isnull;

        int32 job_id  = DatumGetInt32(SPI_getbinval(tup, desc, 1, &isnull));             /* ID задания */
        char *command = TextDatumGetCString(SPI_getbinval(tup, desc, 2, &isnull));       /* SQL или shell-команда */

        Datum sched_datum = SPI_getbinval(tup, desc, 3, &isnull);   /* cron-строка или NULL */
        bool  oneshot     = isnull;                                 /* одноразовое задание? */
        char *cron        = oneshot ? NULL : TextDatumGetCString(sched_datum);  /* cron‑строка */

        bool  is_shell    = DatumGetBool(SPI_getbinval(tup, desc, 4, &isnull));  /* shell-команда? */

        /* ---------- 3.1 выполняем задание внутри под-транзакции ---------- */
        PG_TRY();
        {
            BeginInternalSubTransaction(NULL);    /* изолированная под-транзакция */

            if (is_shell)
                execute_shell_job(job_id, command);  /* Внешняя команда */
            else
                execute_job(job_id, command);        /* SQL-команда */

            ReleaseCurrentSubTransaction();      /* commit под-транзакции */
        }
        PG_CATCH();
        {
            RollbackAndReleaseCurrentSubTransaction();  /* rollback под-транзакции */
            FlushErrorState();                          /* очищаем ошибку, идём дальше */
        }
        PG_END_TRY();

        /* ---------- 3.2 post-processing ---------- */
        if (oneshot)
        {
            /* Одноразовое задание — удаляем строку */
            Oid   at[1]  = {INT4OID};
            Datum vl[1]  = {Int32GetDatum(job_id)};
            char  nl[1]  = {' '};

            SPI_execute_with_args(
                "DELETE FROM scheduler.jobs WHERE job_id = $1",
                1, at, vl, nl, false, 0);
        }
        else
        {
            /* Повторяющееся — вычисляем следующее время */
            TimestampTz next = parse_next_run(cron);

            Oid   at[2] = {INT4OID, TIMESTAMPTZOID};
            Datum vl[2] = {Int32GetDatum(job_id),
                           TimestampTzGetDatum(next)};
            char  nl[2] = {' ', ' '};

            SPI_execute_with_args(
                "UPDATE scheduler.jobs "
                "SET    last_run = clock_timestamp(), "
                "       next_run = $2 "
                "WHERE  job_id   = $1",
                2, at, vl, nl, false, 0);
        }
    }

    /* ---------- 4. завершение транзакции и SPI-сеанса ---------- */
    PopActiveSnapshot();           /* Убираем snapshot */
    SPI_finish();                  /* Завершаем SPI-сеанс */
    CommitTransactionCommand();    /* Завершаем транзакцию */
}

/*
 * scheduler_launcher_main()
 *   Главная функция фонового воркера.
 *   Подключается к базе данных, ждёт сигналы или таймауты
 *   и периодически вызывает launcher_tick() для обработки заданий.
 */
PGDLLEXPORT void
scheduler_launcher_main(Datum arg)
{
    pqsignal(SIGTERM, scheduler_sigterm);         /* Устанавливаем обработчик сигнала завершения */
    BackgroundWorkerUnblockSignals();             /* Разрешаем приём сигналов (SIGTERM и др.) */

    /* Подключаемся к базе данных с именем "postgres" */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    for (;;)
    {
        /* Ожидаем: сигнал, таймаут или завершение postmaster’а */
        int rc = WaitLatch(MyLatch,
                           WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                           1000L,                         /* Проверяем каждые 1 сек */
                           PG_WAIT_EXTENSION);

        ResetLatch(MyLatch);                /* Сбрасываем latch — мы проснулись */

        if (rc & WL_POSTMASTER_DEATH)       /* Завершение сервера → выходим */
            proc_exit(1);

        if (got_sigterm)                    /* Получен SIGTERM → выходим аккуратно */
            proc_exit(0);

        launcher_tick();                    /* Один проход: обработка просроченных job’ов */
    }
}

/*
 * scheduler.schedule()
 *   Добавляет новое задание с расписанием cron и SQL- или shell-командой.
 *   • Вычисляет время первого запуска
 *   • Вставляет строку в таблицу scheduler.jobs
 *   • Возвращает job_id
 */
PG_FUNCTION_INFO_V1(scheduler_schedule);
Datum
scheduler_schedule(PG_FUNCTION_ARGS)
{
    const char *cron   = text_to_cstring(PG_GETARG_TEXT_PP(0));             /* cron‑строка из 1-го аргумента */
    text       *cmdtxt = PG_GETARG_TEXT_PP(1);                              /* SQL или shell-команда */
    bool  is_shell     = (PG_NARGS() >= 3) ? PG_GETARG_BOOL(2) : false;     /* shell-задание? */

    TimestampTz next = parse_next_run(cron);                      /* Вычисляем первое время запуска */

    /* Подготавливаем параметры для вставки в таблицу jobs */
    Oid   argt[4]  = {TEXTOID, TEXTOID, TIMESTAMPTZOID, BOOLOID}; /* Типы аргументов */
    Datum vals[4]  = {CStringGetTextDatum(cron),                  /* cron‑строка */
                      PointerGetDatum(cmdtxt),                    /* команда */
                      TimestampTzGetDatum(next),                  /* next_run */
                      BoolGetDatum(is_shell)};                    /* shell или SQL */
    char  nulls[4] = {' ',' ',' ',' '};                           /* NULL-маска (все NOT NULL) */

    /* Открываем SPI-сеанс */
    int spi_rc = SPI_connect();
    if (spi_rc != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(spi_rc));

    /* Выполняем вставку и возвращаем job_id */
    spi_rc = SPI_execute_with_args(
        "INSERT INTO scheduler.jobs(schedule, command, next_run, is_shell) "
        "VALUES($1,$2,$3,$4) RETURNING job_id",
        4, argt, vals, nulls, false, 0);

    if (spi_rc != SPI_OK_INSERT_RETURNING)
        elog(ERROR, "scheduler_schedule: INSERT failed");

    /* Извлекаем job_id из результата */
    bool isnull;
    int job_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                             SPI_tuptable->tupdesc, 1,
                                             &isnull));

    SPI_finish();                    /* Завершаем SPI-сеанс */
    PG_RETURN_INT32(job_id);         /* Возвращаем ID задания */
}

/*
 * scheduler.schedule_once()
 *   Создаёт одноразовое задание, которое выполнится через заданный delay.
 *   • schedule = NULL → лончер воспринимает как one-shot
 *   • Возвращает job_id добавленного задания
 */
PG_FUNCTION_INFO_V1(scheduler_schedule_once);
Datum
scheduler_schedule_once(PG_FUNCTION_ARGS)
{
    /* ---------- 1. извлекаем аргументы ---------- */
    Interval *delay    = PG_GETARG_INTERVAL_P(0);                           /* Смещение во времени (через сколько запустить) */
    text     *cmd_txt  = PG_GETARG_TEXT_PP(1);                              /* Команда (SQL или shell) */
    bool      is_shell = (PG_NARGS() >= 3) ? PG_GETARG_BOOL(2) : false;     /* shell-команда? */

    /* ---------- 2. вычисляем момент запуска ---------- */
    TimestampTz next_run = DatumGetTimestampTz(             /* next_run = now() + delay */
        DirectFunctionCall2(timestamptz_pl_interval,
                            TimestampTzGetDatum(GetCurrentTimestamp()),
                            PointerGetDatum(delay)));

    /* ---------- 3. вставляем строку в таблицу jobs ---------- */
    Oid   argt[3]  = {TEXTOID, TIMESTAMPTZOID, BOOLOID};    /* Типы параметров */
    Datum vals[3]  = {PointerGetDatum(cmd_txt),             /* Команда */
                      TimestampTzGetDatum(next_run),        /* Время запуска */
                      BoolGetDatum(is_shell)};              /* shell-флаг */
    char  nulls[3] = {' ',' ',' '};                         /* NULL-маска */

    if (SPI_connect() != SPI_OK_CONNECT)                    /* Подключаем SPI */
        elog(ERROR, "scheduler_schedule_once: SPI_connect failed");

    int spi_rc = SPI_execute_with_args(                     /* Вставляем строку: schedule = NULL → one-shot */
        "INSERT INTO scheduler.jobs(schedule, command, next_run, is_shell) "
        "VALUES(NULL, $1, $2, $3) "
        "RETURNING job_id",
        3, argt, vals, nulls, false, 0);

    if (spi_rc != SPI_OK_INSERT_RETURNING)
        elog(ERROR, "scheduler_schedule_once: INSERT failed (%s)",
             SPI_result_code_string(spi_rc));

    /* ---------- 4. извлекаем и возвращаем job_id ---------- */
    bool isnull;
    int32 job_id = DatumGetInt32(
                       SPI_getbinval(SPI_tuptable->vals[0],
                                     SPI_tuptable->tupdesc, 1, &isnull));

    SPI_finish();                        /* Завершаем SPI-сеанс */
    PG_RETURN_INT32(job_id);             /* Возвращаем ID нового задания */
}

/*
 * scheduler.unschedule()
 *   Удаляет задание с указанным job_id из таблицы scheduler.jobs.
 *   Если такого задания нет — ничего не делает.
 */
PG_FUNCTION_INFO_V1(scheduler_unschedule);
Datum
scheduler_unschedule(PG_FUNCTION_ARGS)
{
    int32 job_id = PG_GETARG_INT32(0);  /* Получаем ID задания из аргумента */

    /* ---------- параметры для выполнения DELETE ---------- */
    Oid    argt[1]  = {INT4OID};                /* Тип параметра: int4 */
    Datum  vals[1]  = {Int32GetDatum(job_id)};  /* Значение: сам ID */
    char   nulls[1] = {' '};                    /* ' ' означает NOT NULL */

    if (SPI_connect() != SPI_OK_CONNECT)        /* Открываем SPI-сеанс */
        elog(ERROR, "SPI_connect failed");

    /* Выполняем удаление строки по job_id */
    if (SPI_execute_with_args(
            "DELETE FROM scheduler.jobs WHERE job_id = $1",
            1, argt, vals, nulls, false, 0) != SPI_OK_DELETE)
        elog(ERROR, "scheduler: DELETE failed");

    SPI_finish();       /* Завершаем SPI-сеанс */
    PG_RETURN_VOID();   /* Возвращаем void */
}

/*
 * scheduler.run_now()
 *   Устанавливает для задания next_run = clock_timestamp(),
 *   чтобы оно было выполнено при следующем проходе лончера.
 *   Используется для немедленного запуска вручную.
 */
PG_FUNCTION_INFO_V1(scheduler_run_now);
Datum
scheduler_run_now(PG_FUNCTION_ARGS)
{
    int32 job_id = PG_GETARG_INT32(0);  /* Получаем ID задания из аргумента */

    /* ---------- параметры для выполнения UPDATE ---------- */
    Oid    argt[1]  = {INT4OID};                /* Тип параметра */
    Datum  vals[1]  = {Int32GetDatum(job_id)};  /* Значение параметра */
    char   nulls[1] = {' '};                    /* NOT NULL */

    /* Подключаемся к SPI в рамках уже активной транзакции */
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "scheduler_run_now: SPI_connect failed");

    /* Обновляем поле next_run: задание будет выполнено немедленно */
    if (SPI_execute_with_args(
            "UPDATE scheduler.jobs "
            "SET    next_run = clock_timestamp() "
            "WHERE  job_id   = $1",
            1, argt, vals, nulls, false, 0) != SPI_OK_UPDATE)
        elog(ERROR, "scheduler_run_now: UPDATE failed");

    SPI_finish();       /* Завершаем SPI-сеанс */
    PG_RETURN_VOID();   /* Возвращаем void */
}