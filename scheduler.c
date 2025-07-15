#include "postgres.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"

#include "pgtime.h"

#include "utils/wait_event.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

#ifndef WAIT_EVENT_EXTENSION
#define WAIT_EVENT_EXTENSION 0
#endif

#include <ctype.h>
#include <signal.h>

#define UNIX_EPOCH_IN_POSTGRES 946684800  /* сдвиг между Unix‑ и Postgres‑эпохами в секундах */

PG_MODULE_MAGIC;

/* ---------- configuration (GUCs) ---------- */
static int  scheduler_tick_ms   = 60000;  /* launcher wake‑up interval */
static int  scheduler_log_level = LOG;    /* LOG, INFO, DEBUG1, …        */

/* ---------- signals ---------- */
static volatile sig_atomic_t got_sigterm = false;

static void
scheduler_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    if (MyLatch)
        SetLatch(MyLatch);
    errno = save_errno;
}

/*
 * parse_next_run() — вернуть ближайший момент времени согласно cron-строке
 *  Поддерживает:
 *    • "\N * * * *"     — каждые N минут
 *    • "MM HH * * *"     — ежедневно в HH:MM
 *    • "MM HH * * D"     — еженедельно в HH:MM
 */
static TimestampTz
parse_next_run(const char *cron)
{
    /* ---------- текущее время (локальная календарная дата) ---------- */
    TimestampTz now_ts = GetCurrentTimestamp();

    pg_time_t unix_sec = (pg_time_t)(now_ts / USECS_PER_SEC)
                        + UNIX_EPOCH_IN_POSTGRES;

    struct tm *lt = pg_localtime(&unix_sec, session_timezone);
    if (!lt)
        ereport(ERROR,
                (errmsg("scheduler: pg_localtime() failed")));

    struct pg_tm now_tm;
    now_tm.tm_year = lt->tm_year + 1900;  /* lt->tm_year = год‑1900 */
    now_tm.tm_mon  = lt->tm_mon  + 1;     /* lt->tm_mon  = 0‑based */
    now_tm.tm_mday = lt->tm_mday;
    now_tm.tm_hour = lt->tm_hour;
    now_tm.tm_min  = lt->tm_min;
    now_tm.tm_sec  = lt->tm_sec;
    now_tm.tm_wday = lt->tm_wday;         /* 0=Sun … 6=Sat, как в cron */

    /* ---------- разбор cron‑строки ---------- */
    char  *copy = pstrdup(cron);
    char  *fld[5];
    int    n    = 0;

    for (char *tok = strtok(copy, " \t"); tok && n < 5;
         tok = strtok(NULL, " \t"))
        fld[n++] = tok;

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
        int N = pg_strtoint32(fld[0] + 2);
        if (N <= 0 || N > 59)
            ereport(ERROR,
                    (errmsg("scheduler: bad minute step \"%s\"", fld[0])));

        return now_ts + (int64) N * USECS_PER_MINUTE;
    }

    /* ---------- общий разбор часов и минут ---------- */
    int minute = pg_strtoint32(fld[0]);
    int hour   = pg_strtoint32(fld[1]);
    if (minute < 0 || minute > 59 || hour < 0 || hour > 23)
        ereport(ERROR,
                (errmsg("scheduler: bad hour/minute in \"%s\"", cron)));

    /* сегодняшнее HH:MM */
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
        if (cand <= now_ts)                 /* время уже прошло сегодня */
            cand += USECS_PER_DAY;
        return cand;
    }

    /* ---------- 3. еженедельно ---------- */
    if (strcmp(fld[2], "*") == 0 && strcmp(fld[3], "*") == 0)
    {
        int dow = pg_strtoint32(fld[4]);    /* 0=Sun … 6=Sat */
        if (dow < 0 || dow > 6)
            ereport(ERROR,
                    (errmsg("scheduler: bad day‑of‑week in \"%s\"", cron)));

        int diff = (dow - now_tm.tm_wday + 7) % 7;
        if (diff == 0 && cand <= now_ts)    /* уже прошли — на следующей неделе */
            diff = 7;

        cand += (int64) diff * USECS_PER_DAY;
        return cand;
    }

    ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("scheduler: unsupported cron format \"%s\"", cron)));
    return 0;
}

PGDLLEXPORT void scheduler_launcher_main(Datum arg);

/* ---------- _PG_init ---------- */
void
_PG_init(void)
{
    /* GUCs */
    DefineCustomIntVariable("scheduler.tick_ms",
                            "Launcher check interval in ms.",
                            NULL,
                            &scheduler_tick_ms,
                            60000, 1000, 600000,
                            PGC_POSTMASTER, 0,
                            NULL, NULL, NULL);

    DefineCustomIntVariable("scheduler.log_level",
                            "Log level for job results.",
                            NULL,
                            &scheduler_log_level,
                            LOG, DEBUG1, ERROR,
                            PGC_SIGHUP, 0,
                            NULL, NULL, NULL);

    BackgroundWorker worker;
    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags       = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time  = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 10;
    snprintf(worker.bgw_name, BGW_MAXLEN, "scheduler launcher");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "scheduler");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "scheduler_launcher_main");
    worker.bgw_main_arg    = (Datum) 0;
    worker.bgw_notify_pid  = 0;

    RegisterBackgroundWorker(&worker);
}

static void
execute_job(int32 job_id, const char *command)
{
    bool   success = false;
    TimestampTz started = GetCurrentTimestamp();

    StartTransactionCommand();
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "scheduler: SPI_connect failed");

    PG_TRY();
    {
        if (SPI_execute(command, false, 0) < 0)
            elog(ERROR, "scheduler: SPI_execute failed");
        success = true;
        SPI_finish();
        CommitTransactionCommand();
    }
    PG_CATCH();
    {
        AbortCurrentTransaction();
        success = false;
        FlushErrorState();
    }
    PG_END_TRY();

    StartTransactionCommand();
    if (SPI_connect() == SPI_OK_CONNECT)
    {
        const char *sql = "INSERT INTO scheduler.job_run_log(job_id, success, message, finished_at)"
                          " VALUES ($1,$2,$3,clock_timestamp())";
        Oid   argt[3] = {INT4OID, BOOLOID, TEXTOID};
        Datum vals[3];
        char  nulls[3] = {' ',' ',' '};
        vals[0] = Int32GetDatum(job_id);
        vals[1] = BoolGetDatum(success);
        vals[2] = CStringGetTextDatum(success ? "OK" : "FAILED");
        SPI_execute_with_args(sql, 3, argt, vals, nulls, false, 0);
        SPI_finish();
    }
    CommitTransactionCommand();

    ereport(scheduler_log_level,
            (errmsg("scheduler: job %d %s", job_id, success ? "completed" : "failed")));
}

/* ----------------------------------------------------------------------
 *  launcher_tick() – один проход: выбрать просроченные job'ы и выполнить
 * --------------------------------------------------------------------*/
static void
launcher_tick(void)
{
    StartTransactionCommand();

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "scheduler: SPI_connect failed");

    PushActiveSnapshot(GetTransactionSnapshot());

    /* выбрать все job’ы, которые пора запускать */
    const char *sql_sel =
        "SELECT job_id, command, schedule "
        "FROM   scheduler.jobs "
        "WHERE  next_run <= clock_timestamp() "
        "FOR UPDATE SKIP LOCKED";

    if (SPI_execute(sql_sel, false, 0) != SPI_OK_SELECT)
        elog(ERROR, "scheduler: SELECT failed");

    for (uint64 i = 0; i < SPI_processed; i++)
    {
        HeapTuple tup  = SPI_tuptable->vals[i];
        TupleDesc desc = SPI_tuptable->tupdesc;
        bool      isnull;

        int32 job_id   = DatumGetInt32(SPI_getbinval(tup, desc, 1, &isnull));
        char *command  = TextDatumGetCString(SPI_getbinval(tup, desc, 2, &isnull));

        /* schedule (поле 3) — может быть NULL для one-shot */
        Datum sch_datum = SPI_getbinval(tup, desc, 3, &isnull);
        char *cron      = isnull ? NULL : TextDatumGetCString(sch_datum);
        bool  oneshot   = isnull;

        /* --- выполняем job в под-транзакции ------------------------ */
        PG_TRY();
        {
            BeginInternalSubTransaction(NULL);
            SPI_execute(command, false, 0);      /* выполняем SQL-команду */
            ReleaseCurrentSubTransaction();
        }
        PG_CATCH();
        {
            RollbackAndReleaseCurrentSubTransaction();
            FlushErrorState();                   /* фиксируем failure, но идём дальше */
        }
        PG_END_TRY();

        /* --- post-processing --------------------------------------- */
        if (oneshot)
        {
            /* одноразовое: удаляем запись */
            Oid   at[1] = {INT4OID};
            Datum vl[1] = {Int32GetDatum(job_id)};
            char  nl[1] = {' '};

            SPI_execute_with_args(
                "DELETE FROM scheduler.jobs WHERE job_id = $1",
                1, at, vl, nl, false, 0);
        }
        else
        {
            /* повторное: пересчитываем next_run */
            TimestampTz next = parse_next_run(cron);

            Oid   at[2] = {INT4OID, TIMESTAMPTZOID};
            Datum vl[2] = {Int32GetDatum(job_id),
                           TimestampTzGetDatum(next)};
            char  nl[2] = {' ',' '};

            SPI_execute_with_args(
                "UPDATE scheduler.jobs "
                "SET    last_run = clock_timestamp(), "
                "       next_run = $2 "
                "WHERE  job_id   = $1",
                2, at, vl, nl, false, 0);
        }
    }

    PopActiveSnapshot();
    SPI_finish();
    CommitTransactionCommand();
}

/* ---------- launcher loop ---------- */
PGDLLEXPORT void
scheduler_launcher_main(Datum arg)
{
    pqsignal(SIGTERM, scheduler_sigterm);
    BackgroundWorkerUnblockSignals();

    /* подключаемся к нужной БД */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    for (;;)
    {
        int rc = WaitLatch(MyLatch,
                           WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                           1000L,                         /* 1 сек */
                           PG_WAIT_EXTENSION);

        ResetLatch(MyLatch);
        if (rc & WL_POSTMASTER_DEATH)
            proc_exit(1);

        if (got_sigterm)
            proc_exit(0);

        launcher_tick();
    }
}

/* ----------------------------------------------------------------------
 *  ФУНКЦИЯ  scheduler.schedule(cron text, cmd text) RETURNS int
 * --------------------------------------------------------------------*/
PG_FUNCTION_INFO_V1(scheduler_schedule);
Datum
scheduler_schedule(PG_FUNCTION_ARGS)
{
    text *cron_txt = PG_GETARG_TEXT_PP(0);
    text *cmd_txt  = PG_GETARG_TEXT_PP(1);

    char *cron = text_to_cstring(cron_txt);
    char *cmd  = text_to_cstring(cmd_txt);

    TimestampTz next = parse_next_run(cron);

    Oid   argt[3]   = {TEXTOID, TEXTOID, TIMESTAMPTZOID};
    Datum vals[3]   = {CStringGetTextDatum(cron),
                       CStringGetTextDatum(cmd),
                       TimestampTzGetDatum(next)};
    bool  nulls[3]  = {false,false,false};

    int   spi_rc;
    int32 job_id   = -1;

    /* открываем SPI-сессию внутри УЖЕ начатой транзакции */
    if ((spi_rc = SPI_connect()) != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(spi_rc));

    spi_rc = SPI_execute_with_args(
        "INSERT INTO scheduler.jobs(schedule, command, next_run)"
        "VALUES($1,$2,$3) RETURNING job_id",
        3, argt, vals, nulls, false, 0);

    if (spi_rc != SPI_OK_INSERT_RETURNING)
        elog(ERROR, "unable to insert job: %s", SPI_result_code_string(spi_rc));

    bool isnull;
    job_id = DatumGetInt32(
                 SPI_getbinval(SPI_tuptable->vals[0],
                               SPI_tuptable->tupdesc, 1, &isnull));

    SPI_finish();

    PG_RETURN_INT32(job_id);
}

/* ----------------------------------------------------------------------
 *  ФУНКЦИЯ  scheduler.schedule_once(delay interval, cmd text) RETURNS int
 *  ─ вставляет строку с  next_run = clock_timestamp() + delay
 *    schedule  == NULL  →  лончер поймёт, что это one-shot-job
 * --------------------------------------------------------------------*/
PG_FUNCTION_INFO_V1(scheduler_schedule_once);
Datum
scheduler_schedule_once(PG_FUNCTION_ARGS)
{
    Interval *delay    = PG_GETARG_INTERVAL_P(0);
    text     *cmd_txt  = PG_GETARG_TEXT_PP(1);

    /* вычисляем целевой момент: now() + delay */
    TimestampTz next_run =
        DatumGetTimestampTz(
            DirectFunctionCall2(timestamptz_pl_interval,
                                TimestampTzGetDatum(GetCurrentTimestamp()),
                                PG_GETARG_DATUM(0)));

    Oid   argt[2]  = {TEXTOID, TIMESTAMPTZOID};
    Datum vals[2]  = {PointerGetDatum(cmd_txt),
                      TimestampTzGetDatum(next_run)};
    bool  nulls[2] = {false,false};

    int   spi_rc, job_id = -1;

    if ((spi_rc = SPI_connect()) != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %s", SPI_result_code_string(spi_rc));

    spi_rc = SPI_execute_with_args(
        "INSERT INTO scheduler.jobs(schedule, command, next_run) "
        "VALUES(NULL, $1, $2) RETURNING job_id",
        2, argt, vals, nulls, false, 0);

    if (spi_rc != SPI_OK_INSERT_RETURNING)
        elog(ERROR, "unable to insert one-shot job: %s",
             SPI_result_code_string(spi_rc));

    bool isnull;
    job_id = DatumGetInt32(
                 SPI_getbinval(SPI_tuptable->vals[0],
                               SPI_tuptable->tupdesc, 1, &isnull));

    SPI_finish();
    PG_RETURN_INT32(job_id);
}

PG_FUNCTION_INFO_V1(scheduler_unschedule);
Datum
scheduler_unschedule(PG_FUNCTION_ARGS)
{
    int32 job_id = PG_GETARG_INT32(0);

    /* параметры для DELETE */
    Oid    argt[1]  = {INT4OID};
    Datum  vals[1]  = {Int32GetDatum(job_id)};
    char   nulls[1] = {' '};          /* ' ' = NOT NULL */

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    if (SPI_execute_with_args(
            "DELETE FROM scheduler.jobs WHERE job_id = $1",
            1, argt, vals, nulls, false, 0) != SPI_OK_DELETE)
        elog(ERROR, "scheduler: DELETE failed");

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(scheduler_run_now);
Datum
scheduler_run_now(PG_FUNCTION_ARGS)
{
    int32 job_id = PG_GETARG_INT32(0);
    StartTransactionCommand();
    SPI_connect();
    const char *sql = "UPDATE scheduler.jobs SET next_run = clock_timestamp() WHERE job_id = $1";
    Oid argt[1] = {INT4OID};
    Datum vals[1] = {Int32GetDatum(job_id)};
    char nulls[1] = {' '};
    SPI_execute_with_args(sql, 1, argt, vals, nulls, false, 0);
    SPI_finish();
    CommitTransactionCommand();
    PG_RETURN_VOID();
}