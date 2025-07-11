/* scheduler.c */

#include "postgres.h"               // Основной заголовок PostgreSQL
#include "fmgr.h"                   // Для создания функций расширения

#include "postmaster/bgworker.h"    // Поддержка фоновых воркеров
#include "storage/ipc.h"            // Взаимодействие между процессами
#include "storage/latch.h"          // Работа с latch
#include "utils/timestamp.h"        // Работа с временными метками
#include "executor/spi.h"           // Интерфейс Server Programming Interface (для SQL внутри C)
#include "tcop/tcopprot.h"          // Протокол обработки команд
#include "catalog/pg_type_d.h"      // Определения типов PostgreSQL

PG_MODULE_MAGIC;                    // Необходимая макрос-заглушка для модулей

#define SCHED_TICK_MS 1000          // Интервал "тика" — 1 секунда

PGDLLEXPORT void scheduler_main(Datum); // Точка входа фонового воркера

/* Регистрация фонового воркера при инициализации расширения */
void
_PG_init(void)
{
    BackgroundWorker worker = {0};  // Инициализация структуры воркера нулями

    // Устанавливаем флаги доступа к shared memory и к базе данных
    worker.bgw_flags =
          BGWORKER_SHMEM_ACCESS
        | BGWORKER_BACKEND_DATABASE_CONNECTION;

    // Запускаем воркер после завершения восстановления
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    // Автоматически перезапускать воркер через 5 секунд после сбоя
    worker.bgw_restart_time = 5;

    // Устанавливаем имена воркера, библиотеки и функции запуска
    snprintf(worker.bgw_name,          BGW_MAXLEN, "scheduler bgworker");
    snprintf(worker.bgw_library_name,  BGW_MAXLEN, "scheduler");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "scheduler_main");

    // Регистрируем воркер
    RegisterBackgroundWorker(&worker);
}

/* Основной цикл фонового воркера */
PGDLLEXPORT void
scheduler_main(Datum main_arg)
{
    // Разрешаем обработку сигналов
    BackgroundWorkerUnblockSignals();
    // Инициализируем соединение с базой данных "postgres"
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    elog(LOG, "scheduler bgworker started"); // Лог: воркер запущен

    for (;;)
    {
        // Начинаем SQL-транзакцию
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        // Лог: текущий момент времени (тик)
        elog(DEBUG1, "tick %s", timestamptz_to_str(GetCurrentTimestamp()));

        // Подключаемся к SPI (выполнение SQL внутри C)
        if (SPI_connect() == SPI_OK_CONNECT)
        {
            // SQL-запрос для получения задач, которые пора выполнять
            const char *fetch =
                "SELECT job_id, command_type, command, schedule_intv "
                "FROM scheduler.jobs "
                "WHERE enabled AND next_run <= now() "
                "FOR UPDATE SKIP LOCKED";

            // Выполняем запрос
            if (SPI_execute(fetch, false, 0) == SPI_OK_SELECT &&
                SPI_processed > 0)
            {
                TupleDesc desc = SPI_tuptable->tupdesc; // Описание результата

                // Обрабатываем каждую задачу
                for (uint64 i = 0; i < SPI_processed; i++)
                {
                    HeapTuple tup   = SPI_tuptable->vals[i]; // Текущая строка
                    bool      isnull;

                    // Получаем ID задачи
                    int64 job_id = DatumGetInt64(
                        SPI_getbinval(tup, desc, 1, &isnull));

                    // Получаем тип команды (sql / shell) и саму команду
                    char *cmdtype = SPI_getvalue(tup, desc, 2);
                    char *command = SPI_getvalue(tup, desc, 3);

                    // Получаем интервал выполнения (если есть)
                    Datum     intv_d = SPI_getbinval(tup, desc, 4, &isnull);
                    Interval *intv   = isnull ? NULL : DatumGetIntervalP(intv_d);

                    // Выполняем команду в зависимости от типа
                    if (strcmp(cmdtype, "sql") == 0)
                    {
                        elog(DEBUG1, "SQL job %ld: %s", job_id, command);
                        SPI_execute(command, false, 0); // Выполнение SQL
                    }
                    else
                    {
                        elog(DEBUG1, "shell job %ld: %s", job_id, command);
                        system(command); // Выполнение shell-команды
                    }

                    // Обновляем расписание следующего запуска
                    if (intv)  // Если задача периодическая
                    {
                        Oid   at[2] = { INTERVALOID, INT8OID };         // Типы аргументов
                        Datum av[2] = { PointerGetDatum(intv),          // Интервал
                                        Int64GetDatum(job_id) };        // ID задачи

                        SPI_execute_with_args(
                            "UPDATE scheduler.jobs "
                            "SET next_run = now() + $1 "
                            "WHERE job_id = $2",
                            2, at, av, (const char[]){false,false},
                            false, 0);
                    }
                    else       // Если задача одноразовая — отключаем
                    {
                        Oid   at[1] = { INT8OID };
                        Datum av[1] = { Int64GetDatum(job_id) };

                        SPI_execute_with_args(
                            "UPDATE scheduler.jobs "
                            "SET enabled = false "
                            "WHERE job_id = $1",
                            1, at, av, (const char[]){false},
                            false, 0);
                    }
                }
            }
            SPI_finish(); // Заканчиваем работу с SPI
        }

        // Завершаем SQL-транзакцию
        PopActiveSnapshot();
        CommitTransactionCommand();

        // Ожидаем следующего "тика"
        WaitLatch(&MyProc->procLatch,
                  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                  SCHED_TICK_MS,
                  false);
        ResetLatch(&MyProc->procLatch); // Сбрасываем флаг ожидания
    }
}
