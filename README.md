# pg_scheduler

**pg_scheduler** — лёгкий планировщик заданий (SQL + shell) для PostgreSQL, написанный в виде расширения C.  
Работает как фон-воркер (`background worker`), поддерживает минимально-достаточный cron-синтаксис и позволяет:

* запускать SQL-команды или shell-скрипты по расписанию (`schedule`);
* ставить отложенные/одноразовые задачи (`schedule_once`);
* отменять задания (`unschedule`) и запускать их немедленно (`run_now`);
* получать историю запусков в таблице журнала.

> **Версия**: 1.0                               
> **Тестировалось** на PostgreSQL 15-19 (Linux).

---

## Возможности

| Тип задания | Синтаксис | Пример |
|-------------|-----------|--------|
| «каждые _N_ минут» | `*/N * * * *` | `*/10 * * * *` — каждые 10 мин |
| Ежедневно   | `MM HH * * *` | `55 23 * * *` — 23:55 каждый день |
| Еженедельно | `MM HH * * D` (D = 0–6, 0 — вс) | `30 13 * * 3` — 13:30 по средам |
| Отложенное  | `schedule_once(interval, …)` | `interval '2 hours'` |

Все строки cron интерпретируются в часовом поясе, заданном параметром `TimeZone`.

---

## Установка

```bash
# 1. Склонировать и собрать расширение
git clone https://github.com/Tychaaa/pg_scheduler.git
cd pg_scheduler
make
sudo make install              # копирует scheduler.so и sql-скрипт

# 2. В postgresql.conf
shared_preload_libraries = 'scheduler'   # перезапуск кластера обязателен
# (необязательные GUC):
#   scheduler.tick_ms   = 60000   # интервал пробуждения (мс)
#   scheduler.log_level = LOG     # LOG / INFO / DEBUG1 …

# 3. Перезапуск сервера
pg_ctl restart -m fast          # или systemctl restart postgresql

# 4. Создать расширение в нужной БД
psql -d postgres -c "CREATE EXTENSION scheduler;"
