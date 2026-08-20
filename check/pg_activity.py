"""### 🩺 Сторож метабазы: зависшие сессии, долгие запросы, блокировки
*2026-08-21 12:30 MSK · v1.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Каждые 10 минут снимает `pg_stat_activity` метабазы Airflow и разбирает находки по трём
категориям: **зависшие сессии** (`idle in transaction`), **долгие запросы** (`active`) и
**блокировки** (`pg_blocking_pids()` не пуст). Против каждой находки подставляется таск,
которому она принадлежит — по pid из `application_name` (`app-dataplatform-etl-worker_<pid>`)
и колонке `task_instance.pid`.

| Параметр | Описание |
|---|---|
| `idle_tx_sec` | Порог для `idle in transaction`, сек *(default: `300`)* |
| `long_query_sec` | Порог для активного запроса, сек *(default: `300`)* |
| `lock_wait_sec` | Порог ожидания блокировки, сек *(default: `60`)* |
| `alert` | Краснеть при находках *(default: `True`)* |
| `save_s3` | Писать снимки в S3 *(default: `True`)* |
| `keep_days` | Сколько дней держим снимки, старше — удаляем *(default: `30`)* |
| `dry_run` | При `terminate=True` только показать кандидатов *(default: `True`)* |
| `save_to_var` | Записать значения формы в Variable `tools_pg_activity_cfg` *(default: `False`)* |
| `terminate` | Убивать найденные сессии. **В Variable не сохраняется** *(default: `False`)* |

**Таски:** `collect` → `save` / `terminate` → `report`, рядом `prune` — чистка снимков.

Снимки лежат в бакете логов, в своей папке: `pg_activity/<YYYY-MM-DD>/<HHMMSS>.json`.
Пустые снимки не пишутся — счётчики и так уходят в лог каждый запуск. Старые снимки
даг убирает сам: наш S3-шлюз не принимает lifecycle-правило (`PutBucketLifecycleConfiguration`
требует заголовок `Content-MD5`, которого boto3 больше не шлёт).

> Пороги берутся из Variable `tools_pg_activity_cfg`, форма запуска ими предзаполняется.
> Поменять порог для расписания — запуск с галкой «Сохранить настройки».
"""

from datetime import datetime, timedelta, timezone
import json
import logging

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.utils.trigger_rule import TriggerRule

try:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
except ImportError:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore

logger = logging.getLogger("airflow.task")

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Бакет и коннект — те же, что у логов задач (см. check/log_cleanup.py), но папка своя:
# снимки не должны попасть под чистку логов и не должны мешаться с ними в выдаче.
AWS_CONN_ID = conf.get("logging", "REMOTE_LOG_CONN_ID")
BUCKET_NAME = conf.get("logging", "REMOTE_BASE_LOG_FOLDER").split("//")[-1].split("/")[0]
PREFIX = "pg_activity/"

CFG_VAR = "tools_pg_activity_cfg"

# Значения по умолчанию. Всё, кроме terminate: убийство сессий не должно становиться
# настройкой, живущей между запусками, — галку ставят руками на конкретный ран.
DEFAULTS = {
    "idle_tx_sec": 300,
    "long_query_sec": 300,
    "lock_wait_sec": 60,
    "alert": True,
    "save_s3": True,
    "keep_days": 30,
    "dry_run": True,
}

# default_var={} обязателен: без него отсутствующая переменная роняет разбор файла
# и вешает Broken DAG на весь даг, а не на один запуск.
try:
    _cfg = {**DEFAULTS, **(Variable.get(CFG_VAR, default_var={}, deserialize_json=True) or {})}
except Exception:  # битый JSON в переменной не должен ломать разбор
    logger.warning("Variable %s не разобрана, берём значения по умолчанию", CFG_VAR, exc_info=True)
    _cfg = dict(DEFAULTS)


SQL_ACTIVITY = r"""
SELECT a.pid,
       a.usename,
       a.application_name,
       host(a.client_addr)                                  AS client_addr,
       a.client_port,
       a.state,
       a.wait_event_type,
       a.wait_event,
       round(extract(epoch FROM now() - a.backend_start))   AS backend_age,
       round(extract(epoch FROM now() - a.xact_start))      AS tx_age,
       round(extract(epoch FROM now() - a.query_start))     AS query_age,
       round(extract(epoch FROM now() - a.state_change))    AS state_age,
       pg_blocking_pids(a.pid)                              AS blocking_pids,
       left(regexp_replace(a.query, '\s+', ' ', 'g'), 2000) AS query
  FROM pg_stat_activity a
 WHERE a.datname = current_database()
   AND a.backend_type = 'client backend'
   AND a.pid <> pg_backend_pid()
 ORDER BY a.xact_start
"""

# Таски, которые сейчас идут: по ним опознаётся владелец сессии. pid — это pid процесса
# задачи на поде, он же уезжает в application_name соединения (db_utils.failover_connect).
SQL_RUNNING = r"""
SELECT dag_id, task_id, run_id, map_index, hostname, pid,
       round(extract(epoch FROM now() - start_date)) AS age
  FROM main.task_instance
 WHERE state = 'running' AND pid IS NOT NULL
"""


def _age(row: dict, key: str) -> int:
    """Возраст в секундах: NULL (транзакции/запроса нет) считаем нулём."""
    value = row.get(key)
    return int(value) if value is not None else 0


def _app_pid(application_name: str):
    """pid из application_name вида 'app-dataplatform-etl-worker_958298'.

    До 1.1.23 имя вырождалось в 'airflow_<pid>' — разбор одинаков, хвост после
    последнего '_' и есть pid. Не разобралось — возвращаем None, сопоставления не будет.
    """
    tail = (application_name or "").rsplit("_", 1)[-1]
    return int(tail) if tail.isdigit() else None


def _delete_keys(hook, keys: list) -> int:
    """Удаляет ключи пачкой, а если шлюз пачку не принял — по одному.

    Multi-object delete (POST ?delete) — из того же семейства запросов, что и
    lifecycle: он требует Content-MD5, а boto3 вместо него давно шлёт контрольную
    сумму, и шлюз может ответить отказом. У одиночного DELETE тела нет, спорить
    не о чем — на нём и подстраховываемся.
    """
    try:
        hook.delete_objects(bucket=BUCKET_NAME, keys=keys)
        return len(keys)
    except Exception as err:
        logger.warning("удалить пачкой не вышло (%s), идём по одному", err)

    conn = hook.get_conn()
    deleted = 0
    for key in keys:
        try:
            conn.delete_object(Bucket=BUCKET_NAME, Key=key)
            deleted += 1
        except Exception as err:
            logger.warning("не удалён %s: %s", key, err)
    return deleted


def _fetch(sql: str) -> list:
    """Читает SQL в метабазе и отдаёт список словарей.

    create_session как контекст, а не своя долгоживущая сессия: сторож не должен
    попадать в собственный отчёт.
    """
    from airflow.utils.session import create_session
    from sqlalchemy import text

    with create_session() as session:
        session.execute(text("SET LOCAL statement_timeout = '30s'"))
        result = session.execute(text(sql))
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        # Потолок на таск, а не на весь прогон: подвисший запрос к метабазе умрёт сам
        # и краснотой скажет об этом, а не съест бюджет прогона молча.
        'execution_timeout': timedelta(minutes=5),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 8, 20, tzinfo=timezone.utc),
    schedule_interval='*/10 * * * *',
    # Тег tools важен: по нему ролевка ограничивает запуск (HRPDATALAB-15421)
    tags=['DataLab', 'tools', 'check'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    # Тридцать минут, а не девять. Наложиться на следующий прогон не даёт
    # max_active_runs=1, а dagrun_timeout нужен только чтобы расклинить зависший
    # прогон. При девяти минутах он срабатывал на обычной очереди: тасков в цепочке
    # четыре, каждый ждёт своей очереди в celery, и прогон не укладывался. А по
    # таймауту шедулер помечает прогон failed и все недоделанные таски skipped
    # (scheduler_job_runner.py:1667) — отсюда и был вечный «report пропущен,
    # даг красный» при том, что находки собирались нормально.
    dagrun_timeout=timedelta(minutes=30),
    on_failure_callback=on_callback,
    params={
        'idle_tx_sec': Param(_cfg['idle_tx_sec'], type='integer', minimum=10,
                             description='Порог для idle in transaction, сек'),
        'long_query_sec': Param(_cfg['long_query_sec'], type='integer', minimum=10,
                                description='Порог для активного запроса, сек'),
        'lock_wait_sec': Param(_cfg['lock_wait_sec'], type='integer', minimum=5,
                               description='Порог ожидания блокировки, сек'),
        'alert': Param(_cfg['alert'], type='boolean',
                       description='Краснеть при находках'),
        'save_s3': Param(_cfg['save_s3'], type='boolean',
                         description='Писать снимки в S3'),
        'keep_days': Param(_cfg['keep_days'], type='integer', minimum=1,
                           description='Сколько дней держим снимки в S3, старше — удаляем'),
        'dry_run': Param(_cfg['dry_run'], type='boolean',
                         description='При «Убивать сессии» — только показать кандидатов'),
        'save_to_var': Param(False, type='boolean',
                             description=f'Сохранить настройки формы в Variable {CFG_VAR}'),
        'terminate': Param(False, type='boolean',
                           description='Убивать найденные сессии (в Variable не сохраняется)'),
    },
)
def tools_pg_activity():

    @task(task_id='collect')
    def collect(**context) -> dict:
        """📸 Снимок pg_stat_activity, разбор находок и опознание владельцев."""
        p = context['params']

        if p.get('save_to_var'):
            saved = {k: p[k] for k in DEFAULTS}
            Variable.set(CFG_VAR, saved, serialize_json=True)
            logger.info("💾 Настройки сохранены в Variable %s: %s", CFG_VAR, saved)
            add_note(f"💾 настройки сохранены в `{CFG_VAR}`: {saved}", level='task', context=context)

        rows = _fetch(SQL_ACTIVITY)
        running = _fetch(SQL_RUNNING)
        by_pid = {}
        for ti in running:
            by_pid.setdefault(ti['pid'], []).append(ti)

        # Кто кого блокирует: строим по blocking_pids, чтобы в отчёте виновник стоял рядом
        # с пострадавшим, а не искался глазами по списку.
        blockers = {b for r in rows for b in (r.get('blocking_pids') or [])}

        findings = []
        for r in rows:
            kinds = []
            state = r.get('state') or ''
            if state.startswith('idle in transaction') and _age(r, 'state_age') >= p['idle_tx_sec']:
                kinds.append('idle_tx')
            if state == 'active' and _age(r, 'query_age') >= p['long_query_sec']:
                kinds.append('long_query')
            if r.get('blocking_pids') and _age(r, 'state_age') >= p['lock_wait_sec']:
                kinds.append('blocked')
            if r['pid'] in blockers:
                kinds.append('blocker')
            if not kinds:
                continue

            owners = by_pid.get(_app_pid(r.get('application_name')), [])
            findings.append({
                **{k: (str(v) if isinstance(v, datetime) else v) for k, v in r.items()},
                # Возрасты приходят Decimal'ами: в JSON снимка они превратились бы
                # в строки, а в отчёте — в '32' вместо 32
                **{k: _age(r, k) for k in ('backend_age', 'tx_age', 'query_age', 'state_age')},
                'kinds': kinds,
                # Кандидатов может быть несколько: pid уникален внутри пода, не по кластеру
                'owners': [f"{o['dag_id']}.{o['task_id']} [{o['run_id']}]" for o in owners],
            })

        counts = {kind: sum(kind in f['kinds'] for f in findings)
                  for kind in ('idle_tx', 'long_query', 'blocked', 'blocker')}
        logger.info("🔎 сессий всего: %d, тасков в работе: %d, находок: %d %s",
                    len(rows), len(running), len(findings), counts)
        for f in findings:
            logger.warning(
                "⚠️ pid=%s %s state=%s tx=%ss query=%ss state=%ss app=%s addr=%s owners=%s\n    %s",
                f['pid'], ','.join(f['kinds']), f['state'], f['tx_age'], f['query_age'],
                f['state_age'], f['application_name'], f['client_addr'], f['owners'] or '—',
                f['query'],
            )

        return {
            'ts': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'thresholds': {k: p[k] for k in ('idle_tx_sec', 'long_query_sec', 'lock_wait_sec')},
            'totals': {'sessions': len(rows), 'running_tasks': len(running)},
            'counts': counts,
            'findings': findings,
        }

    @task(task_id='save')
    def save(snapshot: dict, **context) -> str:
        """💾 Кладёт снимок в S3. Пустые снимки не пишем — их 144 в сутки и ноль пользы."""
        from airflow.exceptions import AirflowSkipException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        if not context['params']['save_s3']:
            raise AirflowSkipException("save_s3=False — снимок не сохраняем")
        if not snapshot['findings']:
            raise AirflowSkipException("находок нет — сохранять нечего")

        now = datetime.now(timezone.utc)
        key = f"{PREFIX}{now:%Y-%m-%d}/{now:%H%M%S}.json"
        S3Hook(aws_conn_id=AWS_CONN_ID, verify=False).load_string(
            json.dumps(snapshot, ensure_ascii=False, indent=2, default=str),
            key=key, bucket_name=BUCKET_NAME, replace=True,
        )
        logger.info("💾 снимок сохранён: s3://%s/%s", BUCKET_NAME, key)
        return key

    @task(task_id='terminate', trigger_rule=TriggerRule.NONE_FAILED)
    def terminate(snapshot: dict, **context) -> list:
        """🔪 Гасит найденные сессии. По умолчанию выключено, и даже включённое — dry-run."""
        from airflow.exceptions import AirflowSkipException
        from airflow.utils.session import create_session
        from sqlalchemy import text

        p = context['params']
        if not p['terminate']:
            raise AirflowSkipException("terminate=False — сессии не трогаем")

        # Себя не трогаем ни при каких настройках: таск, который убьёт собственную сессию,
        # упадёт на записи своего же статуса.
        me = context['dag'].dag_id
        victims = [f for f in snapshot['findings']
                   if ('idle_tx' in f['kinds'] or 'long_query' in f['kinds'])
                   and not any(o.startswith(f"{me}.") for o in f['owners'])]
        if not victims:
            raise AirflowSkipException("подходящих кандидатов нет")

        killed = []
        for f in victims:
            # idle in transaction отменять нечего — там нет запроса, только транзакция,
            # поэтому терминируем. Активный запрос сначала пробуем отменить: таск получит
            # ошибку запроса, а не обрыв соединения.
            fn = 'pg_cancel_backend' if 'long_query' in f['kinds'] else 'pg_terminate_backend'
            line = f"{fn}({f['pid']}) — {','.join(f['kinds'])}, {f['owners'] or '—'}"
            if p['dry_run']:
                logger.info("🧪 dry-run: %s", line)
                killed.append(f"[dry-run] {line}")
                continue
            with create_session() as session:
                ok = session.execute(text(f"SELECT {fn}(:pid)"), {'pid': f['pid']}).scalar()
            logger.warning("🔪 %s → %s", line, ok)
            killed.append(f"{line} → {ok}")

        add_note({'🔪 terminate': killed}, level='task', context=context)
        return killed

    @task(task_id='prune', trigger_rule=TriggerRule.NONE_FAILED)
    def prune(**context) -> str:
        """🧹 Убирает снимки старше `keep_days`.

        Сначала здесь стояло lifecycle-правило через `s3_set_ttl` — пусть чистит
        хранилище. Не вышло: шлюз отвечает на PutBucketLifecycleConfiguration
        "Missing required header for this request: Content-MD5", а boto3 этот
        заголовок давно не шлёт, он заменён на контрольные суммы. Поэтому чистим
        сами, благо ключ снимка начинается с даты и ходить за метаданными не нужно.
        """
        from airflow.exceptions import AirflowSkipException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        if not context['params']['save_s3']:
            raise AirflowSkipException("save_s3=False — папку не трогаем")

        days = context['params']['keep_days']
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date()

        hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
        keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=PREFIX) or []

        stale, unknown = [], 0
        for key in keys:
            day = key[len(PREFIX):].split('/')[0]
            try:
                if datetime.strptime(day, '%Y-%m-%d').date() < cutoff:
                    stale.append(key)
            except ValueError:
                # Не наш формат — руками положили или писал кто-то ещё. Чужое не удаляем.
                unknown += 1

        if not stale:
            msg = f"снимков старше {days}д нет, всего в папке {len(keys)}"
            logger.info("🧹 %s", msg)
            return msg

        deleted = _delete_keys(hook, stale)
        msg = f"удалено {deleted} из {len(stale)} снимков старше {days}д (всего было {len(keys)})"
        if unknown:
            msg += f", пропущено чужих ключей: {unknown}"
        logger.info("🧹 %s", msg)
        add_note({'🧹 prune': msg}, level='task', context=context)
        return msg

    @task(task_id='report', trigger_rule=TriggerRule.NONE_FAILED)
    def report(snapshot: dict, **context) -> str:
        """🧾 Сводка в заметку и, если просили, красный таск с уведомлением."""
        from airflow.exceptions import AirflowFailException

        p = context['params']
        counts = snapshot['counts']
        totals = snapshot['totals']
        head = (f"сессий {totals['sessions']}, тасков в работе {totals['running_tasks']}, "
                f"находок {len(snapshot['findings'])}")

        if not snapshot['findings']:
            add_note(f"✅ чисто: {head}", level='task,dag', context=context, title='🩺 pg_activity')
            return head

        # Заметка режется до 1000 символов, поэтому в неё идут счётчики и три самые
        # старые находки; полный список — в логе таска collect.
        top = sorted(snapshot['findings'], key=lambda f: -(f['tx_age'] or 0))[:3]
        lines = [
            f"pid={f['pid']} {','.join(f['kinds'])} tx={f['tx_age']}с "
            f"{f['owners'][0] if f['owners'] else f['application_name']}"
            for f in top
        ]
        add_note({f"⚠️ {head} · {counts}": lines},
                 level='task,dag', context=context, title='🩺 pg_activity')

        if p['alert']:
            raise AirflowFailException(f"⚠️ {head} · {counts}\n" + "\n".join(lines))
        return head

    snapshot = collect()

    # prune не про находки: его дело — папка в S3. В цепочке до report он стоял зря,
    # добавляя лишний прыжок через очередь к тому, ради чего даг и заводился.
    snapshot >> prune()
    [save(snapshot), terminate(snapshot)] >> report(snapshot)


tools_pg_activity()
