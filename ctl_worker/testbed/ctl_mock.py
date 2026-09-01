"""🎭 Эмулятор CTL API для тестового стенда.
*2026-09-01 13:10 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Отвечает так, как отвечает CTL нашим дагам: справочники — из фикстур (снимок боевого
бакета `edpetl-ctl`, развёрнутый `fixtures_from_cache.py`), состояние загрузок — в
postgres, схема `ctl_mock` (`schema.sql`). Ничего, кроме нашего кода, эмулятор не
обслуживает: набор эндпоинтов снят с `plugins/ctl_utils.py`, `plugins/ctl_core.py`
и каталога `ctl_worker/`.

Запуск (юнит `ctl-mock.service` делает то же самое):

    CTL_MOCK_DSN='postgresql://airflow:pass@127.0.0.1:5432/gp_test' \\
    CTL_MOCK_FIXTURES=/opt/aftest/ctl-mock/fixtures \\
    /opt/aftest/venv/bin/python -m uvicorn ctl_mock:app --host 127.0.0.1 --port 9080

Никакой авторизации: `KerberosHttpHook` шлёт Negotiate только в ответ на 401, а мы 401
не отдаём — значит ни KDC, ни keytab на стенде не нужны.

⚠️ Это эмулятор, а не спецификация: он повторяет то, что читает наш код. Поля, которых
мы не касаемся, приезжают из снимка как есть, но выдуманные эмулятором объекты (новая
загрузка, statval) несут только то, что кто-то у нас читает.
"""
from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import psycopg2.pool
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

DSN = os.getenv('CTL_MOCK_DSN', 'postgresql://airflow:airflow@127.0.0.1:5432/gp_test')
FIXTURES = Path(os.getenv('CTL_MOCK_FIXTURES', Path(__file__).with_name('fixtures')))
PROFILE = os.getenv('CTL_MOCK_PROFILE', 'HR_Data')
ROOT_ENTITY = int(os.getenv('CTL_MOCK_ROOT_ENTITY', '941010000'))

TS = '%Y-%m-%d %H:%M:%S.%f'


# ── Фикстуры ─────────────────────────────────────────────────────────────────

def _fixture(name, default):
    path = FIXTURES / name
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else default


PROFILE_OBJ = _fixture('profile.json', {'id': 1557, 'name': PROFILE})
CATEGORIES = _fixture('categories.json', [])
ENTITIES = {int(k): v for k, v in _fixture('entities.json', {}).items()}
WORKFLOWS = {int(w['wf']['id']): w for w in _fixture('workflows.json', [])}

# Дети сущности — по parentId. Считаем один раз: дерево не меняется.
KIDS: dict[int, list[int]] = {}
for _eid, _ent in ENTITIES.items():
    KIDS.setdefault(int(_ent.get('parentId', -1)), []).append(_eid)


def entity_tree(eid: int) -> dict:
    """Узел дерева в том виде, в каком его разбирает ctl_loader.entity_kids."""
    return {'entity': ENTITIES.get(eid, {'id': eid}),
            'kidz': [entity_tree(k) for k in sorted(KIDS.get(eid, []))]}


def all_kids(eid: int, out: set[int]) -> set[int]:
    for k in KIDS.get(eid, []):
        out.add(k)
        all_kids(k, out)
    return out


# ── База ─────────────────────────────────────────────────────────────────────

# Пул, а не соединение на запрос: сборка одной загрузки — это четыре запроса, а
# /loading/extended собирает их пачкой. На соединении-за-запрос первичное наполнение
# упиралось в таймаут запуска systemd.
POOL = psycopg2.pool.ThreadedConnectionPool(1, 8, DSN)


@contextmanager
def db():
    conn = POOL.getconn()
    try:
        conn.autocommit = True
        yield conn
    finally:
        POOL.putconn(conn)


def q(sql: str, args=(), one: bool = False):
    with db() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, args)
        if cur.description is None:
            return None
        rows = cur.fetchall()
        return (dict(rows[0]) if rows else None) if one else [dict(r) for r in rows]


def q_many(sql: str, rows: list) -> None:
    """Пакетная вставка: наполнение — это сотни строк, и по соединению на каждую
    сервис не успевал стартовать."""
    if not rows:
        return
    with db() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)


def seed() -> None:
    """Первичное наполнение состояния из фикстур. Идемпотентно: если загрузки уже есть,
    ничего не трогаем — иначе прогон сценария затирался бы при каждом рестарте."""
    if (q('select count(*) as n from ctl_mock.loading', one=True) or {}).get('n'):
        return

    loadings, params, statuses = [], [], []
    for ld in _fixture('loadings.json', []):
        raw = {k: v for k, v in ld.items() if k not in ('params', 'loading_status')}
        loadings.append((ld['id'], ld['wf_id'], ld.get('profile', PROFILE), ld.get('alive', 'ACTIVE'),
                         ld.get('status', 'INIT'), ld.get('status_log') or '', bool(ld.get('auto', True)),
                         ld.get('start_dttm'), ld.get('end_dttm'), json.dumps(raw, ensure_ascii=False)))
        params += [(ld['id'], pr['param'], pr.get('value')) for pr in ld.get('params') or []]
        statuses += [(ld['id'], s['status'], s.get('log') or '', s['effective_from'])
                     for s in ld.get('loading_status') or []]

    q_many("""insert into ctl_mock.loading
                  (id, wf_id, profile, alive, status, status_log, auto, start_dttm, end_dttm, raw)
              values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (id) do nothing""", loadings)
    q_many('insert into ctl_mock.loading_param values (%s,%s,%s) on conflict do nothing', params)
    q_many("""insert into ctl_mock.loading_status (loading_id, status, log, effective_from)
              values (%s,%s,%s,%s) on conflict do nothing""", statuses)

    q_many("""insert into ctl_mock.statval (profile, entity_id, stat_id, value, published_dttm)
              values (%s,%s,%s,%s,%s)""",
           [(sv['profile'], sv['entity_id'], sv['stat_id'], sv.get('value'), sv['published_dttm'])
            for sv in _fixture('statvals.json', [])])

    q_many("""insert into ctl_mock.wf_state (wf_id, scheduled) values (%s,%s)
              on conflict (wf_id) do nothing""",
           [(wid, bool(item['wf'].get('scheduled'))) for wid, item in WORKFLOWS.items()])

# ── Сборка объектов ──────────────────────────────────────────────────────────

def wf_obj(wid: int) -> dict | None:
    """Воркфлоу с наложенным изменяемым состоянием (расписание и правленые параметры)."""
    item = WORKFLOWS.get(int(wid))
    if not item:
        return None
    wf = json.loads(json.dumps(item['wf']))
    st = q('select * from ctl_mock.wf_state where wf_id = %s', (wid,), one=True) or {}
    wf['scheduled'] = bool(st.get('scheduled', wf.get('scheduled')))
    extra = st.get('params') or {}
    if extra:
        by_name = {p['param']: p for p in wf.get('param', [])}
        for k, v in extra.items():
            by_name.setdefault(k, {'param': k, 'wf_id': wid})['prior_value'] = v
        wf['param'] = sorted(by_name.values(), key=lambda p: p['param'])
    return wf


def loading_obj(lid: int) -> dict | None:
    row = q('select * from ctl_mock.loading where id = %s', (lid,), one=True)
    if not row:
        return None
    ld = dict(row.pop('raw') or {})
    ld.update({
        'id': row['id'], 'wf_id': row['wf_id'], 'profile': row['profile'],
        'alive': row['alive'], 'status': row['status'], 'status_log': row['status_log'],
        'auto': row['auto'],
        'start_dttm': row['start_dttm'].strftime(TS) if row['start_dttm'] else None,
        'end_dttm': row['end_dttm'].strftime(TS) if row['end_dttm'] else None,
    })
    ld['params'] = [
        {'param': r['param'], 'value': r['value']}
        for r in q('select param, value from ctl_mock.loading_param where loading_id = %s order by param', (lid,))
    ]
    # По убыванию: ровно так список приходит из CTL, и ctl_loading_norm берёт из первого
    # элемента дату последнего статуса.
    ld['loading_status'] = [
        {'status': r['status'], 'log': r['log'], 'effective_from': r['effective_from'].strftime(TS)}
        for r in q("""select status, log, effective_from from ctl_mock.loading_status
                      where loading_id = %s order by effective_from desc""", (lid,))
    ]
    ld['stats'] = [
        {'loading_id': lid, 'profile': r['profile'], 'entity_id': r['entity_id'],
         'stat_id': r['stat_id'], 'value': r['value']}
        for r in q("""select profile, entity_id, stat_id, value from ctl_mock.statval
                      where loading_id = %s order by stat_id""", (lid,))
    ]
    if 'workflow' not in ld:
        ld['workflow'] = wf_obj(row['wf_id'])
    return ld


def set_status(lid: int, status: str, log: str = '') -> dict:
    """Новый статус загрузки. effective_from уникален в пределах загрузки (ограничение
    в схеме), поэтому при коллизии — а два статуса в одну микросекунду это реальность
    боевых данных — сдвигаем отметку на микросекунду вперёд."""
    for shift in range(10):
        try:
            q("""insert into ctl_mock.loading_status (loading_id, status, log, effective_from)
                 values (%s,%s,%s, clock_timestamp() + (%s || ' microseconds')::interval)""",
              (lid, status, log or '', shift))
            break
        except psycopg2.errors.UniqueViolation:
            continue
    q('update ctl_mock.loading set status = %s, status_log = %s where id = %s', (status, log or '', lid))
    return loading_obj(lid)


def as_list(value) -> list[str]:
    """'["RUNNING","TIME-WAIT"]' → ['RUNNING', 'TIME-WAIT']. CTL принимает фильтры
    строкой-массивом, а не повторяющимся параметром — ровно так их шлёт ctl_sensor."""
    if value is None:
        return []
    value = str(value).strip()
    try:
        parsed = json.loads(value)
        return [str(v) for v in (parsed if isinstance(parsed, list) else [parsed])]
    except json.JSONDecodeError:
        return [v.strip().strip('"\'') for v in value.strip('[]').split(',') if v.strip()]


# ── Ручки ────────────────────────────────────────────────────────────────────

async def info(request: Request):
    return JSONResponse({'name': 'ctl-mock', 'version': '1.0', 'profile': PROFILE,
                         'now': datetime.now().strftime(TS)})


async def profile_by_name(request: Request):
    return JSONResponse(PROFILE_OBJ)


async def categories(request: Request):
    return JSONResponse(CATEGORIES)


async def entities(request: Request):
    return JSONResponse(list(ENTITIES.values()))


async def entity_tree_search(request: Request):
    eid = int(request.query_params.get('search') or ROOT_ENTITY)
    # Первый элемент ctl_loader игнорирует и берёт [1] — отдаём корень и найденную ветку.
    return JSONResponse([{'entity': ENTITIES.get(0, {'id': 0, 'name': 'root'}), 'kidz': []},
                         entity_tree(eid)])


async def entity_one(request: Request):
    eid = int(request.path_params['eid'])
    return JSONResponse(ENTITIES.get(eid, {'id': eid}))


async def entity_child(request: Request):
    eid = int(request.path_params['eid'])
    direct = request.query_params.get('direct', 'true').lower() != 'false'
    kids = KIDS.get(eid, []) if direct else sorted(all_kids(eid, set()))
    return JSONResponse([ENTITIES[k] for k in kids if k in ENTITIES])


async def entity_export(request: Request):
    eid = int(request.path_params['eid'])
    kids = sorted(all_kids(eid, {eid}))
    return JSONResponse({'entityExt': [ENTITIES[k] for k in kids if k in ENTITIES]})


async def statval_last(request: Request):
    eid, sid = int(request.path_params['eid']), int(request.path_params['sid'])
    prf = request.query_params.get('profile', PROFILE)
    rows = q("""select profile, entity_id, stat_id, loading_id, value, published_dttm
                from ctl_mock.statval
                where profile = %s and entity_id = %s and stat_id = %s
                order by published_dttm desc limit 1""", (prf, eid, sid))
    return JSONResponse([{**r, 'published_dttm': r['published_dttm'].strftime(TS)} for r in rows])


async def statval_all(request: Request):
    eid = int(request.path_params['eid'])
    limit = int(request.query_params.get('limit', 100))
    rows = q("""select profile, entity_id, stat_id, loading_id, value, published_dttm
                from ctl_mock.statval where entity_id = %s
                order by published_dttm desc limit %s""", (eid, limit))
    return JSONResponse([{**r, 'published_dttm': r['published_dttm'].strftime(TS)} for r in rows])


async def wf_extended(request: Request):
    cats = set(as_list(request.query_params.get('category_ids')))
    items = [
        item for item in WORKFLOWS.values()
        if not cats or str(_cat_id(item['wf'])) in cats
    ]
    return JSONResponse([{'wf': wf_obj(item['wf']['id']), 'connectedEntities': item['connectedEntities']}
                         for item in items])


def _cat_id(wf: dict):
    """Категория воркфлоу приходит именем ('p1080.stg'), а фильтр — идентификаторами."""
    name = wf.get('category')
    for c in CATEGORIES:
        if c.get('name') == name:
            return c.get('id')
    return None


async def wf_list(request: Request):
    cat = request.query_params.get('category_id')
    items = [i['wf'] for i in WORKFLOWS.values() if not cat or str(_cat_id(i['wf'])) == str(cat)]
    return JSONResponse([wf_obj(w['id']) for w in items])


async def wf_one(request: Request):
    wf = wf_obj(int(request.path_params['wid']))
    return JSONResponse(wf) if wf else JSONResponse({'error': 'not found'}, status_code=404)


async def wf_entity(request: Request):
    item = WORKFLOWS.get(int(request.path_params['wid']))
    return JSONResponse([{'id': e} for e in (item or {}).get('connectedEntities', [])])


async def wf_export(request: Request):
    wid = int(request.path_params['wid'])
    item = WORKFLOWS.get(wid)
    if not item:
        return JSONResponse({'error': 'not found'}, status_code=404)
    wf = wf_obj(wid)
    blob = json.dumps(wf, ensure_ascii=False, sort_keys=True)
    return JSONResponse({'hash': str(abs(hash(blob)) % (10 ** 12)),
                         'date': datetime.now().strftime(TS),
                         'wfExt': {'wf': wf, 'connectedEntities': item['connectedEntities']}})


async def wf_scheduled(request: Request):
    wid = int(request.path_params['wid'])
    if request.method in ('PUT', 'DELETE'):
        q("""insert into ctl_mock.wf_state (wf_id, scheduled) values (%s,%s)
             on conflict (wf_id) do update set scheduled = excluded.scheduled""",
          (wid, request.method == 'PUT'))
    st = q('select scheduled from ctl_mock.wf_state where wf_id = %s', (wid,), one=True) or {}
    return JSONResponse({'wf_id': wid, 'scheduled': bool(st.get('scheduled'))})


async def wf_tmpl(request: Request):
    wid = int(request.path_params['wid'])
    tid = request.path_params.get('tid')
    if request.method == 'PUT':
        q("""insert into ctl_mock.wf_state (wf_id, tmpl_id) values (%s,%s)
             on conflict (wf_id) do update set tmpl_id = excluded.tmpl_id""", (wid, int(tid)))
    elif request.method == 'DELETE':
        q('update ctl_mock.wf_state set tmpl_id = null where wf_id = %s', (wid,))
    st = q('select tmpl_id from ctl_mock.wf_state where wf_id = %s', (wid,), one=True) or {}
    # Пустой шаблон — пустой объект: ctl_worker пишет `tmpl = ctl_api(...) or {}`
    # и смотрит tmpl['id'], поэтому отдавать null нельзя.
    return JSONResponse({'id': st['tmpl_id'], 'wf_id': wid} if st.get('tmpl_id') else {})


async def wf_params(request: Request):
    wid = int(request.path_params['wid'])
    if request.method == 'DELETE':
        q("""insert into ctl_mock.wf_state (wf_id, params) values (%s, '{}'::jsonb)
             on conflict (wf_id) do update set params = '{}'::jsonb""", (wid,))
        return JSONResponse({'wf_id': wid, 'params': {}})
    body = await _json(request)
    params = body if isinstance(body, dict) else {p['param']: p.get('value') for p in body or []}
    q("""insert into ctl_mock.wf_state (wf_id, params) values (%s, %s::jsonb)
         on conflict (wf_id) do update set params = excluded.params""",
      (wid, json.dumps(params, ensure_ascii=False)))
    return JSONResponse({'wf_id': wid, 'params': params})


async def wf_loading_new(request: Request):
    """Создание загрузки. Возвращает {'loadingId': …} — ctl_worker читает именно это поле."""
    wid = int(request.path_params['wid'])
    params = await _json(request) or {}
    schedule_after = request.query_params.get('scheduleAfterStart', 'false').lower() == 'true'
    lid = (q("select nextval('ctl_mock.loading_id_seq') as id", one=True))['id']
    wf = wf_obj(wid) or {}
    raw = {'workflow': wf, 'wfUuid': wf.get('uuid'), 'abortOnFailure': True,
           'descheduleOnFailure': False, 'retriesLeft': 0, 'retryDelayMs': 0,
           'awaitedEvents': [], 'entityLocksAwaited': [], 'profileLocksAwaited': [],
           'locksSet': [], 'startCondition': 'MANUAL', 'xid': None,
           'activeTriggerId': None, 'uuid': None}
    q("""insert into ctl_mock.loading (id, wf_id, profile, alive, status, status_log, auto, raw)
         values (%s,%s,%s,'ACTIVE','START','',%s,%s)""",
      (lid, wid, wf.get('profile', PROFILE), bool(schedule_after), json.dumps(raw, ensure_ascii=False)))
    # Параметры загрузки: сначала параметры воркфлоу, затем присланные — и обязательно
    # loading_id. Без него ctl_sensor отбрасывает загрузку с пометкой «no loading_id»:
    # в бою этот параметр проставляет CTL при создании, и весь наш код читает его как
    # params['loading_id'].
    merged = {pr['param']: pr.get('prior_value') for pr in wf.get('param', [])}
    merged.update(params if isinstance(params, dict) else {})
    merged['loading_id'] = str(lid)
    merged['wf_id'] = str(wid)          # ctl_sensor читает params['wf_id'] наравне с loading_id
    for name, value in merged.items():
        q('insert into ctl_mock.loading_param values (%s,%s,%s) on conflict do nothing',
          (lid, name, None if value is None else str(value)))
    set_status(lid, 'START', 'created by ctl-mock')
    if schedule_after:
        q("""insert into ctl_mock.wf_state (wf_id, scheduled) values (%s, true)
             on conflict (wf_id) do update set scheduled = true""", (wid,))
    return JSONResponse({'loadingId': lid})


async def loading_extended(request: Request):
    p = request.query_params
    alive = as_list(p.get('alive'))
    statuses = as_list(p.get('status'))
    engines = as_list(p.get('engines'))
    profiles = as_list(p.get('profile_ids'))
    limit = int(p.get('limit') or 1000)

    rows = q('select id from ctl_mock.loading order by id')
    out = []
    for r in rows:
        ld = loading_obj(r['id'])
        if alive and ld['alive'] not in alive:
            continue
        if statuses and ld['status'] not in statuses:
            continue
        # Профиль в загрузке приходит именем, а фильтр — идентификатором: сопоставляем
        # через справочник профиля (в снимке он один, чужих профилей на стенде нет).
        if profiles:
            prof_id = str(PROFILE_OBJ.get('id')) if ld.get('profile') == PROFILE_OBJ.get('name') else ''
            if prof_id not in profiles:
                continue
        if engines and (ld.get('workflow') or {}).get('engine') not in engines:
            continue
        out.append(ld)
        if len(out) >= limit:
            break
    return JSONResponse(out)


async def loading_one(request: Request):
    ld = loading_obj(int(request.path_params['lid']))
    return JSONResponse(ld) if ld else JSONResponse({'error': 'not found'}, status_code=404)


async def loading_status_put(request: Request):
    lid = int(request.path_params['lid'])
    body = await _json(request) or {}
    log = body.get('log', '')
    if not isinstance(log, str):
        log = json.dumps(log, ensure_ascii=False)
    return JSONResponse(set_status(lid, body.get('status', 'RUNNING'), log))


async def loading_finish(request: Request):
    """PUT /loading/{lid}/completed | aborted — закрывает загрузку."""
    lid = int(request.path_params['lid'])
    action = request.path_params['action'].upper()
    alive = 'COMPLETED' if action == 'COMPLETED' else 'ABORTED'
    q('update ctl_mock.loading set alive = %s, end_dttm = clock_timestamp() where id = %s', (alive, lid))
    set_status(lid, alive, f'{action.lower()} by ctl-mock')
    return JSONResponse(loading_obj(lid))


async def loading_statvals(request: Request):
    lid = int(request.path_params['lid'])
    rows = q("""select profile, entity_id, stat_id, loading_id, value, published_dttm
                from ctl_mock.statval where loading_id = %s order by published_dttm""", (lid,))
    return JSONResponse([{**r, 'published_dttm': r['published_dttm'].strftime(TS)} for r in rows])


async def statval_post(request: Request):
    """POST /loading/{lid}/entity/{eid}/stat/{sid}/statval?profile= — публикация значения.
    Тело — либо список строк (ctl_send_html), либо произвольное значение."""
    lid = int(request.path_params['lid'])
    eid, sid = int(request.path_params['eid']), int(request.path_params['sid'])
    prf = request.query_params.get('profile', PROFILE)
    body = await _json(request)
    value = body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)
    q("""insert into ctl_mock.statval (profile, entity_id, stat_id, loading_id, value)
         values (%s,%s,%s,%s,%s)""", (prf, eid, sid, lid, value))
    return JSONResponse({'profile': prf, 'entity_id': eid, 'stat_id': sid, 'loading_id': lid})


async def permission(request: Request):
    """Прав не эмулируем: на стенде один пользователь и ему можно всё."""
    return JSONResponse({'profile': PROFILE, 'permissions': ['READ', 'WRITE', 'START', 'STOP'],
                         'allowed': True})


async def _json(request: Request):
    try:
        return await request.json()
    except Exception:
        raw = (await request.body()).decode('utf-8', 'replace').strip()
        return json.loads(raw) if raw else None


async def log_requests(request: Request, call_next):
    response = await call_next(request)
    try:
        q("""insert into ctl_mock.api_log (method, path, query, status)
             values (%s,%s,%s,%s)""",
          (request.method, request.url.path, str(request.url.query), response.status_code))
    except Exception:  # журнал не должен ронять ответ
        pass
    return response


def route(path, handler, methods=('GET',)):
    """Один обработчик под обе версии API: v4 и v5 отличаются только авторизацией,
    а у нас её нет — контракт один и тот же."""
    return [Route(f'/v4/api{path}', handler, methods=list(methods)),
            Route(f'/v5/api{path}', handler, methods=list(methods))]


routes = [
    *route('/info', info),
    *route('/permission', permission),
    *route('/permission5', permission),
    *route('/profile/name/{name}', profile_by_name),
    *route('/category', categories),
    *route('/category/m', categories),
    *route('/wf/extended', wf_extended),
    *route('/entity/tree', entity_tree_search),
    *route('/entity/child/c/{eid:int}/export', entity_export),
    *route('/entity/{eid:int}/child', entity_child),
    *route('/entity/{eid:int}/stat/{sid:int}/statval/last', statval_last),
    *route('/entity/{eid:int}/stat/{sid:int}/profile/{prf}/statval', statval_last),
    *route('/entity/{eid:int}/statval/all', statval_all),
    *route('/entity/{eid:int}', entity_one),
    *route('/entity', entities),
    *route('/loading/extended', loading_extended),
    *route('/loading/{lid:int}/status', loading_status_put, ('PUT',)),
    *route('/loading/{lid:int}/entity/{eid:int}/stat/{sid:int}/statval', statval_post, ('POST',)),
    *route('/loading/{lid:int}/statvals', loading_statvals),
    *route('/loading/{lid:int}/scheduled', wf_scheduled),
    *route('/loading/{lid:int}/{action:str}', loading_finish, ('PUT',)),
    *route('/loading/{lid:int}', loading_one),
    *route('/wf/{wid:int}/loading', wf_loading_new, ('POST',)),
    *route('/wf/{wid:int}/scheduled', wf_scheduled, ('GET', 'PUT', 'DELETE')),
    *route('/wf/{wid:int}/tmpl/{tid:int}', wf_tmpl, ('GET', 'PUT', 'DELETE')),
    *route('/wf/{wid:int}/tmpl', wf_tmpl, ('GET',)),
    *route('/wf/{wid:int}/params', wf_params, ('POST', 'DELETE')),
    *route('/wf/{wid:int}/entity', wf_entity),
    *route('/wf/{wid:int}/export', wf_export),
    *route('/wf/{wid:int}', wf_one),
    *route('/wf', wf_list),
]

@asynccontextmanager
async def lifespan(_app):
    # Наполнение состояния — на старте, а не при импорте: uvicorn импортирует модуль
    # и в дочерних процессах, а сеять базу должен один.
    seed()
    yield


# on_startup и app.middleware('http') из starlette 1.x убраны — только lifespan и
# явный список middleware.
app = Starlette(routes=routes, lifespan=lifespan,
                middleware=[Middleware(BaseHTTPMiddleware, dispatch=log_requests)])
