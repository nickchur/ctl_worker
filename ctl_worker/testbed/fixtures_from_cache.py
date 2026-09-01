"""🧬 Снимок бакета edpetl-ctl → фикстуры эмулятора CTL.
*2026-09-01 12:35 MSK · v1.0 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Бакет `edpetl-ctl` — это кэш `ctl_loader`, то есть данные CTL, уже пропущенные через
нормализацию (`ctl_core.ctl_wf_norm`, `ctl_core.ctl_loading_norm`). Эмулятор обязан
отдавать СЫРОЙ вид, иначе загрузчик нормализует уже нормализованное и получит не то.
Скрипт разворачивает нормализацию обратно.

    python3 fixtures_from_cache.py ~/edpetl-ctl ./fixtures

На входе — распакованный бакет, на выходе — каталог фикстур, который читает `ctl_mock.py`.
Скрипт ничего не скачивает и не меняет: только читает JSON и пишет JSON.

Что разворачивается:

| Нормализованный вид (в бакете) | Сырой вид (в API) |
|---|---|
| `wf['params'] = {ключ: значение}` | `wf['param'] = [{'param', 'prior_value'}]` |
| `wf['statusNotifications'] = {статус: адреса}` | список `[{'status', 'emails'}]` |
| `wf['wf_event_sched'] = {'prf/eid/sid': active}` | список `[{'profile','entity_id','stat_id','active'}]` |
| `params['wf_entity'] = '1,2,3'` | `connectedEntities = [1, 2, 3]` рядом с `wf` |
| `ld['params'] = {ключ: значение}` | `[{'param', 'value'}]` |
| `ld['loading_status'] = {дата: 'STATUS лог'}` | `[{'status', 'effective_from', 'log'}]` |
| `ld['stats'] = {'lid/prf/eid/sid': значение}` | `[{'loading_id','profile','entity_id','stat_id','value'}]` |

Ключи, которых в API не было вовсе (`wf_name`, `ld_run_type`, `ld_status_last`,
`status_sdt`), выбрасываются: их достраивает наш же код, и в ответе эмулятора им не место.

⚠️ Одно место развернуть нельзя. В нормализованном виде история статусов — словарь по
`effective_from`, поэтому два статуса с ОДИНАКОВОЙ отметкой времени (в снимке это пары
`PARAM` + `LOCK-WAIT`) схлопнулись в одну запись ещё при сохранении в бакет. Обратно
восстановить нечего: потерялось до нас. На поведение это не влияет — код смотрит на
последний статус, а не на первый.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ключи, добавленные загрузчиком поверх ответа API.
LOADER_KEYS = ('wf_name', 'ld_run_type', 'ld_status_last', 'status_sdt')


def _load(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def raw_workflow(wf: dict) -> tuple[dict, list[int]]:
    """Нормализованный воркфлоу → (сырой воркфлоу, connectedEntities)."""
    wf = dict(wf)
    params = dict(wf.pop('params', {}) or {})

    # wf_entity загрузчик собирает из connectedEntities либо из /wf/{id}/entity —
    # возвращаем его туда, откуда он пришёл, иначе после нормализации он удвоится.
    entities = [int(e) for e in str(params.pop('wf_entity', '')).split(',') if e.strip().isdigit()]

    wf['param'] = [{'param': k, 'prior_value': v} for k, v in sorted(params.items())]
    wf['statusNotifications'] = [
        {'status': k, 'emails': v} for k, v in sorted((wf.get('statusNotifications') or {}).items())
    ]

    events = wf.get('wf_event_sched')
    if isinstance(events, dict):
        raw_events = []
        for key, active in sorted(events.items()):
            prf, eid, sid = key.split('/')
            raw_events.append({'profile': prf, 'entity_id': int(eid),
                               'stat_id': int(sid), 'active': bool(active)})
        wf['wf_event_sched'] = raw_events

    return wf, entities


def raw_loading(ld: dict) -> dict:
    """Нормализованная загрузка → сырая, как её отдаёт /v4/api/loading/{id}."""
    ld = {k: v for k, v in ld.items() if k not in LOADER_KEYS}

    ld['params'] = [{'param': k, 'value': v} for k, v in sorted((ld.get('params') or {}).items())]

    statuses = []
    for eff, line in (ld.get('loading_status') or {}).items():
        status, _, log = str(line).partition(' ')
        statuses.append({'status': status, 'effective_from': eff, 'log': log})
    # По УБЫВАНИЮ: именно так список приходит из CTL. Проверяется round-trip'ом —
    # ctl_loading_norm берёт status_sdt из первого элемента (`loading_status[0]`), и при
    # сортировке по возрастанию туда попадала дата создания загрузки вместо последней.
    ld['loading_status'] = sorted(statuses, key=lambda s: s['effective_from'], reverse=True)

    stats = ld.get('stats')
    if isinstance(stats, dict):
        raw_stats = []
        for key, value in sorted(stats.items()):
            lid, prf, eid, sid = key.split('/')
            raw_stats.append({'loading_id': int(lid), 'profile': prf, 'entity_id': int(eid),
                              'stat_id': int(sid), 'value': value})
        ld['stats'] = raw_stats
    # None оставляем None: у загрузки без статистик поле в API именно пустое, а не
    # пустой список, и нормализация эту разницу сохраняет.

    return ld


def build(src: Path, dst: Path, wf_limit: int = 0) -> None:
    """wf_limit > 0 — оставить не больше стольких воркфлоу.

    Зачем ограничивать: ctl_worker.py строит ПО DAG-У НА КАЖДЫЙ воркфлоу профиля, а их
    в снимке 685. Стенд на этом упирается в dagbag_import_timeout (30 сек по умолчанию),
    и файл не даёт ни одного дага. Для прогона тракта хватает пары десятков — в набор
    всегда попадают те, на которые ссылаются загрузки из снимка, иначе сенсору будет
    некого поднимать.
    """
    dst.mkdir(parents=True, exist_ok=True)

    # ── Справочники ──────────────────────────────────────────────────────────
    profile = _load(src / 'ctl_profile.json')
    (dst / 'profile.json').write_text(json.dumps(profile, ensure_ascii=False, indent=1), encoding='utf-8')

    # Категории загрузчик хранит словарём и дописывает parent_name — API отдаёт список.
    cats = [
        {k: v for k, v in c.items() if k != 'parent_name'}
        for c in _load(src / 'ctl_categories.json').values()
    ]
    (dst / 'categories.json').write_text(json.dumps(cats, ensure_ascii=False, indent=1), encoding='utf-8')

    entities = _load(src / 'ctl_entities.json')
    (dst / 'entities.json').write_text(json.dumps(entities, ensure_ascii=False, indent=1), encoding='utf-8')

    enames_path = src / 'ctl_enames.json'
    if enames_path.exists():
        (dst / 'enames.json').write_text(enames_path.read_text(encoding='utf-8'), encoding='utf-8')

    # ── Загрузки ─────────────────────────────────────────────────────────────
    # ctl_working — то, что воркер видел последним; ctl_*_events — ожидающие события
    # и расписание. Вместе это готовый набор состояний: RUNNING, TIME-WAIT, EVENT-WAIT.
    loadings: dict[int, dict] = {}
    for path in sorted((src / 'ctl_working').glob('*.json')) if (src / 'ctl_working').is_dir() else []:
        ld = raw_loading(_load(path))
        loadings[int(ld['id'])] = ld
    for name in ('ctl_prf_events.json', 'ctl_ue_events.json', 'ctl_entity_events.json'):
        path = src / name
        if not path.exists():
            continue
        data = _load(path)
        for ld in (data.values() if isinstance(data, dict) else data):
            if isinstance(ld, dict) and 'id' in ld:
                loadings.setdefault(int(ld['id']), raw_loading(ld))
    (dst / 'loadings.json').write_text(
        json.dumps([loadings[k] for k in sorted(loadings)], ensure_ascii=False, indent=1), encoding='utf-8')

    # ── Воркфлоу ─────────────────────────────────────────────────────────────
    # Считаем ПОСЛЕ загрузок: если набор урезаем, воркфлоу этих загрузок обязаны в него
    # попасть — иначе сенсор поднимет даг, которого нет.
    wanted = {int(ld['wf_id']) for ld in loadings.values()}
    wfs, extra = [], []
    for wf in _load(src / 'ctl_workflows.json').values():
        raw, entity_ids = raw_workflow(wf)
        item = {'wf': raw, 'connectedEntities': entity_ids}
        (wfs if int(raw['id']) in wanted else extra).append(item)

    if wf_limit:
        # Добираем до лимита теми, что не удалены и не выключены: они интереснее архива.
        extra.sort(key=lambda i: (bool(i['wf'].get('deleted')), -int(i['wf'].get('id', 0))))
        wfs += extra[:max(0, wf_limit - len(wfs))]
    else:
        wfs += extra
    (dst / 'workflows.json').write_text(json.dumps(wfs, ensure_ascii=False, indent=1), encoding='utf-8')

    # ── Затравка statval'ов ──────────────────────────────────────────────────
    # По одному значению на каждое известное событие, датированному вчерашним днём:
    # тогда EVENT-WAIT честно ждёт (событие старше даты запуска), а «наступление»
    # события — это ручная публикация свежего значения через POST statval.
    stamp = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    seeds = []
    events_path = src / 'ctl_events.json'
    if events_path.exists():
        for key in _load(events_path):
            prf, eid, sid = key.split('/')
            seeds.append({'profile': prf, 'entity_id': int(eid), 'stat_id': int(sid),
                          'value': 'seed', 'published_dttm': stamp})
    (dst / 'statvals.json').write_text(json.dumps(seeds, ensure_ascii=False, indent=1), encoding='utf-8')

    print(f"фикстуры в {dst}:")
    for name, count in (
        ('workflows.json', len(wfs)),
        ('categories.json', len(cats)),
        ('entities.json', len(entities)),
        ('loadings.json', len(loadings)),
        ('statvals.json', len(seeds)),
    ):
        print(f"  {name:18} {count}")


if __name__ == '__main__':
    if not 3 <= len(sys.argv) <= 4:
        raise SystemExit(
            __doc__.strip().splitlines()[0]
            + "\n\n  python3 fixtures_from_cache.py <снимок бакета> <каталог фикстур> [сколько воркфлоу]"
            + "\n\n  Без последнего аргумента переносятся все — на стенде это 685 дагов воркера"
              "\n  и разбор файла не укладывается в dagbag_import_timeout."
        )
    build(Path(sys.argv[1]).expanduser(), Path(sys.argv[2]).expanduser(),
          int(sys.argv[3]) if len(sys.argv) == 4 else 0)
