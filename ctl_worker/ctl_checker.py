"""### DAG: Проверка API CTL

Ручной запуск HTTP-запроса к CTL API для диагностики и отладки.

| Параметр | Описание |
|---|---|
| `url` | Шаблон URL (`/v4/api/entity/{eid}/child`, `/v4/api/loading/{lid}` …) |
| `method` | HTTP-метод: `GET`, `POST`, `PUT`, `DELETE` |
| `eid` | ID сущности (автозаполнение из `ctl_entities`) |
| `wid` | ID workflow (автозаполнение из `ctl_workflows`) |
| `lid` | ID загрузки |
| `limit` | Лимит результатов |
| `data` | JSON-тело для POST/PUT |
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Param
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException

from plugins.utils import  on_callback # type: ignore
from plugins.ctl_utils import ctl_obj_load, ctl_api # type: ignore

from  logging import getLogger
logger = getLogger("airflow.task")

ent_dict = ctl_obj_load('ctl_entities')
cts_dict = ctl_obj_load('ctl_categories')
wfs_dict = ctl_obj_load('ctl_workflows')


params={
    # "conn_id": Param('ctl', type="string", title="ID подключение" ), 
    "url": Param("/v5/api/info", type="string",
        title = "API URL",
        examples=[
            "/v5/api/info",
            "/v4/api/permission",
            "/v4/api/permission5",
            "/v4/api/wf/{wid}",
            "/v4/api/loading/{lid}",
            "/v4/api/entity/{eid}",
            "/v4/api/entity/{eid}/child?direct=false",
            "/v4/api/entity/{eid}/statval/all?onlyLastLoading=false&offset=0&limit={limit}&includeSynchronized=true",
            "/v4/api/entity/{eid}/stat/2/statval/last", 
            "/v4/api/wf/{wid}/loading?scheduleAfterStart=false",
            "/v4/api/loading/{lid}/scheduled",
        ],
    ),
    "method": Param(
        'GET', type="string", 
        examples=["GET", "POST", "PUT", "DELETE"],
        title="Метод API",
        # values_display={c: f"{c.lower()}" for c in methods},
        
    ),
    "eid": Param("941010000", type=["string", "null"], 
        enum=list(ent_dict.keys()), 
        values_display={c : f"{c} {ent_dict[c]['name'].replace('DATAMART#','').lower()}" for c in list(ent_dict.keys())}, 
        title="Entity",
        section="Options",
    ),
    "wid": Param(
        sorted(
            wfs_dict.keys(), 
            key=lambda c: (wfs_dict[c].get('category', ''), wfs_dict[c].get('name', ''))
        )[0] if wfs_dict else None, 
        type=["string", "null"], 
        enum=sorted(
            wfs_dict.keys(), 
            key=lambda c: (wfs_dict[c].get('category', ''), wfs_dict[c].get('name', ''))
        ) if wfs_dict else [], 
       values_display={
            c: f"{wfs_dict[c].get('category', '').split('.')[-1]}/{wfs_dict[c].get('name', '').split('.')[-1]}" 
            for c in (wfs_dict.keys() if wfs_dict else [])
        }, 
        title="WorkFlow",
        section="Options",
    ),
    "lid": Param(0, type=["integer","null"], title="loading_id", min=0, section="Options",),
    # "a_simple_list": ["",],
    
    # "offset": Param(0, const=0, type="integer", min=1),
    "limit": Param(100, const=100, type="integer", min=1, max=1000),
    "data": Param({}, section="Options",),
    # "data": Param("{\n}", type="string", format="multiline",),
}


# Основная логика DAG
with DAG(
    dag_id="tools_ctl_check_api",
    description="Tools: CTL Check API",
    default_args={
        "owner": "EDP.ETL", 
        "retries": 0,
        "on_failure_callback": on_callback,
        "on_success_callback": on_callback,
    },
    tags=["EDP_ETL", 'CTL_agent', "tools", "CTL"],
    start_date=days_ago(1),
    schedule_interval=None,
    # schedule_interval=timedelta(minutes=11),
    catchup=False,
    render_template_as_native_obj=True,
    params=params,
    on_failure_callback=on_callback,
    on_success_callback=on_callback,
    doc_md=__doc__,
) as dag:

    @task
    def chk_ctl_api(**context):
        """### 🔍 Проверка API CTL

    Выполняет HTTP-запрос к API CTL с параметрами из `context['params']`.

    **Функционал:**
    - Подставляет значения в шаблон URL (`{lid}`, `{wid}`, `{eid}`, `{limit}`).
    - Парсит тело запроса (`data`) как Python-объект.
    - Отправляет запрос через `ctl_api`.
    - Логирует:
      - URL, метод, данные.
      - Ответ в формате `pprint` (читаемый JSON).
    - Возвращает сырой ответ.

    **Использование:**
    - Для отладки новых интеграций.
    - Проверка доступности сущностей, workflow'ов, загрузок.
    - Тестирование прав доступа и параметров.

    **Примеры URL:**
    - `/v5/api/info`
    - `/v4/api/permission`
    - `/v4/api/wf/{wid}/loading`

    **XCom Output:** полный ответ API (dict/list).
    """
        # from pprint import pformat
        import json
        
        p = context["params"]
        method = p.get("method", "GET")
        eid = p.get("eid", "941010000") 
        wid = p.get("wid", "") 
        lid = p.get("wid", 0) 
        limit = p.get("limit", 100)
        offset = p.get("offset", 0)
        
        url = p["url"]
        url = url.format(lid=lid, wid=wid, eid=eid, limit=limit, offset=offset)
        
        try:
            import ast
            data = ast.literal_eval(p["data"])
        except (ValueError, TypeError, SyntaxError):
            data = p.get("data", {})
        
        logger.info(f"🔍 CTL API {method} {url} {data}")
        
        try:
            ret = ctl_api(url=url, method=method, json=data)
        except Exception as e:
            raise AirflowSkipException(str(e))
        
        # logger.info(f"🔍\n {pformat(ret, indent=4)}")
        logger.info(f"🔍\n{json.dumps(ret, indent=4, default=str)}")
        
        return ret

    chk_ctl_api()

