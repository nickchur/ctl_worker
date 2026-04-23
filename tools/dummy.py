"""
# Тестовый DAG

Используется для проверки отображения Markdown в Airflow UI.

---

| **Параметр** | *Значение* | Описание |
| :--- | :---: | :--- |
| `timeout` | 30 | Время ожидания |
| `retries` | 0 | Количество попыток |
| `retry_delay` | 30 | Время между попытками |

---

> Данный DAG предназначен для тестирования и отладки.

- один
    - *один_один*
- два
- три

🔗 [Открыть лог в Elastic](https://logs.company.com)

```json
{
    "key": "value"
}
```
"""

import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag

from plugins.utils import on_callback, default_args  # type: ignore


@dag(
    doc_md=__doc__,
    tags=['HR_Data', 'tools', 'dummy'],
    owner_links={'HR_Data (CI02750757)': 'https://confluence.delta.sbrf.ru/pages/viewpage.action?pageId=1774392110'},
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 22, tz=pendulum.UTC),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    on_failure_callback=on_callback,
    on_success_callback=on_callback,
)
def dummy_dag():
    EmptyOperator(
        task_id='dummy_task',
        on_failure_callback=on_callback,
        on_success_callback=on_callback,
        doc_md=__doc__,
    )


dummy_dag()
