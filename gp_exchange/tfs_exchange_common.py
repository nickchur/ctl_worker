import logging
import uuid

import pendulum
from airflow import Dataset

logger = logging.getLogger(__name__)

# переменные, относящиеся к конфигурации ТФС, объявлены на странице в конфлюенсе
# https://confluence.delta.sbrf.ru/pages/viewpage.action?pageId=140421537
TFS_IN_SCENARIO = '27671910'

TFS_IN_CONN_ID = 's3-tfs-hrplt' ###
TFS_IN_BUCKET = 'tfshrplt' ###
TFS_IN_TOPIC = 'TFS.DSSIGMA.IN'
TFS_IN_PREFIX = 'to/CAPUE/pkap1080_to_hrplt/pc1080.' ###
# ⚠️ Путь собирается из __name__: ProduceToTopicOperator принимает delivery_callback
# только строкой и делает import_string. Прежнее значение указывало на ЧУЖОЙ модуль —
# export_tfs.tfs_common, — хотя колбэк лежит здесь же, ниже, а сам файл приезжает как
# CI06932748.analytics.datalab.gp_exchange.tfs_exchange_common на контуре (см. импорты
# в tfs_exchange_import.py и tfs_exchange_sensor.py) и как gp_exchange.tfs_exchange_common
# на стенде. __name__ даёт верное имя в обоих случаях.
# Продюсера в gp_exchange пока нет — константа готова к тому, что он появится.
TFS_KAFKA_CALLBACK = f"{__name__}.tfs_message_delivery_callback"

ON_CLUSTER = 'ON CLUSTER datalab'
CH_BD = 'support'
CH_ID = 'dlab-click'
GP_EXCHANGE = 'gp_exchange'


REPLICATED = 'Replicated'
# TFS_IN_DATASET = Dataset(f's3://{TFS_IN_BUCKET}/{TFS_IN_PREFIX}ue_exchange.csv')
TFS_IN_DATASET = Dataset(f's3://{TFS_IN_BUCKET}/{TFS_IN_PREFIX}ue_exchange_*.csv')

default_args = dict(
    owner='DataLab (CI02420667)',
    retries=2,
    retry_delay=pendulum.duration(seconds=30),
    bucket_name=TFS_IN_BUCKET,
    bucket=TFS_IN_BUCKET,
    s3_bucket=TFS_IN_BUCKET,
    aws_conn_id=TFS_IN_CONN_ID,
    clickhouse_conn_id=CH_ID,
    do_xcom_push=True,
    depends_on_past=False,
    wait_for_downstream=False,
)


def produce_tfs_kafka_notification(scenario_id: str, file_name: str):
    """
    Создает управляющее сообщение ТФС по формату определенному здесь:
    https://confluence.delta.sbrf.ru/pages/viewpage.action?pageId=130829910
    :param scenario_id: идентификатор сценария ТФС
    :param file_name: название файла, указывается ключ в S3 без префикса
    :return:
    """
    # генерируем UUID
    rq_uuid = str(uuid.uuid4()).replace('-', '')

    # создаем сообщение
    message = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <TransferFileCephRq>
        <RqUID>{rq_uuid}</RqUID>
        <RqTm>{pendulum.now().format('YYYY-MM-DDTHH:mm:ss.SSSZ')}</RqTm>
        <ScenarioInfo>
            <ScenarioId>{scenario_id}</ScenarioId>
        </ScenarioInfo>
        <File>
            <FileInfo>
                <Name>{file_name}</Name>
            </FileInfo>
        </File>
    </TransferFileCephRq>"""

    logger.info("Prepare message to send:\n%s", message)

    # отправляем пару (key, message)
    yield None, message


def tfs_message_delivery_callback(err: object, msg: object) -> None:
    """
    Коллбек, который нужен, чтобы рейзить ошибку, дефолтный просто логгирует
    Нам нужно чтобы таск падал и ретраился
    :param err: ошибка из кафки или None
    :type err: confluent_kafka.KafkaError
    :param msg: сообщение
    :type msg: confluent_kafka.Message
    :return:
    """
    if err is not None:
        raise RuntimeError(f"Failed to deliver message: {err}")
    else:
        logger.info(
            "Produced record to topic %s, partition [%s] @ offset %s, text;\n%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.value(),
        )

