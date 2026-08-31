#!/usr/bin/env bash
# Выгрузка пакета обмена из PG в файл формата ТФС и укладка в S3.
#
# Формат диктует загрузчик (tfs_exchange_import.py, задача import_files):
# CustomSeparatedWithNames, разделитель — таб, экранирование CSV,
# format_csv_allow_double_quotes=False. Последнее и есть причина трюка с QUOTE:
# поля не должны оборачиваться в кавычки, иначе ClickHouse получит их как часть
# JSON. Даём PostgreSQL символ кавычки, которого в данных не бывает (\x01), —
# и он не оборачивает ничего. Табов и переводов строк внутри JSON тоже нет:
# row_to_json экранирует их сам.
set -euo pipefail

PKG_ID="${1:-0}"
TS=$(date -u +%Y%m%d%H%M%S)
NAME="pc1080.ue_exchange_${TS}.csv"
OUT="/tmp/${NAME}"

docker exec -i aftest-postgres psql -U airflow -d gp_test -v ON_ERROR_STOP=1 -q -c "
COPY (
    SELECT wf_id, wf_name, wf_key, wf_data
    FROM s_grnplm_vd_hr_edp_vda.pr_exchange(${PKG_ID})
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\t', QUOTE E'\x01', ESCAPE E'\x01', HEADER true)
" > "$OUT"

echo "строк в файле: $(wc -l < "$OUT") (первая — заголовок)"
docker exec -i aftest-minio mc alias set local http://localhost:9000 "$MINIO_USER" "$MINIO_PASS" >/dev/null 2>&1 || true
docker cp "$OUT" aftest-minio:/tmp/"$NAME"
docker exec -i aftest-minio mc cp /tmp/"$NAME" "local/tfshrplt/to/CAPUE/pkap1080_to_hrplt/${NAME}"
echo "выложено: s3://tfshrplt/to/CAPUE/pkap1080_to_hrplt/${NAME}"
