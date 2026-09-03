CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data (
	zscore double precision null,
	ts timestamp without time zone null,
	object text null,
	key_date date null,
	rows_count bigint null,
	key_diff integer null,
	value bigint null,
	avg bigint null,
	std bigint null,
	cnt bigint null,
	min bigint null,
	max bigint null,
	log_id integer null,
	notes json null
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (log_id);