CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_skew (
	ts timestamp without time zone null,
	tbl text null,
	skew double precision null,
	segments integer null,
	sum bigint null,
	min bigint null,
	max bigint null,
	avg bigint null,
	std bigint null,
	data_size bigint null,
	distributedby text null,
	tbl_size bigint null,
	duration interval null,
	options text null,
	storage text null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED BY (tbl);