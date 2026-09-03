CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl_all (
	ts timestamp without time zone not null DEFAULT clock_timestamp(),
	url text not null,
	data text null,
	msg text null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;