CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl (
	ts timestamp without time zone not null DEFAULT now(),
	id bigint null,
	obj text null,
	url text null,
	msg jsonb null
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;