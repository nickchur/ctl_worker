CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config (
	object text null,
	active boolean null,
	rollback boolean null,
	z_cfg json null,
	z_except date[] null,
	z_error date[] null,
	ts timestamp without time zone null DEFAULT now()
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;