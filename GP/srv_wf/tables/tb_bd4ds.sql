CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_bd4ds (
	grp text null,
	sch text null,
	tbl text null,
	whr text null,
	bdate text null,
	operation text null,
	condition text null
)
WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
DISTRIBUTED RANDOMLY;