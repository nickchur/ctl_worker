CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_swf (
	wf_id integer not null DEFAULT nextval('s_grnplm_vd_hr_edp_srv_wf.tb_swf_wf_id_seq'::regclass),
	wf_order integer not null DEFAULT 0,
	wf_name text not null,
	wf_exec text not null,
	wf_beg timestamp without time zone not null DEFAULT now(),
	wf_interval interval not null DEFAULT '1 day'::interval,
	wf_end timestamp without time zone null,
	wf_relations text[] null,
	wf_waits text[] null,
	wf_last timestamp without time zone null,
	wf_duration interval null,
	wf_swf text null,
	wf_reselt json null,
	wf_expire interval not null DEFAULT '08:00:00'::interval,
	ctl_wf_id integer null
)
WITH (appendonly=false)
DISTRIBUTED BY (wf_name);