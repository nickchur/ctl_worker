CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_get_next(wf_first timestamp without time zone, wf_interval interval, wf_last timestamp without time zone, wf_end timestamp without time zone DEFAULT NULL::timestamp without time zone) 
	RETURNS timestamp without time zone
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

declare 
	ret timestamp;
begin 
--	ret =  (select (max(wf_next)) wf_next from (select generate_series(wf_first, wf_last, wf_interval) wf_next) a);
--	ret =  (select (max(wf_next)) wf_next from (select generate_series(wf_first, now(), wf_interval) wf_next) a);
	ret =  (select (max(wf_next)) wf_next from (select generate_series(wf_first, wf_last + wf_interval, wf_interval) wf_next) a);
	if ret <= wf_last then
		ret = ret  + wf_interval;
	end if;
	ret = coalesce(ret, wf_first);
	if ret > wf_end then
		return null;
	end if;

	return ret;
end; 

$body$
EXECUTE ON ANY;
	