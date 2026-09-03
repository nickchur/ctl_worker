CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_get_status(wf text) 
	RETURNS json
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

declare
--	rec record;
	jsn json;
begin 
	select row_to_json(a.*) into jsn
	from (select * from s_grnplm_vd_hr_edp_srv_wf.vw_swf where wf_name = wf) a;
		
	if not found then
		return json_build_object('code', -1, 'status', 'not found');
	end if;

	if not (jsn->>'active')::bool then
		return json_build_object('code', 0, 'status', 'not active', 'rec', jsn);
	end if;

	if (jsn->>'todo')::bool then
		return json_build_object('code', 1, 'status', 'todo', 'rec', jsn);
	end if;

	if (jsn->>'done')::bool then
		return json_build_object('code', 2, 'status', 'done', 'rec', jsn);
	end if;

	return json_build_object('code', 3, 'status', 'wait', 'rec', jsn);
end; 

$body$
EXECUTE ON ANY;
	