CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.try_cast2int(cast_text text, debug boolean DEFAULT false) 
	RETURNS bigint
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

begin
    return nullif(cast_text,'')::int8; 
exception when OTHERS then
    if debug is True then
        raise info 'Error %', cast_text;
    end if;
    return null::int8;
end;

$body$
EXECUTE ON ANY;
	