DROP FUNCTION plan_scans_index (query text, index_name text);
DROP FUNCTION comparison_operator (is_unique boolean);
DROP FUNCTION nullChecks(col_list text);
DROP FUNCTION checkIntegrity();

CREATE FUNCTION plan_scans_index(query text, index_name text) RETURNS boolean AS $$
DECLARE
    line text;
BEGIN
    FOR line IN EXECUTE 'EXPLAIN ' || query LOOP
        -- This serves as a smoke test to verify the plan atleast contains a scan over the index we want.
        IF line LIKE '%Index Scan%' THEN
	    IF line LIKE '%' || index_name || '%' THEN
	        RETURN TRUE;
	    ELSE
	        RAISE WARNING 'Index (%) is probably redundant and should be removed', index_name;
		RAISE INFO 'See: %', line;
	    END IF;
	END IF;
    END LOOP;
    
    --ELSE
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION comparison_operator(is_unique boolean) RETURNS text AS $$
BEGIN
    IF is_unique THEN
        RETURN '<=';
    ELSE
        RETURN '<';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION nullChecks(col_list text) RETURNS text AS $$
DECLARE
    col text;
    retval text := '';
    funct text;
    paren_count int;
BEGIN
    IF col_list LIKE '%(%,%)%' THEN
        -- TODO Solve the function case
	RETURN '';
    END IF;
    FOR col IN SELECT unnest(regexp_split_to_array(col_list, ',')) LOOP
	
	retval = retval || ' AND ' || col || ' IS NOT NULL ';
    END LOOP;
    RETURN retval;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION checkIntegrity() RETURNS VOID AS $$
DECLARE
    ind RECORD;
    index_name text;
    inddef text;
    column_list text;
    where_conditions text;
    table_of_index text;
    error_entry RECORD;
    scan_query text;
    sort_list text;
BEGIN
    -- TODO: btree only
    FOR ind IN SELECT * FROM pg_index WHERE 0 <> ANY (indcollation) ORDER BY pg_table_size(indrelid::regclass) ASC LOOP
	SELECT pg_catalog.pg_get_indexdef(ind.indexrelid, 0, false) INTO inddef;
	SELECT ind.indexrelid::regclass::text INTO index_name;
	SELECT ind.indrelid::regclass::text INTO table_of_index;
        RAISE INFO 'Checking index: %', index_name;
        RAISE INFO 'Def: %', inddef;
        
	inddef = regexp_replace(inddef, '.*USING btree ', '');
	column_list = regexp_replace(inddef, ' WHERE.*', '');
        where_conditions = substring(inddef from ' WHERE.*');
	where_conditions = CASE WHEN where_conditions IS NULL THEN ' WHERE 1=1' ELSE where_conditions END;
	sort_list = regexp_replace(column_list, '(^[(])|([)]$)', '', 'g');
	scan_query = 'SELECT cur, lag FROM (SELECT ' || column_list || ' as cur, lag (' || column_list || ') OVER (ORDER BY ' || sort_list || ')' ||
	             'FROM ' || table_of_index || where_conditions || nullChecks(sort_list)||') f WHERE cur ' || comparison_operator(ind.indisunique) || ' lag';
	RAISE INFO '%', scan_query;
       
        IF inddef LIKE '%DESC%' THEN
	    RAISE WARNING 'Index (%) contains a DESC field...  Unsupported.', index_name;
	ELSIF NOT plan_scans_index(scan_query, index_name) THEN
	    RAISE WARNING 'Unable to convince the planner to scan the index: %', index_name;
	ELSE
            FOR error_entry IN EXECUTE scan_query  LOOP
	        RAISE WARNING 'Found bad entry % in index %', error_entry, index_name;
            END LOOP;
	END IF;

    END LOOP;

END;
$$ LANGUAGE plpgsql;

-- Makes it much easier to convince planner to use the index
--SET enable_sort to False;
--SELECT pg_temp.checkIntegrity();
