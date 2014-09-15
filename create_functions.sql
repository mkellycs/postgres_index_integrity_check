/**
 * This function checks if the query plan postgres is going to use actually
 * scans the expected index. Used mostly as a sanity check. The one exception
 * to this rule is if the keys of index a is a prefix of index b, then postgres
 * will probably use index b even when we are trying to test a.  This really
 * shouldn't matter unless you find an issue with index b.  That means index a
 * probably has a similar issue.
 */
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
            RAISE WARNING '(%) is probably redundant and should be removed.  See: %', index_name, line;
        END IF;
    END IF;
    END LOOP;
    
    --ELSE
    RAISE WARNING '(%) unable to convince query planner to use index for unknown reason.', index_name;
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

/**
 * If the index is unique, the keys should be monotonically increasing.
 * Else they should be non-decreasing.
 */
CREATE FUNCTION comparison_operator(is_unique boolean) RETURNS text AS $$
BEGIN
    IF is_unique THEN
        RETURN '<=';
    ELSE
        RETURN '<';
    END IF;
END;
$$ LANGUAGE plpgsql;

/**
 * Index keys that contain nulls are just a bit messy to compare, so we just
 * ignore those rows.
 * However, pulling apart the keys in a functional index is painful.  I didn't
 * find a functional index here at TripAdvisor, where the function returned
 * null, so I just left a TODO.
 */
CREATE FUNCTION null_checks(columns text) RETURNS text AS $$
DECLARE
    col text;
    retval text := '';
BEGIN
    IF columns LIKE '%(%,%)%' THEN
        -- TODO Solve the function case
    RETURN '';
    END IF;

    FOR col IN SELECT unnest(regexp_split_to_array(columns, ',')) LOOP
    
    retval = retval || ' AND ' || col || ' IS NOT NULL ';
    END LOOP;
    RETURN retval;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION check_index(index_reg regclass, out skipped boolean, out valid boolean, out bad_entry_count int) AS $$
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
    -- Init output params
    skipped = true;
    valid = null;
    bad_entry_count = 0;

    SELECT * INTO ind FROM pg_index WHERE indexrelid = index_reg::oid;
    SELECT pg_catalog.pg_get_indexdef(ind.indexrelid, 0, false) INTO inddef;
    SELECT ind.indexrelid::regclass::text INTO index_name;
    SELECT ind.indrelid::regclass::text INTO table_of_index;

    RAISE INFO '(%) Starting', index_name;
    RAISE INFO '(%) Definition: %', index_name, inddef;
    
    -- Check for our unsupported cases
    IF inddef NOT LIKE '%USING btree%' THEN
       RAISE WARNING '(%) btree not supported.  Skipping...', index_name;
       RETURN;
    END IF;

    IF inddef LIKE '%DESC%' THEN
        RAISE WARNING '(%) DESC field not supported.  Skipping...', index_name;
        RETURN;
    END IF;

    -- The following is pretty hacky, but it works.
    -- Just kill USING btree because we already established this is a btree.
    inddef = regexp_replace(inddef, '.*USING btree ', '');
    -- Everything before the WHERE is the column list, conviniently with parans
    -- which give us a composite type
    column_list = regexp_replace(inddef, ' WHERE.*', '');
    -- Sort list needs to have the parens stripped though
    sort_list = regexp_replace(column_list, '(^[(])|([)]$)', '', 'g');

    -- Use the where conditions that were on the index.  Unless there were none,
    -- Then add a dummy where clause, that way we can append further checks
    -- without having to do extra accounting.
    where_conditions = substring(inddef from ' WHERE.*');
    where_conditions = CASE WHEN where_conditions IS NULL THEN ' WHERE 1=1' ELSE where_conditions END;
    
    -- Construct the query that is the core of the check.
    -- The subquery creates a result set with two columns, an anonymous composite
    -- of the current rows intex keys as cur, and the same thing for the previous
    -- row as lag.
    -- The outer query makes sure that they are in proper order.
    --
    -- Because the subquery mirrors the index definition so closely, and because
    -- we are going to disable the sorting option for this session, the planner
    -- will almost certainly fufill the query with an index scan of the index
    -- in question (we'll check lower that it does).  The index scan gives us
    -- rows in index order, and through use of a window function, we can check
    -- if index order actually matches the ordering of the current machine.
    scan_query = 'SELECT cur, lag FROM (' ||
                     'SELECT ' || column_list || ' as cur, ' ||
                             'lag (' || column_list || ') OVER (ORDER BY ' || sort_list || ')' ||
                     'FROM ' || table_of_index || where_conditions || null_checks(sort_list)||
                 ') f WHERE cur ' || comparison_operator(ind.indisunique) || ' lag';
    RAISE INFO '%', scan_query;
    

    SET enable_sort TO FALSE;
    IF NOT plan_scans_index(scan_query, index_name) THEN
	-- The above function logs the warning because it has enough information to do so.
        RETURN;
    END IF;

    valid = true;
    FOR error_entry IN EXECUTE scan_query  LOOP
        RAISE WARNING '(%) found bad entry %', index_name, error_entry;
        bad_entry_count = bad_entry_count + 1;
	valid = false;
    END LOOP;
    
    skipped = false;
    RETURN;
END;
$$ LANGUAGE plpgsql;


/**
 * Finds all the indexes that have collated keys and check them for integrity.
 */
CREATE FUNCTION check_integrity(out total int, out skipped int, out invalid int, out bad_records int) RETURNS RECORD AS $$
DECLARE
    idx regclass;
    test_results RECORD;
BEGIN
    total = 0; skipped = 0; invalid = 0; bad_records = 0;
    FOR idx IN SELECT indexrelid::regclass FROM pg_index WHERE 0 <> ANY (indcollation) ORDER BY pg_table_size(indrelid::regclass) ASC LOOP

        test_results = check_index(idx);
        total = total + 1;
	IF test_results.skipped THEN
	    skipped = skipped + 1;
	ELSIF NOT test_results.valid THEN
	    invalid = invalid + 1;
	    bad_records = test_results.bad_entry_count;
	END IF;

	RAISE INFO 'Current Progress { total : %, skipped %, invalid %, bad_records % }', total, skipped, invalid, bad_records;

    END LOOP;

    RETURN;
END;
$$ LANGUAGE plpgsql;
