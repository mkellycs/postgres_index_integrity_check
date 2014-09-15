DROP FUNCTION plan_scans_index (query text, index_name text);
DROP FUNCTION comparison_operator (is_unique boolean);
DROP FUNCTION null_checks(col_list text);
DROP FUNCTION check_index(index_reg regclass, out skipped boolean, out valid boolean, out bad_entry_count int);
DROP FUNCTION check_integrity();
