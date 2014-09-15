DROP FUNCTION ta_plan_scans_index (query text, index_name text);
DROP FUNCTION ta_idx_comp_oper (is_unique boolean);
DROP FUNCTION ta_null_checks(col_list text);
DROP FUNCTION ta_check_index(index_reg regclass, out skipped boolean, out valid boolean, out bad_entry_count int);
DROP FUNCTION ta_check_collated_index_integrity();
