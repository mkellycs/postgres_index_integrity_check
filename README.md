Streaming Replica Collated Index Integrity Checker
==============================

This utility grew out of the following discussion and the need to have a tool to test for the problem.
http://www.postgresql.org/message-id/flat/BA6132ED-1F6B-4A0B-AC22-81278F5AB81E@tripadvisor.com

This utility is clearly a smoke test.  It doesn't catch all of the cases, but it should catch the most frequent ones.

We wanted something that felt safe to run on production systems.  This utility accomplishes its job by constucting a targeted SELECT statment to validate each index.  The code should be straight forward enough for most to follow.  The main interesting stuff is in ```ta_check_index```.  The test certainly creates additional load but it should not be any worse than any other long running query.  (It effectively, scans the leaf nodes of each index, and then must refer to the table to check each tuple).

## Usage
You will need to add the functions to the master before you can check the slaves.
```
# Before
psql -f create_functions.sql
# When you are done
psql -f drop_functions.sql
```

Then on any slave you want to check:
```
-- Checks a single index
SELECT * FROM check_index('idx_cms_display_template_id')
-- Checks all indexes where one or more of the keys has a collation (is a text key)
-- Does so from smallest table to largest that way it can start giving immediate feedback.
SELECT * FROM ta_check_collated_index_integrity();
```

If the current master was ever a slave then you will want to check it as well.
