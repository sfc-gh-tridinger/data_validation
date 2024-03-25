
--
--The Code below is a dashboard that can help view each test that was executed.



--DASHBOARDS SQL 
--Code below is for setting up a new filter for your new dashboard
--add new filter :RUN_ID
 select distinct(run_id) from TABLE_SUMMARY_VW   order by run_id

-------------------------------------------------------------------------------------------------------------------
--          top row "Information about this Test"                                                              -
-------------------------------------------------------------------------------------------------------------------
-- row 2 left "Percent Passed" | row 2 right "# of Measures Failed Per Column (hover to view) (3)"                -
-------------------------------------------------------------------------------------------------------------------
--                  row 3 summary "All Dataset Measures (Summary)"                                                -
--

--top row "Information about this test" (1)
select 'TPS DATASET NAME' as "ATTRIBUTE", JOB_NAME AS VALUE, ' ' DATE_PROFILED from table_summary_vw where RUN_ID = :RUN_ID
UNION ALL
select 'QA(SNOWPARK) TABLE PATH' AS "ATTRIBUTE", qa_database_name||'.'||qa_schema_name||'.'|| QA_table_name AS VALUE, QA_PROFILE_TIMESTAMP::VARCHAR DATE_PROFILED from table_summary_vw where RUN_ID = :RUN_ID
UNION ALL
select 'PROD(PYSPARK) TABLE PATH' AS "ATTRIBUTE", prod_database_NAME || '.' || prod_schema_NAME ||'.'|| prod_table_name AS VALUE, PROD_PROFILE_TIMESTAMP::varchar DATE_PROFILED from table_summary_vw where RUN_ID = :RUN_ID
UNION ALL
select 'TABLE FILTER' AS "ATTRIBUTE", PROD_TABLE_FILTER AS VALUE, PROD_PROFILE_TIMESTAMP::varchar DATE_PROFILED from table_summary_vw where RUN_ID = :RUN_ID
UNION ALL
select 'FLOAT ROUNDING PRECISION' AS "ATTRIBUTE", PROD_FLOAT_PRECISION::VARCHAR AS VALUE, PROD_PROFILE_TIMESTAMP::varchar DATE_PROFILED from table_summary_vw where RUN_ID = :RUN_ID

--row 2  left "Percent Passed (2 left)"
select percent_passed from table_summary_vw where RUN_ID = :RUN_ID

--row 2  right "# of Measures Failed Per Column (hover to view) (right)"
select column_name,num_measures_failed from column_compare_vw where RUN_ID = :RUN_ID and num_measures_failed > 0

--row 3 summary "All Dataset Measures (Summary)"
select 	PERCENT_PASSED,
    NUM_COLUMNS_CHECKED,
	NUM_COLUMNS_FAILED,
	NUM_MEASURES_CHECKED,
	NUM_MEASURES_FAILED,
	PROD_PROFILE_GEN_TIME_IN_SECS,
	PROD_ROW_COUNT
    from table_summary_vw where RUN_ID = :RUN_ID

--row 4 -measures that failed "Column Measures that Failed QA vs PROD [<> indicates values that don't match the given test]"
select 	COLUMN_NAME,
	DATA_TYPE,
    NUM_MEASURES_FAILED,
	TOTAL_TBL_ROW_COUNT_TEST ROW_COUNT_TEST,
	MINIMUM_VALUE_TEST,
	MAXIMUM_VALUE_TEST,
	AVERAGE_VALUE_TEST,
	MEDIAN_VALUE_TEST,
	DISTINCT_VALUES_COUNT_TEST,
	DISTINCT_VALUES_PERCENT_TEST,
	NULL_COUNT_TEST,
	NULL_COUNT_PERCENT_TEST,
	HASH_AGG_TEST,
    COLUMN_DEFAULT_TEST,
	IS_NULLABLE_TEST,
	DATA_TYPE_TEST,
    AVERAGE_VALUE_PCT_VARIANCE,
    MEDIAN_VALUE_PCT_VARIANCE,
    DISTINCT_VALUES_PCT_VARIANCE,
	PROD_PROFILE_TIMESTAMP
    from column_compare_vw where RUN_ID = :RUN_ID and num_measures_failed > 0
    ORDER BY COLUMN_NAME

--row 5 - Raw Profile Data QA and PROD
select * from data_profile where RUN_ID = :RUN_ID order by run_id,column_name,TABLE_SCHEMA

-------------------

