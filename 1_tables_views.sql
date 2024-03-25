
--CREATE Database/Schema to store data validation objects/results
CREATE DATABASE IF NOT EXISTS VALIDATION_DB;
USE DATABASE validation_db;  --Customize to your DB
CREATE SCHEMA IF NOT EXISTS DATA_VALIDATION; --Customize to your Schema
use schema DATA_VALIDATION; --Customize to your Schema

--sequence generator used to create unique run_ids
--this is optional but allows the framework to auto number each run_id.
--The framework uses the same run_id to compare your old dataset to your new dataset.
create or replace sequence run_id_seq;

--Main table used to track a profile for each column in each table
--Metadata from each run/table/column will be stored in this table
create or replace TABLE DATA_PROFILE (
	RUN_ID NUMBER(38,0),
	TABLE_DATABASE VARCHAR(16777216),
	TABLE_SCHEMA VARCHAR(16777216),
	TABLE_NAME VARCHAR(16777216),
	COLUMN_NAME VARCHAR(16777216),
	ORDINAL_POSITION NUMBER(38,0),
	COLUMN_DEFAULT VARCHAR(16777216),
	IS_NULLABLE VARCHAR(3),
	DATA_TYPE VARCHAR(16777216),
	MINIMUM_VALUE VARCHAR(16777216),
	MAXIMUM_VALUE VARCHAR(16777216),
	AVERAGE_VALUE NUMBER(38,10),
	MEDIAN_VALUE NUMBER(38,10),
	TOTAL_TBL_ROW_COUNT NUMBER(38,0),
	DISTINCT_VALUES_COUNT NUMBER(38,0),
	DISTINCT_VALUES_PERCENT NUMBER(38,10),
	NULL_COUNT NUMBER(38,0),
	NULL_COUNT_PERCENT NUMBER(38,10),
	IS_ACTIVE VARCHAR(8),
	UPDATE_DATE DATE,
	UPDATE_TS TIMESTAMP_LTZ(9),
	HASH_AGG VARCHAR(16777216),
	JOB_NAME VARCHAR(16777216),  
	TABLE_FILTER VARCHAR(16777216),
	FLOAT_PRECISION NUMBER(38,0)
);

--These views allow you to identify by database where each table will live.
--Typically QA indicates your Snowpark Pipelines where PROD indicates your existing spark Pipeline
--If you store all tables in the same db/schema and want to use a prefix/suffix, change the views below
--so the correct tables appear
create or replace view qa_tables_vw as select * from data_profile where table_database = 'QA_DB';
create or replace view prod_tables_vw as select * from data_profile where table_database != 'QA_DB';

--This is the primary view that compares the columns and identifies measures that do not match
create or replace view column_compare_vw as
select 
  QA.COLUMN_NAME COLUMN_NAME,
  QA.data_type,
  qa.run_id run_id,
  qa.total_tbl_row_count qa_row_count,
  prod.total_tbl_row_count prod_row_count,
  iff(qa.column_default<>NVL(prod.column_default,'MISSING'),qa.column_default || ' <> ' || NVL(prod.column_default,'MISSING') ,' ') column_default_test,
  iff(qa.ordinal_position<>prod.ordinal_position,qa.ordinal_position || ' <> ' || prod.ordinal_position ,' ') ordinal_position_test,
  iff(qa.is_nullable<>prod.is_nullable,qa.is_nullable || ' <> ' || prod.is_nullable ,' ') is_nullable_test, 
  iff(qa.data_type<>prod.data_type,qa.data_type || ' <> ' || prod.data_type ,' ') data_type_test, 
  iff(qa.minimum_value<>prod.minimum_value,qa.minimum_value || ' <> ' || prod.minimum_value ,' ') minimum_value_test,  
  iff(qa.MAXIMUM_VALUE<>prod.MAXIMUM_VALUE,qa.MAXIMUM_VALUE || ' <> ' || prod.MAXIMUM_VALUE ,' ') MAXIMUM_VALUE_test,   
  iff(qa.AVERAGE_VALUE<>prod.AVERAGE_VALUE,qa.AVERAGE_VALUE || ' <> ' || prod.AVERAGE_VALUE ,' ') AVERAGE_VALUE_test, 
  iff(try_to_number(prod.AVERAGE_VALUE::NUMBER(38,6),38,6)=0,0,
  100-(DIV0(qa.AVERAGE_VALUE::NUMBER(38,6),prod.AVERAGE_VALUE::NUMBER(38,6))) * 100 ) AVERAGE_VALUE_PCT_VARIANCE,
  iff(qa.MEDIAN_VALUE<>prod.MEDIAN_VALUE,qa.MEDIAN_VALUE || ' <> ' || prod.MEDIAN_VALUE ,' ') MEDIAN_VALUE_test,   
    iff(try_to_number(prod.MEDIAN_VALUE::NUMBER(38,6),38,6)=0,0,
  100-(DIV0(qa.MEDIAN_VALUE::NUMBER(38,6),prod.MEDIAN_VALUE::NUMBER(38,6))) * 100 ) MEDIAN_VALUE_PCT_VARIANCE,
  iff(qa.total_tbl_row_count<>NVL(prod.total_tbl_row_count,-1),qa.total_tbl_row_count || ' <> ' || NVL(prod.total_tbl_row_count,-1) ,' ') total_tbl_row_count_test,
  iff(qa.DISTINCT_VALUES_COUNT<>NVL(prod.DISTINCT_VALUES_COUNT,-1),qa.DISTINCT_VALUES_COUNT || ' <> ' || NVL(prod.DISTINCT_VALUES_COUNT,-1) ,' ') DISTINCT_VALUES_COUNT_test,  
    iff(try_to_number(prod.DISTINCT_VALUES_COUNT::NUMBER(38,6),38,6)=0,0,
  100-(DIV0(qa.DISTINCT_VALUES_COUNT::NUMBER(38,6),prod.DISTINCT_VALUES_COUNT::NUMBER(38,6))) * 100 ) DISTINCT_VALUES_PCT_VARIANCE,
  iff(qa.DISTINCT_VALUES_PERCENT<>prod.DISTINCT_VALUES_PERCENT,qa.DISTINCT_VALUES_PERCENT || ' <> ' || prod.DISTINCT_VALUES_PERCENT ,' ') DISTINCT_VALUES_PERCENT_test, 
  iff(qa.NULL_COUNT<>prod.NULL_COUNT,qa.NULL_COUNT || ' <> ' || prod.NULL_COUNT ,' ') NULL_COUNT_test, 
  iff(qa.NULL_COUNT_PERCENT<>prod.NULL_COUNT_PERCENT,qa.NULL_COUNT_PERCENT || ' <> ' || prod.NULL_COUNT_PERCENT ,' ') NULL_COUNT_PERCENT_test, 
  iff(qa.HASH_AGG<>NVL(prod.HASH_AGG,'MISSING'),qa.HASH_AGG || ' <> ' || NVL(prod.HASH_AGG,'MISSING') ,' ') HASH_AGG_test, 
  iff(column_default_test=' ',0,1) + 
  iff(ordinal_position_test=' ',0,1) + 
  iff(is_nullable_test=' ',0,1) 
  + iff(data_type_test=' ',0,1)+ iff(minimum_value_test=' ',0,1)+ iff(MAXIMUM_VALUE_test=' ',0,1)  
  + iff(AVERAGE_VALUE_test=' ',0,1) + iff(MEDIAN_VALUE_test=' ',0,1)+ iff(total_tbl_row_count_test=' ',0,1)
  + iff(DISTINCT_VALUES_COUNT_test=' ',0,1)+ iff(DISTINCT_VALUES_PERCENT_test=' ',0,1) + iff(NULL_COUNT_test=' ',0,1) 
  + iff(NULL_COUNT_PERCENT_test=' ',0,1) + iff(HASH_AGG_test=' ',0,1)   
  num_measures_failed,
  QA.TABLE_DATABASE QA_DATABASE_NAME,
  QA.TABLE_SCHEMA QA_SCHEMA_NAME,
  QA.TABLE_NAME QA_TABLE_NAME,
  QA.UPDATE_TS QA_PROFILE_TIMESTAMP,
  prod.table_database PROD_DATABASE,
  prod.table_schema PROD_SCHEMA,
  PROD.TABLE_NAME PROD_TABLE_NAME,
  PROD.UPDATE_TS PROD_PROFILE_TIMESTAMP,
    QA.JOB_NAME,
  prod.float_precision as prod_float_precision,
  prod.table_filter as prod_table_filter
 from qa_tables_vw qa  
  LEFT OUTER join prod_tables_vw prod
  on qa.run_id = prod.run_id AND
     qa.column_name = prod.column_name
where  qa.is_active = 'YES'
;

--This view provides a summary for each table, including overall score and metadata about the job used
--
CREATE OR REPLACE VIEW TABLE_SUMMARY_VW AS
select JOB_NAME,
qa_database_name,qa_schema_name,prod_table_name,run_id,sum(1) as num_columns_checked,
SUM(IFF(num_measures_failed>0,1,0)) num_columns_failed,
sum(13) as num_measures_checked, 
sum(num_measures_failed) as num_measures_failed,
timestampdiff(seconds,min(qa_profile_timestamp),max(prod_profile_timestamp) ) prod_profile_gen_time_in_secs ,
min(prod_row_count) as prod_row_count,
(sum(num_measures_failed)/sum(13))*100 PERECENT_FAILED,
iff(SUM(nvl(qa_row_count,0))=0,-1,100-(sum(num_measures_failed)/sum(13)*100)) percent_PASSED,
max(qa_PROFILE_TIMESTAMP) QA_PROFILE_TIMESTAMP,
max(PROD_PROFILE_TIMESTAMP) PROD_PROFILE_TIMESTAMP,
max(prod_float_precision) as prod_float_precision,
max(prod_table_filter) as prod_table_filter,
max(prod_database) || '.' || max(prod_schema) || '.' || max(prod_table_name) as prod_table_path
  from column_compare_vw
  group by all order by run_id;


