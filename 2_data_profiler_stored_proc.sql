--This is the primary data profiler procedure that will capture measures for passed in table
--It collects all the measures for each column and merges those into the DATA_PROFILE table
--
create or replace procedure DATA_PROFILER  (FULL_TABLE_PATH VARCHAR, TABLE_FILTER VARCHAR, FLOAT_TO_NUMBER_PRECISION VARCHAR, RUN_ID NUMBER,JOB_NAME VARCHAR )
returns VARCHAR(16777216)
language SQL
execute as caller
as $$
DECLARE
    v_database          VARCHAR := UPPER(split_part(FULL_TABLE_PATH,'.',1));
    v_schema            VARCHAR := UPPER(split_part(FULL_TABLE_PATH,'.',2));
    v_table_name        VARCHAR := UPPER(split_part(FULL_TABLE_PATH,'.',3));
    rs_meta             VARCHAR;
    rs_sql              VARCHAR;
    sqr                 VARCHAR;
    meta_table          VARCHAR;
    v_returnValue       VARCHAR DEFAULT '';
    v_v_profileScope    VARCHAR DEFAULT '';
    v_colCount          NUMBER DEFAULT 0;
    v_run_id            VARCHAR;
    run_id_num          NUMBER;
    merge_tbl_exists INT     := 0;

    exception_notenoughinputs   EXCEPTION (-20001,   'User Error: Unable to fully parse table name. Specify full path: db.schema.table');
    profile_opt         INT;
    v_eval_cols_p_tbl   INT DEFAULT 1;
    str_temp_view       varchar;

    c_profile CURSOR for SELECT
                            table_database, 
                            table_schema,
                            table_name,     
                            column_name,
                            data_type,      
                            n_cols
                        FROM TEMP_FILTERED_PROFILE ORDER BY table_database,table_schema,table_name,n_cols;
BEGIN
FLOAT_TO_NUMBER_PRECISION:= TO_NUMBER(NVL(FLOAT_TO_NUMBER_PRECISION,3));  --If not specified, default to 3 digits of precision

/*                             
# Parameters: 		                                                                			
#              full_table_path:           Represents the full path db.schema.table to be profiled        
#       
#              table_filter:              Optional where clause parameter that if supplied will create a
#                                         view that is a subset of the rows. Ignored if NULL is passed.
#
#              float_to_number_precision: Optional parameter that will round floats to numeric 
#                                         value on the HASH_AGG function.  Ignored if NULL is passed.                                    	          
#				                                                                            	          
#              run_id:                    Optional ID Number that that identifies the RUN_ID for this table. If included, 
#                                         this ID will be assigned to the associated records in the data_profile table.  
#                                         If NULL, a sequence generator is used for id number.
*/

     IF (v_database = '' or v_schema = '' or v_table_name= '')   THEN
        RAISE exception_notenoughinputs;
     END IF;
    --Table level analysis
    sqr := 'SELECT COUNT(1) FROM '|| v_database ||'.' || v_schema || '.' || v_table_name;
    EXECUTE IMMEDIATE sqr; --perform simple count to see if table exists. this will throw error if table is not found...                              

    
    --Check if RUN_ID was supplied, if not grab a new one
    IF (NVL(RUN_ID,0) <> 0) THEN
        run_id_num:= RUN_ID;        
    ELSE
        SELECT run_id_seq.nextval INTO :run_id_num; --get new RUN_ID so we don't step on top of other  runs usnig same table name
    END IF;
    v_run_id := :run_id_num;;

   --Grab the column names that will be profiled and store in temp table
    rs_sql := '
    CREATE OR REPLACE TEMPORARY TABLE TEMP_DATA_PROFILE AS
    SELECT CLMN.TABLE_CATALOG TABLE_DATABASE,
            CLMN.TABLE_SCHEMA ,
            CLMN.TABLE_NAME,
            CLMN.COLUMN_NAME,
            CLMN.ORDINAL_POSITION,
            CLMN.COLUMN_DEFAULT,
            CLMN.IS_NULLABLE,
            CLMN.DATA_TYPE,
            ''YES'' as IS_ACTIVE,
            '|| v_run_id || ' AS RUN_ID
    FROM ' || v_database || '.INFORMATION_SCHEMA.COLUMNS CLMN JOIN  ' || v_database || '.INFORMATION_SCHEMA.TABLES TABS
    ON  CLMN.TABLE_CATALOG=TABS.TABLE_CATALOG
    AND CLMN.TABLE_SCHEMA=TABS.TABLE_SCHEMA
    AND CLMN.TABLE_NAME=TABS.TABLE_NAME 
    WHERE
    TABS.TABLE_SCHEMA NOT IN (''INFORMATION_SCHEMA'')
    AND TABS.TABLE_SCHEMA =  ''' || v_schema || '''
        --Filter to only select tables
    AND TABS.TABLE_TYPE like (''%TABLE%'')
    -- Filter by Table Name
    and tabs.table_name= ''' || v_table_name || '''
    --Filter unsupported data types
    AND CLMN.DATA_TYPE NOT IN (''ARRAY'',''BINARY'',''GEOGRAPHY'',''OBJECT'')
    
    ';
    execute immediate rs_sql;      

    --Clean up any old runs
    rs_sql := '
    DELETE FROM DATA_PROFILE WHERE RUN_ID = '|| v_run_id ||' AND TABLE_DATABASE = ''' || v_database || '''
        AND TABLE_SCHEMA = ''' || v_schema || ''' and TABLE_NAME = ''' || v_table_name || '''
    ';
    execute immediate rs_sql; 

    --Add empty records back to the DATA_PROFILE table in prep of upcoming merge operation
    rs_sql := '
    INSERT INTO DATA_PROFILE(TABLE_DATABASE,TABLE_SCHEMA ,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,IS_ACTIVE,RUN_ID)            SELECT * FROM TEMP_DATA_PROFILE;
    ';
    execute immediate rs_sql; 

    --Create a driving table in which we will process all columns
    --You could perform slight refactor to build one large driving table and let the entire process run for all tables
    --depending on your use case.
    rs_sql := '
    CREATE OR REPLACE TEMPORARY TABLE TEMP_FILTERED_PROFILE AS 
         SELECT 
            data_profile.table_database,
            data_profile.table_schema,
            data_profile.table_name,
            data_profile.RUN_ID,
            data_profile.column_name,
            data_profile.data_type,
            tbl.n_cols
     FROM TEMP_DATA_PROFILE data_profile
     JOIN 
         (SELECT table_database,table_schema,table_name,run_id,count(column_name) as n_cols
           FROM 
               TEMP_DATA_PROFILE
           WHERE 
               IS_ACTIVE = ''YES''
           GROUP BY table_database,table_schema,table_name,run_id) tbl
           ON
                data_profile.table_database = tbl.table_database AND
                data_profile.table_schema   = tbl.table_schema   AND
                data_profile.table_name     = tbl.table_name     AND
                data_profile.RUN_ID         = tbl.RUN_ID
    ';
    --return rs_sql;
    execute immediate rs_sql; 

    --Create temp table that will store all intermediate results from the profiling.
    --This table will be merged into DATA_PROFILE once complete
    meta_table:= 'create or replace TEMPORARY TABLE TEMP_INSERT_METADATA (
                    TABLE_NAME VARCHAR(16777216),
                    COLNAME VARCHAR(16777216),
                    MINIMUM_VALUE VARIANT,
                    MAXIMUM_VALUE VARIANT,
                    TOTAL_TBL_ROW_COUNT VARIANT,
                    DISTINCT_VALUES_COUNT VARIANT,
                    DISTINCT_VALUES_PERCENT VARIANT,
                    NULL_COUNT VARIANT,
                    NULL_COUNT_PERCENT VARIANT,
                    MEDIAN_VALUE VARIANT,
                    AVERAGE_VALUE VARIANT,
                    HASH_AGG VARIANT,
                    TABLE_SCHEMA VARCHAR(16777216),
                    TABLE_DATABASE VARCHAR(16777216)
                )';
    EXECUTE IMMEDIATE meta_table;
   
    --Loop through each column to be profiled
    FOR record  IN  c_profile   DO
       v_colCount := v_colCount + 1;
       let db_name      VARCHAR := record.table_database;
       let schma_name   VARCHAR := record.table_schema;
       let tbl_name     VARCHAR := record.table_name;
       let column_name  VARCHAR := record.column_name;
       let col_data_typ VARCHAR := record.data_type;
       let n_cols       INT     := record.n_cols;

    --JSON approach used so that all operations can be sent in one bulk operation
        IF (v_eval_cols_p_tbl = 1)   THEN
        -- if this is the first column of the table concatenate the select
            rs_meta := 'SELECT PARSE_JSON('||''''||'{"TABLES":{"'||tbl_name||'":{ \n';
        END IF;

        --Getting general metrics
        let count_1         INT DEFAULT 0;
        let count_distinct  INT     DEFAULT 0;
        let count_if_null   INT     DEFAULT 0;
        let max_val           VARCHAR DEFAULT '0';
        let min_val           VARCHAR DEFAULT '0';
        let str_metadata_get  VARCHAR;
        let str_temp_view     VARCHAR;
        let query               resultset;
      
        let full_def_name    VARCHAR DEFAULT db_name ||'.'|| schma_name ||'.'|| tbl_name;
        IF (NVL(TABLE_FILTER,'') <> '') THEN
            str_temp_view := 'CREATE OR REPLACE TEMPORARY VIEW ' || v_table_name || '_TEMP AS SELECT * FROM ' || full_def_name || ' ' || TABLE_FILTER;
            execute immediate str_temp_view;
            full_def_name  := v_table_name || '_TEMP';
        END IF;

        -- get null counts from metadata partition
        str_metadata_get := 'SELECT (count(*)-COUNT( '|| column_name ||')) as res,
                                                                  count(1) as resb
                                FROM    ' || full_def_name;
        query := (execute immediate str_metadata_get);
        
        let c2 cursor for query;
        FOR rec  IN  c2   DO
            count_if_null := rec.res;
            count_1       := rec.resb;
        END FOR;

        
        -- Apply profiling if data type is Number 
        IF(col_data_typ IN ('NUMBER') ) THEN
            let avg_val_get     INT DEFAULT 0;
            let median_val_get  INT DEFAULT 0;
            
            -- get min and max val from metadata partition
            str_metadata_get := 'SELECT NVL(MIN( '|| column_name ||'),''0'') as min,
                                        NVL(MAX( '|| column_name ||'),''0'') as max FROM    ' || full_def_name ;
            query := (execute immediate str_metadata_get);
            let c3 cursor for query;
            FOR rec  IN  c3   DO
                min_val := rec.min;
                max_val := rec.max;
            END FOR;
            rs_meta := rs_meta ||
                        '"' || column_name || '"' || ':[{' ||
                        '"TOTAL_TBL_ROW_COUNT":"'   || '''|| '|| count_1 ||' || '''|| '" , ' ||
                        '"DISTINCT_VALUES_COUNT":"'  || '''|| '|| 'NVL(COUNT (DISTINCT '|| column_name ||'),0)' ||' || '''|| '" , ' ||
                        '"DISTINCT_VALUES_PERCENT":"'|| '''|| NVL(DIV0('|| 'NVL( COUNT (DISTINCT '|| column_name ||'),0)' ||','||count_1||'),''0'') || '''|| '" , '    ||
                        '"NULL_COUNT":"'             || '''||'|| count_if_null || '|| '''|| '" , ' ||
                        '"NULL_COUNT_PERCENT":"'      || '''|| NVL(DIV0('''|| count_if_null || ''','''||count_1||'''),''0'') || '''|| '" ,' ||
                        '"MINIMUM_VALUE":"'          || '''|| '''|| min_val || ''' || '''|| '" ,'  ||
                        '"MAXIMUM_VALUE":"'          || '''|| '''|| max_val || ''' || '''|| '"' ||',' ;
            rs_meta := rs_meta ||
                        '"AVERAGE_VALUE":"'          || '''||NVL(TO_NUMBER(AVG('||column_name||'),38,10),0)' ||' || '''|| '" , ' ||
                        '"MEDIAN_VALUE":"'           || '''||NVL(TO_NUMBER(MEDIAN('||column_name||'),38,10),0)' ||' || '''|| '" , ';
            rs_meta := rs_meta ||  
                        '"HASH_AGG":"'  || '''|| '|| 'NVL(HASH_AGG ( '|| column_name ||'),0)' ||' || '''|| '" , ' ;
    
        ELSEIF (col_data_typ IN ('FLOAT') ) THEN
            let avg_val_get     INT DEFAULT 0;
            let median_val_get  INT DEFAULT 0;
            
            -- get min and max val from metadata partition
            str_metadata_get := 'SELECT NVL(MIN( '|| column_name ||'),''0'') as min,
                                        NVL(MAX( '|| column_name ||'),''0'') as max FROM    ' || full_def_name ;
            query := (execute immediate str_metadata_get);
            let c3 cursor for query;
            FOR rec  IN  c3   DO
                min_val := rec.min;
                max_val := rec.max;
            END FOR;
            --COUNT(DISTINCT(ROUND(IFF(METRIC_VALUE='NaN',0,METRIC_VALUE),2)))
            rs_meta := rs_meta ||
                        '"' || column_name || '"' || ':[{' ||
                        '"TOTAL_TBL_ROW_COUNT":"'   || '''|| '|| count_1 ||' || '''|| '" , ' ||
                        '"DISTINCT_VALUES_COUNT":"'  || '''|| '|| 'COUNT(DISTINCT(ROUND(IFF('|| column_name ||'=''NaN'',0,'|| column_name || '),'|| FLOAT_TO_NUMBER_PRECISION || ')))' ||' || '''|| '" , ' ||
                        '"DISTINCT_VALUES_PERCENT":"'|| '''|| NVL(ROUND(DIV0('|| 'COUNT(DISTINCT(ROUND(IFF('|| column_name ||'=''NaN'',0,'|| column_name || '),'|| FLOAT_TO_NUMBER_PRECISION || ')))' ||','||count_1||'),'|| FLOAT_TO_NUMBER_PRECISION || '),''0'') || '''|| '" , '    ||
                        '"NULL_COUNT":"'             || '''||'|| count_if_null || '|| '''|| '" , ' ||
                        '"NULL_COUNT_PERCENT":"'      || '''|| NVL(DIV0('''|| count_if_null || ''','''||count_1||'''),''0'') || '''|| '" ,' ||
                        '"MINIMUM_VALUE":"'          || '''|| '''|| min_val || ''' || '''|| '" ,'  ||
                        '"MAXIMUM_VALUE":"'          || '''|| '''|| max_val || ''' || '''|| '"' ||',' ;
            rs_meta := rs_meta ||
                        '"AVERAGE_VALUE":"'          || '''||NVL(ROUND(AVG(ROUND(IFF('|| column_name ||'=''NaN'',0,'|| column_name || '),'|| FLOAT_TO_NUMBER_PRECISION || ')),'||  FLOAT_TO_NUMBER_PRECISION   ||'),0)' ||' || '''|| '" , ' ||
                        '"MEDIAN_VALUE":"'           || '''||NVL(MEDIAN(ROUND(IFF('|| column_name ||'=''NaN'',0,'|| column_name || '),'|| FLOAT_TO_NUMBER_PRECISION || ')),0)' ||' || '''|| '" , ';
            rs_meta := rs_meta ||  
                        '"HASH_AGG":"'  || '''|| '|| 'NVL(HASH_AGG ( ROUND(IFF('|| column_name ||'=''NaN'',0,'|| column_name || '),'|| FLOAT_TO_NUMBER_PRECISION || ')'||'),0)' ||' || '''|| '" , ' ;

        ELSE
            -- Apply profiling for all other non numeric cols
            rs_meta := rs_meta ||
                        '"' || column_name || '"' || ':[{' ||
                        '"TOTAL_TBL_ROW_COUNT":"'   || '''|| '|| count_1 ||' || '''|| '" , ' ||
                        '"DISTINCT_VALUES_COUNT":"'  || '''|| '|| 'NVL(COUNT (DISTINCT '|| column_name ||'),''0'')' ||' || '''|| '" , ' ||
                        '"DISTINCT_VALUES_PERCENT":"'|| '''|| NVL(DIV0('|| 'NVL(COUNT (DISTINCT '|| column_name ||'),''0'')' ||','||count_1||'),''0'') || '''|| '" , '    ||
                        '"NULL_COUNT":"'             || '''||'|| count_if_null || '|| '''|| '" , ' ||
                        '"NULL_COUNT_PERCENT":"'      || '''|| NVL(DIV0('''|| count_if_null || ''','''||count_1||'''),''0'') || '''|| '" ,' ||
                        '"MINIMUM_VALUE":"'          || '''|| '|| 'NVL(REPLACE(REPLACE(TO_CHAR(MIN( '|| column_name ||')),''\\n'','' ''),''"'',''\\\\"''),'''')' || ' || '''|| '" ,'  ||
                        '"MAXIMUM_VALUE":"'          || '''|| '|| 'NVL(REPLACE(REPLACE(TO_CHAR(MAX( '|| column_name ||')),''\\n'','' ''),''"'',''\\\\"''),'''')' || ' || '''|| '"' || ',
                        "AVERAGE_VALUE":"0",
                        "MEDIAN_VALUE":"0",';
            rs_meta := rs_meta || 
                        '"HASH_AGG":"'  || '''|| '|| 'NVL(HASH_AGG ( '|| column_name ||'),0)' ||' || '''|| '" , ' ;
        END IF;


        -- end array col metadata for next column
        rs_meta := rs_meta || '}] \n';
        -- If final col of the tbl adds the from clause, if not a comma for next column
        IF (v_eval_cols_p_tbl = n_cols) THEN
            rs_meta := rs_meta ||
                        '}}}'''||') src from ' || full_def_name;
            v_eval_cols_p_tbl := 1;

                --Insert new data into table
            rs_meta := 'INSERT INTO TEMP_INSERT_METADATA
                    (select p.*
                    ,'''||schma_name||''' as TABLE_SCHEMA
                    ,'''||db_name||'''    as TABLE_DATABASE
                                    from
                                        (
                                        with t as
                                        (' || rs_meta || ')
                                        select a.key as table_name,b.key as column_name,c.key as metricname , c.value
                                        from t,
                                        lateral flatten(src:"TABLES") a
                                        ,lateral flatten(a.value) b
                                        ,lateral flatten(b.value[0]) c
                                        ) aux
                                        pivot(max(value) for
                                                metricname in (''MINIMUM_VALUE'',''MAXIMUM_VALUE'',''TOTAL_TBL_ROW_COUNT'',
                                                ''DISTINCT_VALUES_COUNT'',''DISTINCT_VALUES_PERCENT'',
                                                ''NULL_COUNT'',''NULL_COUNT_PERCENT'',''MEDIAN_VALUE'',''AVERAGE_VALUE'',''HASH_AGG'')
                                            )
                                    as p(table_name,colname,MINIMUM_VALUE,MAXIMUM_VALUE,TOTAL_TBL_ROW_COUNT,DISTINCT_VALUES_COUNT,
                                        DISTINCT_VALUES_PERCENT,NULL_COUNT,NULL_COUNT_PERCENT,MEDIAN_VALUE,AVERAGE_VALUE,HASH_AGG)
                                        )';
 
            --return (rs_meta);
            execute immediate rs_meta;
       ELSE
            -- Add next column of the table column iteration
            v_eval_cols_p_tbl := v_eval_cols_p_tbl + 1;
            rs_meta := rs_meta || ',' ;
       END IF;
    END FOR;
    
/*
#####################################################################################################################
#   Merge into  DATA_PROFILE table with the data collected and profiling details                                      #
#####################################################################################################################
*/
            --Assemble merge statement directly

            rs_meta := 'merge into DATA_PROFILE a using
                            TEMP_INSERT_METADATA t
                                on
                                a.TABLE_SCHEMA              = t.TABLE_SCHEMA
                                    AND a.TABLE_NAME        = t.table_name
                                    AND a.TABLE_DATABASE    = t.TABLE_DATABASE
                                    AND a.COLUMN_NAME       = t.colname
                                    AND ' || :v_run_id || '
                                when matched then
                                update
                                    set
                                        a.MINIMUM_VALUE             = t.MINIMUM_VALUE,
                                        a.MAXIMUM_VALUE             = t.MAXIMUM_VALUE,
                                        a.DISTINCT_VALUES_COUNT     = t.DISTINCT_VALUES_COUNT,
                                        a.DISTINCT_VALUES_PERCENT   = t.DISTINCT_VALUES_PERCENT,
                                        a.NULL_COUNT                = t.NULL_COUNT,
                                        a.NULL_COUNT_PERCENT        = t.NULL_COUNT_PERCENT,
                                        a.TOTAL_TBL_ROW_COUNT       = t.TOTAL_TBL_ROW_COUNT,
                                        a.AVERAGE_VALUE             = t.AVERAGE_VALUE,
                                        a.HASH_AGG                = t.HASH_AGG,
                                        a.MEDIAN_VALUE              = t.MEDIAN_VALUE,
                                        a.JOB_NAME        = ''' || NVL(JOB_NAME,'') || ''',
                                        a.UPDATE_DATE               = CURRENT_DATE(),
                                        a.TABLE_FILTER              = '''||  REPLACE(NVL(:TABLE_FILTER,''), '\'','\\\'') || ''',
                                        a.FLOAT_PRECISION           = '|| FLOAT_TO_NUMBER_PRECISION|| ',
                                        a.UPDATE_TS                 = CURRENT_TIMESTAMP()';
            --return (rs_meta);
            execute immediate rs_meta;

     return('Data profiling completed: ' || :full_table_path || ', ' || v_colCount || ' columns, RUN_ID: ' || run_id_num );
END
$$;

--call DATA_PROFILER  ('PROD_DB.PUBLIC.TABLE1',NULL, '6', NULL,'JOB1_NAME');

