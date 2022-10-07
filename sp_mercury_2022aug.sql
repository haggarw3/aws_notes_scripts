CREATE OR REPLACE PROCEDURE mercury_output_file_aug2022()
LANGUAGE plpgsql AS $$
DECLARE
	date_var timestamp(0);
--    set statement_timeout to 10000000;
--    commit;
BEGIN
	SELECT into date_var CURRENT_timestamp;
        
    drop table if exists mercury_oncall_temp_table;
    drop table if exists mercury_oncall_temp_table_2;
    
    create temporary table mercury_oncall_temp_table as
   
       SELECT
        
        -- root

        -- 1
        "root".Identifier AS INCIDENT_ID,
        
        -- 2
        CASE 
            WHEN "oncall_v2_curated"."final_dealer_response".accepted = 'true' 
            THEN 'T'
            WHEN "oncall_v2_curated"."final_dealer_response".accepted = 'false' 
            THEN 'F'
        ELSE 
            NULL 
        END 
        AS ACCEPTED_FLAG, 
        
        -- TODO: Mercury Team request 1.
        /*
        CASE 
            WHEN "oncall_v2_curated"."final_dealer_response".accepted = 'true' 
            THEN '1'
            WHEN "oncall_v2_curated"."final_dealer_response".accepted = 'false' 
            THEN '0'
        ELSE 
            NULL 
        END 
        AS ACCEPTED_EVENT_FLAG,
        */
        
        -- TODO: Mercury Team request 3.a
        -- 3
        CONCAT (
            EXTRACT( 
                year from to_timestamp(
                    "oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS'
                )
            ),    
            LPAD (
                EXTRACT (
                    month from to_timestamp(
                        "oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS'
                    )
                ), 2, '0'
            )
        )
        AS CALENDAR_CCYYMM_NBR,

        -- 4 
        "oncall_v2_curated"."assets".breakdown_city AS CITY,

        -- 5 
        "dev"."oncall_v2_curated"."invoice_products".attributes_tirecondition AS CONDITION_OF_TIRE,
        
        -- TODO: Mercury Team request 2.
        -- TODO: double check - in theory there is no bill_to for service providers
        -- 6
        "root".orderer_billto AS CUST_BT_CUST_NBR,
        
        -- 7
        "oncall_v2_curated"."assets".unit AS DEFECTIVE_UNIT,
        
        -- TODO: Mercury Team request 3.e
        -- 8 
        CASE 
            WHEN "oncall_v2_curated"."event_status_history".newstatus = 'DISPATCH' 
            --THEN extract(month FROM to_timestamp("oncall_v2_curated"."event_status_history".changedat, 'YYYY-MM-DD HH24:MI:SS'))
            THEN 
                CONCAT (
                    EXTRACT( 
                        year from to_timestamp(
                            "oncall_v2_curated"."event_status_history".changedat, 'YYYY-MM-DD HH24:MI:SS'
                        )
                    ),    
                    LPAD (
                        EXTRACT (
                            month from to_timestamp(
                                "oncall_v2_curated"."event_status_history".changedat, 'YYYY-MM-DD HH24:MI:SS'
                            )
                        ), 2, '0'
                    )
                )
        ELSE 
            NULL 
        END 
        AS DISPATCH_CCYYMM_NBR,

        -- 9
        CASE 
            WHEN "oncall_v2_curated"."event_status_history".newstatus = 'DISPATCH' 
            THEN split_part("dev"."oncall_v2_curated"."event_status_history".changedat, 'T', 1) 
        ELSE 
            NULL 
        END 
        AS DISPATCH_DATE,
        
        -- 10
        CASE 
            WHEN "oncall_v2_curated"."event_status_history".newstatus = 'DISPATCH' 
            THEN left(split_part("dev"."oncall_v2_curated"."event_status_history".changedat, 'T', 2),8) 
        ELSE 
            NULL 
        END 
        AS DISPATCH_TIME,

        -- TODO: Mercury Team request 3.c
        --extract(month FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS'))
        -- 11
        CONCAT (
            EXTRACT( 
                year from to_timestamp(
                    "oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS'
                )
            ),    
            LPAD (
                EXTRACT (
                    month from to_timestamp(
                        "oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS'
                    )
                ), 2, '0'
            )
        )
        AS DOWNTIME_CCYYMM_NBR,
        
        -- 12
        CASE 
            WHEN extract(dayofweek FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS')) = 0 
            THEN 'sunday'
            WHEN extract(dayofweek FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS')) = 1 
            THEN 'monday'
            WHEN extract(dayofweek FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS')) = 2 
            THEN 'tuesday'
            WHEN extract(dayofweek FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS')) = 3 
            THEN 'wednesday'
            WHEN extract(dayofweek FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS')) = 4 
            THEN 'thursday'
            WHEN extract(dayofweek FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS')) = 5 
            THEN 'friday'
            WHEN extract(dayofweek FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_downat", 'YYYY-MM-DD HH24:MI:SS')) = 6 
            THEN 'saturday'
        END 
        AS DOWNTIME_DAY_OF_WEEK,
        
        -- 13
        split_part("oncall_v2_curated"."root"."event_attributes_downat", 'T', 1) 
        AS "DOWNTIME_START_DATE", 
        
        -- 14
        left(split_part("oncall_v2_curated"."root"."event_attributes_downat", 'T', 2),8) 
        AS "DOWNTIME_START_TIME",

        -- 15
        CASE 
            WHEN "dev"."oncall_v2_curated"."orderer_person"."type" 
            LIKE '%DRIVER%' 
            THEN split_part("dev"."oncall_v2_curated"."orderer_person"."name", ' ', 1) 
        ELSE 
            NULL
        END 
        AS driver_first_name,
        
        -- 16
        CASE 
            WHEN "dev"."oncall_v2_curated"."orderer_person"."type" 
            LIKE '%DRIVER%' 
            THEN split_part("dev"."oncall_v2_curated"."orderer_person"."name", ' ', 2) 
        ELSE 
            NULL 
        END 
        AS "driver_last_name",
        
        -- 17
        CASE 
            WHEN "dev"."oncall_v2_curated"."orderer_person"."type" 
            LIKE '%DRIVER%' 
            THEN "dev"."oncall_v2_curated"."orderer_person"."phone" 
        ELSE 
            NULL 
        END 
        AS "driver_phone",
        
        -- 18
        CASE 
            WHEN "dev"."oncall_v2_curated"."orderer_person"."type" 
            LIKE '%DRIVER%'  
            THEN "dev"."oncall_v2_curated"."orderer_person"."type" 
        ELSE 
            NULL 
        END 
        AS "driver_phone_type",

        -- 19
        "oncall_v2_curated"."ch_inboundprogram".fields_store AS INBOUND_CALLER_STORE, 
       
        -- 20
        "oncall_v2_curated"."ch_inboundprogram".fields_name AS INBOUND_PROGRAM,
        
        -- 21
        "oncall_v2_curated"."assets".breakdown_latitude_double AS LATITUDE,
        
        -- 22
        "oncall_v2_curated"."assets".breakdown_longitude_double AS LONGITUDE,
        
        -- 23 
        "root".orderer_accountselected AS NATIONAL_ACCOUNT,

        -- 24
        CASE 
            WHEN "dev"."oncall_v2_curated"."final_dealer_response".accepted = false 
            THEN "dev"."oncall_v2_curated"."final_dealer_response".reason
        ELSE 
            NULL 
        END 
        AS REFUSAL_ACCEPTED_REASON,

        -- 25
        "dev"."oncall_v2_curated"."invoice_products".attributes_requestedaction AS REPLACEMENT_TIRE,
        
        -- 26
        "dev"."oncall_v2_curated"."invoice_products".attributes_rimtype AS RIM_TYPE,
        

        -- TODO: Mercury Team request 3.d
        -- extract(month FROM to_timestamp("oncall_v2_curated"."root"."event_attributes_rollingat", 'YYYY-MM-DD HH24:MI:SS'))
        -- 27
        CONCAT (
            EXTRACT( 
                year from to_timestamp(
                    "oncall_v2_curated"."root"."event_attributes_rollingat", 'YYYY-MM-DD HH24:MI:SS'
                )
            ),    
            LPAD (
                EXTRACT (
                    month from to_timestamp(
                        "oncall_v2_curated"."root"."event_attributes_rollingat", 'YYYY-MM-DD HH24:MI:SS'
                    )
                ), 2, '0'
            )
        )
        AS ROLL_CCYYMM_NBR,
        
        -- 28
        split_part("oncall_v2_curated"."root"."event_attributes_rollingat", 'T', 1) 
        AS "ROLL_DATE", 
        
        -- 29
        left(split_part("oncall_v2_curated"."root"."event_attributes_rollingat", 'T', 2),8) 
        AS "ROLL_TIME",

        -- 30
        "dev"."oncall_v2_curated"."invoice_products".attributes_requestedaction AS SERVICE_PROVIDED,

        -- 31
        "oncall_v2_curated"."final_dealer_response".shipto AS SERVICING_DEALER_ST_CUST_NBR,

        -- 32
        "root".orderer_shipto AS ST_CUST_NBR,
        
        -- 33
        "oncall_v2_curated"."assets".breakdown_state AS STATE,

        -- 34
        "oncall_v2_curated"."assets".assettype AS STATUS_LEVEL,
        
        -- 35
        "oncall_v2_curated"."assets".breakdown_street AS STREET_ADR,
        
        -- 36
        "dev"."oncall_v2_curated"."invoice_products".attributes_sculptedtreadname AS SUPPLIED_TREAD_DESIGN,

        -- 37
        CASE 
            WHEN "dev"."oncall_v2_curated"."final_dealer_response".accepted = true 
            THEN split_part("dev"."oncall_v2_curated"."final_dealer_response".responsetime, 'T', 1) 
        ELSE 
            NULL 
        END 
        AS TECH_ACCEPTANCE_DATE,
        
        -- 38
        CASE 
            WHEN "dev"."oncall_v2_curated"."final_dealer_response".accepted = true 
            THEN left(split_part("dev"."oncall_v2_curated"."final_dealer_response".responsetime, 'T', 2),8) 
        ELSE 
            NULL 
        END 
        AS TECH_ACCEPTANCE_TIME,
        
        -- TODO: Mercury Team request 3.b
        -- 39
        CASE 
            WHEN "dev"."oncall_v2_curated"."final_dealer_response".accepted = true 
            --THEN extract(month FROM to_timestamp("oncall_v2_curated"."final_dealer_response".responsetime, 'YYYY-MM-DD HH24:MI:SS'))
            THEN 
                CONCAT (
                    EXTRACT( 
                        year from to_timestamp(
                            "oncall_v2_curated"."final_dealer_response".responsetime, 'YYYY-MM-DD HH24:MI:SS'
                        )
                    ),    
                    LPAD (
                        EXTRACT (
                            month from to_timestamp(
                                "oncall_v2_curated"."final_dealer_response".responsetime, 'YYYY-MM-DD HH24:MI:SS'
                            )
                        ), 2, '0'
                    )
                )
        ELSE 
            NULL 
        END 
        AS TECH_ACCPT_CCYYMM_NBR,
        
        -- 40
        "provider_person".name AS TECH_NAME, 
        
        -- 41
        "provider_person".phone AS TECH_NBR_1,

        -- 42
        "oncall_v2_curated"."estimate_products".attributes_tireposition AS TIRE_POSITION,

        -- 43
        "dev"."oncall_v2_curated"."order_products".attributes_tiresize AS TIRE_SIZE,

        -- 44
        "dev"."oncall_v2_curated"."order_products".attributes_sculptedtreadname AS TREAD_DESIGN,

        -- 45
        "oncall_v2_curated"."assets".assettype AS VEHICLE_TYPE,

        -- 46
        "root".provider_location_zip AS ZIP_CODE,

        -- 47
        "oncall_v2_curated"."ch_dealerresponse".fields_response_time AS TIME_OF_CALL_RECEIVED,

        -- 48
        CASE 
            
            WHEN lower(fields_reason) LIKE '%accepted%' 
            THEN 'ACCEPTED'
            
            WHEN fields_reason 
            IN (
                'ETA missed / customer cancelled', 'ETA too long', 'Holiday or event closure', 
                'Incorrect tech rotation', 'No afterhours service', 'No answer', 'No national accounts', 'No service available',
                'Out of service area (Less than 50 miles)', 'Out of service area (Greater than 50 miles)', 'Phone number disconnected', 'Referred to store', 'Rim Not Available',
                'Technician refused service', 'Tire preference not available', 'Tire Brand brand - Not stocked by dealer',
                'Tire Brand brand - Not stocked by Service Provider', 'Tire size not available (included in stock profile)',
                'Too busy', 'Tire size not available', 'FX No Product Available', 'FX No Service Available'
            ) 
            THEN 'DECLINED'
            
            WHEN fields_reason 
            IN (
                'Checking Tire Availability', 'Fleet cancellation', 'Customer cancellation', 
                'Fleet cancelled after dispatch', 'Customer cancelled after dispatch', 'Fleet ON credit hold', 
                'Customer ON credit hold', 'Does not accept payment method', 'Other shop closer', 'Out of service area (Greater than 50 Miles)', 'Phone outage', 'Poor weather conditions', 'Power outage', 'Referred to backup', 'Tire size not available (outside stock profile)', 'Tires ON backorder', 'Unacceptable pricing', 'Tire Brand - Not stocked by dealer (Non Michelin)', 
                'FX Fleet Canceled', 'FX Customer Canceled', 'FX Fleet Canceled After Dispatch', 'FX Customer Canceled After Dispatch',
                'Other Service Provider dispatched', 'Other Dealer Dispatched', 'ETA missed / Fleet cancelled', 'Rejected'
            ) 
            THEN 'NOT_SERVICED'

        ELSE 
            NULL 
        END 
        AS CALL_STATUS,

        -- 49
        "oncall_v2_curated"."ch_dealerresponse".fields_asset_location_drive_distance AS ACTUAL_DISTANCE_MILES,

        -- 50
        "oncall_v2_curated"."assets".breakdown_country AS BREAKDOWN_COUNTRY,

        -- 51
        -- TODO: field not found
        -- SUB_DEFECTIVE_UNIT
        NULL AS SUB_DEFECTIVE_UNIT,

        -- 52
        "dev"."oncall_v2_curated"."invoice_products".attributes_manufacturer AS SUPPLIED_BRAND,

        -- 53
        -- TODO: field not found
        -- TIRE_SIZE_TYPE
        NULL AS TIRE_SIZE_TYPE,

        -- 54
        "dev"."oncall_v2_curated"."order_products".attributes_tirecondition AS FAILURE_REASON,

        -- 55
        "root".orderer_name AS ST_FLEET_NAME,

        -- 56
        "dev"."oncall_v2_curated"."order_products".attributes_producttype AS TIRE_TYPE,


        -- Date used for different purposes    
        "oncall_v2_curated"."root"."lastupdated" AS LASTUPDATED


        FROM "dev"."oncall_v2_curated"."root"
        LEFT JOIN "dev"."oncall_v2_curated"."provider_person"
            ON "dev"."oncall_v2_curated"."root"."provider_person_sk" = "dev"."oncall_v2_curated"."provider_person"."provider_person_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."orderer_person"
            ON "dev"."oncall_v2_curated"."root"."orderer_person_sk" = "dev"."oncall_v2_curated"."orderer_person"."orderer_person_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."order_products"
            ON "dev"."oncall_v2_curated"."root"."order_products_sk" = "dev"."oncall_v2_curated"."order_products"."order_products_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."invoice_products"
            ON "dev"."oncall_v2_curated"."root"."invoice_products_sk" = "dev"."oncall_v2_curated"."invoice_products"."invoice_products_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."final_dealer_response"
            ON "dev"."oncall_v2_curated"."root"."finaldealersresponse_sk" = "dev"."oncall_v2_curated"."final_dealer_response"."finaldealersresponse_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."estimate_products"
            ON "dev"."oncall_v2_curated"."root"."estimate_products_sk" = "dev"."oncall_v2_curated"."estimate_products"."estimate_products_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."ch_inboundprogram"
            ON "dev"."oncall_v2_curated"."root"."combinedhistory_inboundprogram_sk" = "dev"."oncall_v2_curated"."ch_inboundprogram"."combinedhistory_inboundprogram_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."ch_dealerresponse"
            ON "dev"."oncall_v2_curated"."root"."combinedhistory_dealerresponse_sk" = "dev"."oncall_v2_curated"."ch_dealerresponse"."combinedhistory_dealerresponse_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."assets" 
            ON "dev"."oncall_v2_curated"."root"."assets_sk" = "dev"."oncall_v2_curated"."assets"."assets_sk"
        LEFT JOIN "dev"."oncall_v2_curated"."event_status_history"
            ON "dev"."oncall_v2_curated"."root"."event_statushistory_sk" = "dev"."oncall_v2_curated"."event_status_history"."event_statushistory_sk"
        WHERE 
            extract(year FROM to_timestamp("oncall_v2_curated"."root".LASTUPDATED, 'YYYY-MM-DD HH24:MI:SS')) = extract(year from current_date)
            and 
        extract(month from to_timestamp("oncall_v2_curated"."root".LASTUPDATED, 'YYYY-MM-DD HH24:MI:SS')) = extract(month from current_date)
            and 
        extract(day from to_timestamp("oncall_v2_curated"."root".LASTUPDATED, 'YYYY-MM-DD HH24:MI:SS')) = extract(day from current_date)
--        and substring("oncall_v2_curated"."root".event_attributes_downat, 9,2) <= cast(substring(current_date, 9,2) as integer) 
        
        -- if the SP is run today, it will get the data for yesterday
		;
                       
        
       	create temporary table mercury_oncall_temp_table_2 as
       	select row_number() over(partition by incident_id, accepted_flag, refusal_accepted_reason, failure_reason), *
        from 
        mercury_oncall_temp_table;
        
        
        delete from mercury_oncall_temp_table_2
        where row_number != 1;
        
        ALTER TABLE mercury_oncall_temp_table_2
  		DROP COLUMN row_number;
        
        
		create table if not exists "mercury_request".daily
        (like mercury_oncall_temp_table_2);
        
        Insert into mercury_request.daily
        select * from mercury_oncall_temp_table_2;
                
        ALTER TABLE mercury_oncall_temp_table_2
  		DROP COLUMN LASTUPDATED;
        
    
EXECUTE 'unload ('
        || '''  select * from  mercury_oncall_temp_table_2  '''
        || ') '
		|| 'to '
        || '''s3://mercury-oncall/daily/'
        || date_var
        || ' '''
        || ' iam_role '
        || '''arn:aws:iam::464340339497:role/oncall-90-day-file-test-redshift-to-s3'''
        || ' header'
        || ' parallel off'
--        || ' MAXFILESIZE 64 MB'
		|| ' csv;'
        ;

--         || ' MAXFILESIZE 128 MB' this is deleted for daily uploads 

		drop table mercury_oncall_temp_table;
        drop table mercury_oncall_temp_table_2;
END;
$$;
Call mercury_output_file_aug2022();