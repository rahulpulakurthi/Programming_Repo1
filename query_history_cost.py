from snowflake.snowpark import Session
import snowflake.connector
import re

connection_parameters = {
"account": "BIJBKYG-IMETADATA",
"user": "rpulakurthi",
"password": "xxxxxxxxxxxx",
"role": "SYSADMIN",  # optional
"warehouse": "ADHOC_WH",  # optional
"database": "TESTDB",  # optional
"schema": "PUBLIC",  # optional
}

snowpark_session = Session.builder.configs(connection_parameters).create()

def gen_query_execute(account_locator, history_table, min_query_time, last_enriched_query_end_time):
    query = f"""
insert into query_history_enriched_test1  (
with query_history as (
    select
        *
    from TABLE('{history_table}')
    where end_time < {min_query_time}  --getdate()
    and end_time > {last_enriched_query_end_time})

, dates_base as (
    select date_day as date from (    
        with rawdata as (
            with p as (
                select 0 as generated_number union all select 1
            ), 
            unioned as (
                select
                p0.generated_number * power(2, 0)
                 + 
                p1.generated_number * power(2, 1)
                 + 
                p2.generated_number * power(2, 2)
                 + 
                p3.generated_number * power(2, 3)
                 + 
                p4.generated_number * power(2, 4)
                 + 
                p5.generated_number * power(2, 5)
                 + 
                p6.generated_number * power(2, 6)
                 + 
                p7.generated_number * power(2, 7)
                 + 
                p8.generated_number * power(2, 8)
                 + 
                p9.generated_number * power(2, 9)
                 + 
                p10.generated_number * power(2, 10)
                 + 
                p11.generated_number * power(2, 11)
                 +
                p12.generated_number * power(2, 12)
                + 1
                as generated_number
                from
                p as p0
                 cross join 
                p as p1
                 cross join 
                p as p2
                 cross join 
                p as p3
                 cross join 
                p as p4
                 cross join 
                p as p5
                 cross join 
                p as p6
                 cross join 
                p as p7
                 cross join 
                p as p8
                 cross join 
                p as p9
                 cross join 
                p as p10
                 cross join
                p as p11 
                 cross join
                p as p12
            )
            select *
            from unioned
            where generated_number <= 10000
            order by generated_number
        ),

        all_periods as (

            select (
            dateadd(
                day,
                row_number() over (order by 1) - 1,
                '2018-01-01'
                )
            ) as date_day
            from rawdata

        ),

        filtered as (
            select *
            from all_periods
            where date_day <= dateadd(day, 1, current_date)
        )

        select * from filtered


    )
) 
, rate_sheet_daily_base as (
    select
        date,
        usage_type,
        currency,
        effective_rate,
        service_type
    from snowflake.organization_usage.rate_sheet_daily   -- updated 
    where
        account_locator = '{account_locator}' --current_account()
)

, remaining_balance_daily_without_contract_view as (
    select
        date,
        organization_name,
        currency,
        free_usage_balance,
        capacity_balance,
        on_demand_consumption_balance,
        rollover_balance
    from snowflake.organization_usage.remaining_balance_daily

    qualify row_number() over (partition by date order by contract_number desc) = 1
)
, stop_thresholds as (
    select min(date) as start_date
    from rate_sheet_daily_base

    union all

    select min(date) as start_date
    from remaining_balance_daily_without_contract_view
)

, date_range as (
    select
        max(start_date) as start_date,
        current_date as end_date
    from stop_thresholds
)

, remaining_balance_daily as (
    select
        date,
        free_usage_balance + capacity_balance + on_demand_consumption_balance + rollover_balance as remaining_balance,
        remaining_balance < 0 as is_account_in_overage
    from remaining_balance_daily_without_contract_view
)

, latest_remaining_balance_daily as (
    select
        date,
        remaining_balance,
        is_account_in_overage
    from remaining_balance_daily
    qualify row_number() over (order by date desc) = 1
)

, rate_sheet_daily as (
    select rate_sheet_daily_base.*
    from rate_sheet_daily_base
    inner join date_range
        on rate_sheet_daily_base.date between date_range.start_date and date_range.end_date
)

, rates_date_range_w_usage_types as (
    select
        date_range.start_date,
        date_range.end_date,
        usage_types.usage_type
    from date_range
    cross join (select distinct usage_type from rate_sheet_daily) as usage_types
)

, base as (
    select
        db.date,
        dr.usage_type
    from dates_base as db
    inner join rates_date_range_w_usage_types as dr
        on db.date between dr.start_date and dr.end_date
)
, rates_w_overage as (
    select
        base.date,
        base.usage_type,
        coalesce(
            rate_sheet_daily.service_type,
            lag(rate_sheet_daily.service_type) ignore nulls over (partition by base.usage_type order by base.date),
            lead(rate_sheet_daily.service_type) ignore nulls over (partition by base.usage_type order by base.date)
        ) as service_type,
        coalesce(
            rate_sheet_daily.effective_rate,
            lag(rate_sheet_daily.effective_rate) ignore nulls over (partition by base.usage_type order by base.date),
            lead(rate_sheet_daily.effective_rate) ignore nulls over (partition by base.usage_type order by base.date)
        ) as effective_rate,
        coalesce(
            rate_sheet_daily.currency,
            lag(rate_sheet_daily.currency) ignore nulls over (partition by base.usage_type order by base.date),
            lead(rate_sheet_daily.currency) ignore nulls over (partition by base.usage_type order by base.date)
        ) as currency,
        base.usage_type like 'overage-%' as is_overage_rate,
        replace(base.usage_type, 'overage-', '') as associated_usage_type,
        coalesce(remaining_balance_daily.is_account_in_overage, latest_remaining_balance_daily.is_account_in_overage, false) as _is_account_in_overage,
        case
            when _is_account_in_overage and is_overage_rate then 1
            when not _is_account_in_overage and not is_overage_rate then 1
            else 0
        end as rate_priority

    from base
    left join latest_remaining_balance_daily on latest_remaining_balance_daily.date is not null
    left join remaining_balance_daily
        on base.date = remaining_balance_daily.date
    left join rate_sheet_daily
        on base.date = rate_sheet_daily.date
            and base.usage_type = rate_sheet_daily.usage_type
)

, rates as (
    select
        date,
        usage_type,
        associated_usage_type,
        service_type,
        effective_rate,
        currency,
        is_overage_rate
    from rates_w_overage
    qualify row_number() over (partition by date, service_type, associated_usage_type order by rate_priority desc) = 1
)

, daily_rates as (
    select
        date,
        associated_usage_type as usage_type,
        service_type,
        effective_rate,
        currency,
        is_overage_rate,
        row_number() over (partition by service_type, associated_usage_type order by date desc) = 1 as is_latest_rate
    from rates
    order by date
)

, stop_threshold as (
    select max(end_time) as latest_ts
    from snowflake.organization_usage.warehouse_metering_history where account_locator ='{account_locator}'  -- updated
)

, filtered_queries as (
    select
        query_id,
        query_text as original_query_text,
        credits_used_cloud_services,
        warehouse_id,
        warehouse_size is not null as ran_on_warehouse,
        timeadd(
            'millisecond',
            queued_overload_time + compilation_time
            + queued_provisioning_time + queued_repair_time
            + list_external_files_time,
            start_time
        ) as execution_start_time,
        start_time,
        end_time
    from TABLE('{history_table}')  -- updated
    where  
    end_time <= (select latest_ts from stop_threshold)
    and end_time < {min_query_time}  --getdate()
    and end_time > {last_enriched_query_end_time}
)
, hours_list as (
    select
        dateadd(
            'hour',
            '-' || row_number() over (order by seq4() asc),
            dateadd('day', '+1', current_date::timestamp_tz)
        ) as hour_start,
        dateadd('hour', '+1', hour_start) as hour_end
    from table(generator(rowcount => (24 * 730)))
)
, query_hours as (
    select
        hours_list.hour_start,
        hours_list.hour_end,
        queries.*
    from hours_list
    inner join filtered_queries as queries
        on hours_list.hour_start >= date_trunc('hour', queries.execution_start_time)
            and hours_list.hour_start < queries.end_time
            and queries.ran_on_warehouse
)
, query_seconds_per_hour as (
    select
        *,
        datediff('millisecond', greatest(execution_start_time, hour_start), least(end_time, hour_end)) as num_milliseconds_query_ran,
        sum(num_milliseconds_query_ran) over (partition by warehouse_id, hour_start) as total_query_milliseconds_in_hour,
        div0(num_milliseconds_query_ran, total_query_milliseconds_in_hour) as fraction_of_total_query_time_in_hour,
        hour_start as hour
    from query_hours
)

, credits_billed_hourly as (
    select
        start_time as hour,
        warehouse_id,
        credits_used_compute,
        credits_used_cloud_services
    from --snowflake.account_usage.warehouse_metering_history
    snowflake.organization_usage.warehouse_metering_history where account_locator ='{account_locator}'   -- updated
)

, query_cost as (
    select
        query_seconds_per_hour.*,
        credits_billed_hourly.credits_used_compute * daily_rates.effective_rate as actual_warehouse_cost,
        credits_billed_hourly.credits_used_compute * query_seconds_per_hour.fraction_of_total_query_time_in_hour * daily_rates.effective_rate as allocated_compute_cost_in_hour,
        credits_billed_hourly.credits_used_compute * query_seconds_per_hour.fraction_of_total_query_time_in_hour as allocated_compute_credits_in_hour
    from query_seconds_per_hour
    inner join credits_billed_hourly
        on query_seconds_per_hour.warehouse_id = credits_billed_hourly.warehouse_id
            and query_seconds_per_hour.hour = credits_billed_hourly.hour
    inner join daily_rates
        on date(query_seconds_per_hour.start_time) = daily_rates.date
            and daily_rates.service_type = 'WAREHOUSE_METERING'
            and daily_rates.usage_type = 'compute'
)
, cost_per_query as (
    select
        query_id,
        any_value(start_time) as start_time,
        any_value(end_time) as end_time,
        any_value(execution_start_time) as execution_start_time,
        sum(allocated_compute_cost_in_hour) as compute_cost,
        sum(allocated_compute_credits_in_hour) as compute_credits,
        any_value(credits_used_cloud_services) as credits_used_cloud_services,
        any_value(ran_on_warehouse) as ran_on_warehouse
    from query_cost
    group by 1
)

, credits_billed_daily as (
    select
        date(hour) as date,
        sum(credits_used_compute) as daily_credits_used_compute,
        sum(credits_used_cloud_services) as daily_credits_used_cloud_services,
        greatest(daily_credits_used_cloud_services - daily_credits_used_compute * 0.1, 0) as daily_billable_cloud_services
    from credits_billed_hourly
    group by 1
)
, all_queries as (
    select
        query_id,
        start_time,
        end_time,
        execution_start_time,
        compute_cost,
        compute_credits,
        credits_used_cloud_services,
        ran_on_warehouse
    from cost_per_query

    union all

    select
        query_id,
        start_time,
        end_time,
        execution_start_time,
        0 as compute_cost,
        0 as compute_credits,
        credits_used_cloud_services,
        ran_on_warehouse
    from filtered_queries
    where
        not ran_on_warehouse
)

, stg__cost_per_query as (
    select
        all_queries.query_id,
        all_queries.start_time,
        all_queries.end_time,
        all_queries.execution_start_time,
        all_queries.compute_cost,
        all_queries.compute_credits,
        -- For the most recent day, which is not yet complete, this calculation won't be perfect.
        -- So, we don't look at any queries from the most recent day t, just t-1 and before.
        (div0(all_queries.credits_used_cloud_services, credits_billed_daily.daily_credits_used_cloud_services) * credits_billed_daily.daily_billable_cloud_services) * coalesce(daily_rates.effective_rate, current_rates.effective_rate) as cloud_services_cost,
        div0(all_queries.credits_used_cloud_services, credits_billed_daily.daily_credits_used_cloud_services) * credits_billed_daily.daily_billable_cloud_services as cloud_services_credits,
        all_queries.compute_cost + cloud_services_cost as query_cost,
        all_queries.compute_credits + cloud_services_credits as query_credits,
        all_queries.ran_on_warehouse,
        coalesce(daily_rates.currency, current_rates.currency) as currency
    from all_queries
    inner join credits_billed_daily
        on date(all_queries.start_time) = credits_billed_daily.date
    left join daily_rates
        on date(all_queries.start_time) = daily_rates.date
            and daily_rates.service_type = 'CLOUD_SERVICES'
            and daily_rates.usage_type = 'cloud services'
    inner join daily_rates as current_rates
        on current_rates.is_latest_rate
            and current_rates.service_type = 'CLOUD_SERVICES'
            and current_rates.usage_type = 'cloud services'
    order by all_queries.start_time asc
)
select
    '{account_locator}' account_locator,
    cost_per_query.query_id,
    cost_per_query.compute_cost,
    cost_per_query.compute_credits,
    cost_per_query.cloud_services_cost,                                                                               
    cost_per_query.cloud_services_credits,                                                                                                                                                        
    cost_per_query.query_cost,
    cost_per_query.query_credits,
    cost_per_query.execution_start_time,

    -- Grab all columns from query_history (except the query time columns which we rename below)
    query_history.query_text,
    query_history.database_id,
    query_history.database_name,
    query_history.schema_id,
    query_history.schema_name,
    query_history.query_type,
    query_history.session_id,
    query_history.user_name,
    query_history.role_name,
    query_history.warehouse_id,
    query_history.warehouse_name,
    query_history.warehouse_size,
    query_history.warehouse_type,
    query_history.cluster_number,
    query_history.query_tag,
    query_history.execution_status,
    query_history.error_code,
    query_history.error_message,
    query_history.start_time,
    query_history.end_time,
    query_history.total_elapsed_time,
    query_history.bytes_scanned,
    query_history.percentage_scanned_from_cache,
    query_history.bytes_written,
    query_history.bytes_written_to_result,
    query_history.bytes_read_from_result,
    query_history.rows_produced,
    query_history.rows_inserted,
    query_history.rows_updated,
    query_history.rows_deleted,
    query_history.rows_unloaded,
    query_history.bytes_deleted,
    query_history.partitions_scanned,
    query_history.partitions_total,
    query_history.bytes_spilled_to_local_storage,
    query_history.bytes_spilled_to_remote_storage,
    query_history.bytes_sent_over_the_network,
    query_history.outbound_data_transfer_cloud,
    query_history.outbound_data_transfer_region,
    query_history.outbound_data_transfer_bytes,
    query_history.inbound_data_transfer_cloud,
    query_history.inbound_data_transfer_region,
    query_history.inbound_data_transfer_bytes,
    query_history.credits_used_cloud_services,
    query_history.release_version,
    query_history.external_function_total_invocations,
    query_history.external_function_total_sent_rows,
    query_history.external_function_total_received_rows,
    query_history.external_function_total_sent_bytes,
    query_history.external_function_total_received_bytes,
    query_history.query_load_percent,
    query_history.is_client_generated_statement,
    query_history.query_acceleration_bytes_scanned,
    query_history.query_acceleration_partitions_scanned,
    query_history.query_acceleration_upper_limit_scale_factor,

    -- Rename some existing columns for clarity
    query_history.total_elapsed_time as total_elapsed_time_ms,
    query_history.compilation_time as compilation_time_ms,
    query_history.queued_provisioning_time as queued_provisioning_time_ms,
    query_history.queued_repair_time as queued_repair_time_ms,
    query_history.queued_overload_time as queued_overload_time_ms,
    query_history.transaction_blocked_time as transaction_blocked_time_ms,
    query_history.list_external_files_time as list_external_files_time_ms,
    query_history.execution_time as execution_time_ms,

    -- New columns
    query_history.warehouse_size is not null as ran_on_warehouse,
    query_history.bytes_scanned / power(1024, 3) as data_scanned_gb,
    data_scanned_gb * query_history.percentage_scanned_from_cache as data_scanned_from_cache_gb,
    query_history.bytes_spilled_to_local_storage / power(1024, 3) as data_spilled_to_local_storage_gb,
    query_history.bytes_spilled_to_remote_storage / power(1024, 3) as data_spilled_to_remote_storage_gb,
    query_history.bytes_sent_over_the_network / power(1024, 3) as data_sent_over_the_network_gb,

    query_history.total_elapsed_time / 1000 as total_elapsed_time_s,
    query_history.compilation_time / 1000 as compilation_time_s,
    query_history.queued_provisioning_time / 1000 as queued_provisioning_time_s,
    query_history.queued_repair_time / 1000 as queued_repair_time_s,
    query_history.queued_overload_time / 1000 as queued_overload_time_s,
    query_history.transaction_blocked_time / 1000 as transaction_blocked_time_s,
    query_history.list_external_files_time / 1000 as list_external_files_time_s,
    query_history.execution_time / 1000 as execution_time_s,
    cost_per_query.currency

from query_history
inner join stg__cost_per_query as cost_per_query
    on query_history.query_id = cost_per_query.query_id
order by query_history.start_time
)

    """
    #print(query)

    try:
        snowpark_session.sql(query).collect()
        df1_str = str(snowpark_session.sql(min_query_time).collect()[0][0])
        df2_str = str(snowpark_session.sql(last_enriched_query_end_time).collect()[0][0])
        return f"*********Batch Load Completed for {account_locator} from end_date > {df2_str} and end_date < {df1_str}*************"
    except Exception as e:
        er = str(e)
        status = f"""Batch Load Failed for the {account_locator} - caused: {er}"""
        print(status)
        log_query =f"""insert into query_cost_history_logs SELECT CONVERT_TIMEZONE('Asia/Kolkata', CURRENT_TIMESTAMP()), "{status}"
        """
        snowpark_session.sql(log_query).collect()
        return e
    
def execute_cal_cost(account_locator):
    min_query_time = "(select getdate()::date)"
    print(min_query_time)
    for i in range(len(account_locator)):
        history_table = account_locator[i] + '_SNOWDBA.public.maint_query_history'
        print(history_table)
        last_enriched_query_end_time = f"(select nvl(max(end_time),'1970-01-01'::timestamp) as last_enriched_query_end_time from query_history_enriched where account_locator='{account_locator[i]}')"
        print(last_enriched_query_end_time)
        a = gen_query_execute(account_locator[i], history_table, min_query_time, last_enriched_query_end_time)
        print(a)

def gen_acc_loc():
    # Set up Snowflake connection details
    conn = snowflake.connector.connect(
        account='BIJBKYG-IMETADATA',
        user='rpulakurthi',
        password='Rahul111@',
        role='SYSADMIN',
        warehouse='ADHOC_WH'
    )
 
    #check connection
    #print(conn)

    # Execute Snowflake query
    query = "show databases"
    cur = conn.cursor()
    cur.execute(query)

    databases = cur.fetchall()

    # Extract only the database names
    all_database_names = [row[1] for row in databases]

    # Display all the database names
    #print(all_database_names)

    snowdba_list = [value for value in all_database_names if value.endswith("_SNOWDBA")]

    # Display the result
    #print(snowdba_list)
 
    # Close the cursor and connection
    cur.close()
    conn.close()
    return snowdba_list

def cal_cost(inp):

    #execute for all accounts
    if (inp.upper()).strip() == 'ALL': # converting to upper and remove_space (leading / trailing)

        rows = snowpark_session.sql("show databases").collect()
        all_database_names = [row.name for row in rows]
        print(all_database_names)
        complete_account_locator = [value for value in all_database_names if value.endswith("_SNOWDBA")]
        account_locator = [name.replace('_SNOWDBA', '') for name in complete_account_locator]
        print(account_locator)

    else: #this will execute for 1 or more accounts
        result = re.split(',', inp)
        #print(result)
        account_locator = [x.strip() for x in result] #remove_space (leading / trailing)
        print(account_locator)

    a = execute_cal_cost(account_locator)
    #print(a)


cal_cost('mkl') 