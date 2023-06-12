with
source as (
    select * from {{ source('prod_repl','customer') }}
),
renamed as (
    select
        customer_id,
        company_name,
        first_name, 
        last_name, 
        email,
        company_address_1, 
        company_address_2,
        company_city,   
        company_state,
        company_zip,
        created_timestamp,
        update_timestamp,
        dbt_run_at as CURRENT_TIMESTAMP()
    from source
)
select * from renamed