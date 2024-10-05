{% snapshot scd_products %}
 
{{
    config(
        target_database='BUY_BOX',
        target_schema='DEV',
        unique_key='s_no',
        strategy='check',
        check_cols=['ASIN','MARKETPLACEID']
    )
}}
 
SELECT
   *
FROM {{ ref('src_products') }}
 
{% endsnapshot %}