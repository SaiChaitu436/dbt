{% snapshot scd_offer_change_trigger %}
 
{{
    config(
        target_database='BUY_BOX',
        target_schema='DEV',
        unique_key='s_no',
        strategy='check',
        check_cols=['ASIN','PUBLISHTIME','ITEMCONDITION']
    )
}}
 
SELECT
   *
FROM {{ ref('src_offer_change_trigger') }}
 
{% endsnapshot %}