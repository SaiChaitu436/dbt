{% snapshot scd_buybox_offers %}
 
{{
    config(
        target_database='BUY_BOX',
        target_schema='DEV',
        unique_key='s_no',
        strategy='check',
        check_cols=['ELIGIBLEFULFILLMENTCHANNEL', 'NUMBEROFFERCONDITION','ELIGIBLEOFFERCOUNT','ELIGIBLEOFFERCOUNT']
    )
}}
 
SELECT
   *
FROM {{ ref('src_buybox_offers') }}
 
{% endsnapshot %}