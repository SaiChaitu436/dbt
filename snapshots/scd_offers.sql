{% snapshot scd_offers %}
 
{{
    config(
        target_database='BUY_BOX',
        target_schema='DEV',
        unique_key='s_no',
        strategy='check',
        check_cols=['OfferID', 'SellerId','ListingPriceAmount']
    )
}}
 
SELECT
   *
FROM {{ ref('src_offers') }}
 
{% endsnapshot %}