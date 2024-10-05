{% snapshot scd_notification_lowest_prices %}
 
{{
    config(
        target_database='BUY_BOX',
        target_schema='DEV',
        unique_key='s_no',
        strategy='check',
        check_cols=['NOTIFICATIONID', 'FULFILLMENTCHANNEL','LANDEDCURRENCYCODE','LISTINGPRICEAMOUNT','LISTINGCURRENCYCODE']
    )
}}
 
SELECT
   *
FROM {{ ref('src_notification_lowest_prices') }}
 
{% endsnapshot %}