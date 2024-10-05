{% snapshot scd_notification_summary %}
 
{{
    config(
        target_database='BUY_BOX',
        target_schema='DEV',
        unique_key='s_no',
        strategy='check',
        check_cols=['NOTIFICATIONID', 'LANDEDPRICEAMOUNT','LANDEDCURRENCYCODE','LISTINGPRICEAMOUNT','LISTINGCURRENCYCODE',
                    'SHIPPINGCOST','SHIPPINGCODE','LISTPRICEAMOUNT']
    )
}}
 
SELECT
   *
FROM {{ ref('src_notification_summary') }}
 
{% endsnapshot %}