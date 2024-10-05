{% snapshot scd_shipping %}
 
{{
    config(
        target_database='BUY_BOX',
        target_schema='DEV',
        unique_key='s_no',
        strategy='check',
        check_cols=['NOTIFICATIONID','ShippingAmount','ShippingCurrencyCode','ShippingAvailabilityType','ShippingMaximumHours','ShippingMinimumHours','ShipsDomestically']
    )
}}
 
SELECT
   *
FROM {{ ref('src_shipping') }}
 
{% endsnapshot %}