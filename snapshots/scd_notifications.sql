{% snapshot scd_notifications %}
 
{{
    config(
        target_schema='DEV',
        unique_key='s_no',
        strategy='timestamp',
        updated_at='PublishTime',
        invalidate_hard_deletes=True,
    )
}}
 
SELECT
   *
FROM {{ ref('src_notifications') }}
 
{% endsnapshot %}