{% snapshot scd_sales_ranking %}
 
{{
    config(
        target_database='BUY_BOX',
        target_schema='DEV',
        unique_key='s_no',
        strategy='check',
        check_cols=['PRODUCTCATEGORYID','RANKING']
    )
}}
 
SELECT
   *
FROM {{ ref('src_sales_ranking') }}
 
{% endsnapshot %}