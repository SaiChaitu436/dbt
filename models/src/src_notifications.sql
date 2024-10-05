WITH source_data AS (
    SELECT 
        PARSE_JSON(message_body) AS raw_data,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::ARRAY AS offer
    FROM 
        BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_data AS (
    SELECT
        row_number() OVER (ORDER BY raw_data:"NotificationMetadata"::OBJECT:"NotificationId") AS s_no,
        offer.value:"ASIN"::STRING AS ASIN,
        raw_data:"NotificationMetadata"::OBJECT:"ApplicationId"::STRING AS ApplicationId,
        raw_data:"NotificationMetadata"::OBJECT:"SubscriptionId"::STRING AS SubscriptionId,
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        raw_data:"NotificationType"::STRING AS NotificationType,
        raw_data:"NotificationVersion"::STRING AS NotificationVersion,
        raw_data:"NotificationMetadata"::OBJECT:"PublishTime"::TIMESTAMP AS PublishTime
    FROM    
        source_data,
    LATERAL FLATTEN(input => source_data.offer) AS offer
)
 
SELECT
    *
FROM flatten_data
