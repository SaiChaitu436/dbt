WITH source_data AS (
    SELECT 
        PARSE_JSON(MESSAGE_BODY) AS raw_data
    FROM 
        BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload AS (
    SELECT 
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfOffers"::ARRAY AS offer,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfBuyBoxEligibleOffers"::ARRAY AS offers
    FROM    
        source_data
),
flatten_payload_1 AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY offers.value:"Condition"::STRING) AS s_no,
        offers.value:"Condition"::STRING AS EligibleOfferCondition,
        offers.value:"FulfillmentChannel"::STRING AS EligibleFulFillmentChannel,
        offers.value:"OfferCount"::STRING AS EligibleOfferCount,
        
        offer.value:"Condition"::STRING AS NumberOfferCondition,
        offer.value:"FulfillmentChannel"::STRING AS NumberFulFillmentChannel,
        offer.value:"OfferCount"::STRING AS NumberOfferCount
    FROM 
        flatten_payload, 
        LATERAL FLATTEN(input => flatten_payload.offer) AS offer,
        LATERAL FLATTEN(input => flatten_payload.offers) AS offers
)
SELECT * 
FROM flatten_payload_1