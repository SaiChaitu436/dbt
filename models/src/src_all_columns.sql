{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        PARSE_JSON(MESSAGE_BODY) AS raw_data
    FROM 
        BUY_BOX.DEV.BUYBOX_RAW_DS_FIRST20_ENTRIES
),
flatten_payload AS (
    SELECT 
        raw_data:"EventTime"::STRING AS EventTime,
         
        raw_data:"NotificationMetadata":"ApplicationId"::STRING AS ApplicationId,
        raw_data:"NotificationMetadata":"NotificationId"::STRING AS NotificationId,
        raw_data:"NotificationMetadata":"PublishTime"::TIMESTAMP AS PublishTime,
        raw_data:"NotificationMetadata":"SubscriptionId"::STRING AS SubscriptionId,
        
        raw_data:"NotificationType"::STRING AS NotificationType,
        raw_data:"NotificationVersion"::STRING AS NotificationVersion,

        raw_data:"Payload":"AnyOfferChangedNotification":"OfferChangeTrigger":"ASIN"::STRING AS ASIN,
        raw_data:"Payload":"AnyOfferChangedNotification":"OfferChangeTrigger":"ItemCondition"::STRING AS ItemCondition,
        raw_data:"Payload":"AnyOfferChangedNotification":"OfferChangeTrigger":"MarketplaceId"::STRING AS MarketplaceId,
        raw_data:"Payload":"AnyOfferChangedNotification":"OfferChangeTrigger":"OfferChangeType"::STRING AS OfferChangeType,
        raw_data:"Payload":"AnyOfferChangedNotification":"OfferChangeTrigger":"TimeOfOfferChange"::STRING AS TimeOfOfferChange,
        
        raw_data:"Payload":"AnyOfferChangedNotification":"Offers"::ARRAY AS offers,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"BuyBoxPrices"::ARRAY as bbp,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"ListPrice"::ARRAY as lp,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"LowestPrices"::ARRAY as lop,
    raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfBuyBoxEligibleOffers"::ARRAY as neo,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfOffers"::ARRAY as no,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"SalesRankings"::ARRAY as sr
    FROM 
        source_data
),
flatten__payload as(
SELECT 
    EventTime,
    ApplicationId,
    NotificationId,
    PublishTime,
    SubscriptionId,
    NotificationType,
    NotificationVersion,
    ASIN,
    ItemCondition,
    MarketplaceId,
    OfferChangeType,
    TimeOfOfferChange,
    
    offer.value:"IsBuyBoxWinner"::BOOLEAN AS IsBuyBoxWinner,
    offer.value:"IsFeaturedMerchant"::BOOLEAN AS IsFeaturedMerchant,
    offer.value:"IsFulfilledByAmazon"::BOOLEAN AS IsFulfilledByAmazon,
    
    offer.value:"ListingPrice":"Amount"::FLOAT AS ListingPriceAmount,
    offer.value:"ListingPrice":"CurrencyCode"::STRING AS ListingPriceCurrencyCode,
    
    offer.value:"PrimeInformation":"IsOfferNationalPrime"::BOOLEAN AS IsOfferNationalPrime,
    offer.value:"PrimeInformation":"IsOfferPrime"::BOOLEAN AS IsOfferPrime,

    offer.value:"SellerId"::STRING AS SellerId,
    
    offer.value:"SellerFeedbackRating":"FeedbackCount"::INTEGER AS SellerFeedbackCount,
    offer.value:"SellerFeedbackRating":"SellerPositiveFeedbackRating"::INTEGER AS SellerPositiveFeedbackRating,
    
    offer.value:"Shipping":"Amount"::FLOAT AS ShippingAmount,
    offer.value:"Shipping":"CurrencyCode"::STRING AS ShippingCurrencyCode,
    
    offer.value:"ShippingTime":"AvailabilityType"::STRING AS ShippingAvailabilityType,
    offer.value:"ShippingTime":"MaximumHours"::INTEGER AS ShippingMaxHours,
    offer.value:"ShippingTime":"MinimumHours"::INTEGER AS ShippingMinHours,
    
    offer.value:"ShipsDomestically"::BOOLEAN AS ShipsDomestically,
    
    offer.value:"ShipsFrom":"Country"::STRING AS ShipsFromCountry,
    offer.value:"ShipsFrom":"State"::STRING AS ShipsFromState,
   
    offer.value:"SubCondition"::STRING AS SubCondition,

    bbp.value:"Condition"::STRING as BuyBoxPricesCondition,
        
    bbp.value:"LandedPrice":"Amount"::STRING as BLandedPriceAmount,
    bbp.value:"LandedPrice"::OBJECT:"CurrencyCode"::STRING as BLandedPriceCurrencyCode,

    bbp.value:"ListingPrice"::OBJECT:"Amount"::STRING as BListingPriceAmount,
    bbp.value:"ListingPrice"::OBJECT:"CurrencyCode"::STRING as BListingPriceCurrencyCode,

    bbp.value:"Shipping"::OBJECT:"Amount"::STRING as BShippingAmount,
    bbp.value:"Shipping"::OBJECT:"CurrencyCode"::STRING as BShippingCurrencyCode,

    lp.value:"Amount"::FLOAT AS ListPriceAmount,
    lp.value:"CurrencyCode"::STRING AS ListPriceCurrencyCode,

    lop.value:"Condition"::STRING AS LowestPricesCondition,
    lop.value:"FulfillmentChannel"::STRING AS LowestPricesFulfillmentChannel,

    lop.value:"LandedPrice":"Amount"::FLOAT AS LandedPriceAmount,
    lop.value:"LandedPrice":"CurrencyCode"::STRING AS LandedPriceCurrencyCode,

    lop.value:"ListingPrice":"Amount"::FLOAT AS LowestPricesListingPriceAmount,
    lop.value:"ListingPrice":"CurrencyCode"::STRING AS LowestPricesListingCurrencyCode,

    lop.value:"Shipping":"Amount"::FLOAT AS LowestPricesShippingPriceAmount,
    lop.value:"Shipping":"CurrencyCode"::STRING AS LowestPricesShippingCurrencyCode,

    neo.value:"Condition"::STRING AS BuyBoxEligibleOffersCondition,
    neo.value:"FulfillmentChannel"::STRING AS BuyBoxEligibleOffersFulfillmentChannel,
    neo.value:"OfferCount"::STRING AS BuyBoxEligibleOffersCount,

    no.value:"Condition"::STRING AS NumberOfOffersCondition,
    no.value:"FulfillmentChannel"::STRING AS NumberOfOffersFulfillmentChannel,
    no.value:"OfferCount"::STRING AS NumberOfOffersCount,

    sr.value:"ProductCategoryId"::STRING AS SalesRankingsProductCategoryId,
    sr.value:"Rank"::INTEGER AS SalesRankingsRank
   
FROM 
    flatten_payload,
    LATERAL FLATTEN(input => offers) AS offer,
    LATERAL FLATTEN(input => bbp) AS bbp,
    LATERAL FLATTEN(input => lp) AS lp,
    LATERAL FLATTEN(input => lop) AS lop,
    LATERAL FLATTEN(input => neo) AS neo,
    LATERAL FLATTEN(input => no) AS no,
    LATERAL FLATTEN(input => sr) AS sr,
)
select * from flatten__payload