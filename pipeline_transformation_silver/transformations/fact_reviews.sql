
CREATE or REFRESH STREAMING TABLE `02_silver`.fact_reviews 
AS
WITH model_output AS(
    Select *,
        ai_query(
            "system.ai.gpt-oss-20b",
                CONCAT(
        'Analyze the following restaurant review and identify potential issues. ',
        'Return ONLY a valid JSON object with exactly this structure: ',
        '{',
        '"sentiment":"positive|neutral|negative",',
        '"issue_delivery":true/false,',
        '"issue_delivery_reason":"reason or empty string",',
        '"issue_food_quality":true/false,',
        '"issue_food_quality_reason":"reason or empty string",',
        '"issue_pricing":true/false,',
        '"issue_pricing_reason":"reason or empty string",',
        '"issue_portion_size":true/false,',
        '"issue_portion_size_reason":"reason or empty string"',
        '}',
        'Rules: ',
        '1. sentiment must be exactly one of: positive, neutral, negative. ',
        '2. All issue fields must contain only true or false. ',
        '3. For any issue=true, provide a short explanation in the corresponding reason field. ',
        '4. For any issue=false, return an empty string for the corresponding reason field. ',
        '5. Detect delivery issues such as late delivery, missing items, cold food due to delivery, or poor delivery service. ',
        '6. Detect food quality issues such as stale food, bad taste, undercooked food, spoiled food, or poor food quality. ',
        '7. Detect pricing issues where the customer explicitly complains about cost, value for money, hidden charges, or overpricing. ',
        '8. Detect portion size issues when the customer complains about quantity being too small or insufficient. ',
        '9. Do not return markdown, code fences, explanations, or any text outside the JSON object. ',
        'Review text: ',
        review_text
        )
    ) AS ai_response
    From STREAM(`01_bronze`.reviews)
)
SELECT 
    review_id,
    order_id,
    customer_id,
    restaurant_id,
    review_text,
    rating,
    review_timestamp,
    get_json_object(ai_response, '$.sentiment') as sentiment,
    get_json_object(ai_response, '$.issue_delivery') :: BOOLEAN as issue_delivery,
    get_json_object(ai_response, '$.issue_delivery_reason') as issue_delivery_reason,
    get_json_object(ai_response, '$.issue_food_quality') :: BOOLEAN as issue_food_quality,
    get_json_object(ai_response, '$.issue_food_quality_reason') as issue_food_quality_reason,
    get_json_object(ai_response, '$.issue_pricing') :: BOOLEAN as issue_pricing,
    get_json_object(ai_response, '$.issue_pricing_reason') as issue_pricing_reason,
    get_json_object(ai_response, '$.issue_portion_size') :: BOOLEAN as issue_portion_size,
    get_json_object(ai_response, '$.issue_portion_size_reason') as issue_portion_size_reason

From model_output
