WITH source AS (
    SELECT
        "LoanNumber" AS type_id,
        "BusinessType" ,
        current_timestamp AS ingestion_timestamp
    FROM {{ ref('stg_SBAdata') }}
),
unique_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY "BusinessType" ORDER BY ingestion_timestamp) AS row_number
    FROM source
)
SELECT
    type_id,
    "BusinessType" ,
    ingestion_timestamp
FROM unique_source
WHERE row_number = 1