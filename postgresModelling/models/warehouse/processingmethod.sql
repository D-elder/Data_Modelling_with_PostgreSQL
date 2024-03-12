WITH source AS (
    SELECT
        "LoanNumber" AS id,
        "ProcessingMethod" ,
        current_timestamp AS ingestion_timestamp
    FROM {{ ref('stg_SBAdata') }}
),
unique_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY "ProcessingMethod" ORDER BY ingestion_timestamp) AS row_number
    FROM source
)
SELECT
    id,
    "ProcessingMethod" ,
    ingestion_timestamp
FROM unique_source
WHERE row_number = 1