WITH source AS (
    SELECT
        "LoanNumber" AS id,
        "BusinessAgeDescription" ,
        current_timestamp AS ingestion_timestamp
    FROM {{ ref('stg_SBAdata') }}
),
unique_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY "BusinessAgeDescription" ORDER BY ingestion_timestamp) AS row_number
    FROM source
)
SELECT
    id,
    "BusinessAgeDescription" ,
    ingestion_timestamp
FROM unique_source
WHERE row_number = 1