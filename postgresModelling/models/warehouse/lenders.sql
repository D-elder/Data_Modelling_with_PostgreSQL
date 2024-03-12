WITH source AS (
    SELECT
        "ServicingLenderLocationID" AS Lender_id,
        "ServicingLenderName" AS Name,
        "ServicingLenderCity" AS city,
        "SBAOfficeCode" as office_code,
        "ServicingLenderAddress" AS address,
        "ServicingLenderState" AS state,
        current_timestamp AS ingestion_timestamp
    FROM {{ ref('stg_SBAdata') }}
),
unique_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY Lender_id ORDER BY ingestion_timestamp) AS row_number
    FROM source
)
SELECT
    Lender_id,
    Name,
    city,
    office_code,
    address,
    state,
    ingestion_timestamp
FROM unique_source
WHERE row_number = 1
