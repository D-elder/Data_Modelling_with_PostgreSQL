WITH source AS (
    SELECT
        "LoanNumber" AS Borrower_id,
        "BorrowerName" AS Name,
        "BorrowerCity" AS city,
        "Gender",
        "Race",
        "Veteran",
        "BorrowerAddress" AS address,
        "BorrowerState" AS state,
        "ProjectCity",
        "ProjectState",
        "RuralUrbanIndicator",
        current_timestamp AS ingestion_timestamp
    FROM {{ ref('stg_SBAdata') }}
),
unique_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY Borrower_id ORDER BY ingestion_timestamp) AS row_number
    FROM source
)
SELECT
    Borrower_id,
    Name,
    city,
    "Gender",
    "Race",
    "Veteran",
    address,
    state,
    "ProjectCity",
    "ProjectState",
    "RuralUrbanIndicator",
    ingestion_timestamp
FROM unique_source
WHERE row_number = 1
