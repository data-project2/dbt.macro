-- contractdeliverypoint_check
WITH hashes AS (

    SELECT
        'DATAVERSE_ENC_CONTRACTDELIVERYPOINT_HIST' AS source,
        HASH_AGG(id, modifiedon) AS hash_val
    FROM {{ source('DATAVERSE_CRM365','DATAVERSE_ENC_CONTRACTDELIVERYPOINT_HIST') }}
    WHERE is_current = 1

    UNION ALL

    SELECT
        'stg_ldm_commercialcontract_contracteddeliverypoint_crm' AS source,
        HASH_AGG(sor_id, modifiedon) AS hash_val
    FROM {{ ref('stg_ldm_commercialcontract_contracteddeliverypoint_crm') }}


),

result_cte AS (

SELECT
    MAX(CASE WHEN source = 'DATAVERSE_ENC_CONTRACTDELIVERYPOINT_HIST' THEN hash_val END) AS DATAVERSE_ENC_CONTRACTDELIVERYPOINT_HIST_hash,
    MAX(CASE WHEN source = 'stg_ldm_commercialcontract_contracteddeliverypoint_crm' THEN hash_val END) AS stg_ldm_commercialcontract_contracteddeliverypoint_crm_hash,

    CASE
        WHEN COUNT(DISTINCT hash_val) = 1 THEN 'MATCH'
        ELSE 'MISMATCH'
    END AS result

FROM hashes

)

SELECT *
FROM result_cte
WHERE result != 'MATCH'
