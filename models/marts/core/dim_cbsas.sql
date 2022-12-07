select distinct
    cbsa_code,
    statistical_area
from
    {{ ref('stg_cbsa_codebook') }}
order by
    2