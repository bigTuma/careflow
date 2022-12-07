with rnks as (

    select
        *,
        rank() over (partition by fips order by lastupdateddate::date desc) as most_recent_record
    from
        {{ ref('stg_counties') }}
),

most_recent as (

    select 
        *
    from
        rnks
    where
        most_recent_record = 1
)

select
    fips,
    state,
    county,
    lastupdateddate::date as date,
    metricstestpositivityratio::float as test_positivity_ratio,
    metricscasedensity::float as case_density,
    metricsinfectionrate::float as infection_rate,
    metricsicucapacityratio::float as icu_capacity_ratio,
    metricsweeklynewcasesper100k::float as weekly_cases_per_100k,
    metricsweeklycovidadmissionsper100k::float as weekly_covid_admissions_per_100k,
    risklevelsoverall::integer as risk_level_overall,
    actualscases::integer as actual_cases,
    actualsdeaths::integer as actual_deaths,
    actualsnewcases::integer as actual_new_cases,
    actualsnewdeaths::integer as actual_new_deaths,
    cdctransmissionlevel::integer as cdc_transmission_level
from
    most_recent
order by
    1