CREATE OR REPLACE TABLE `covid19_challenge.dashboard_table`
PARTITION BY reporting_month
CLUSTER BY county_fips_code
AS
WITH
/* Transform County Population Info */
  county_population_analysis AS
    (
      SELECT
        a.geo_id,
        c.state_name,
        b.state_fips_code,
        b.county_name,
        b.county_fips_code,
        b.lsad_name,
        b.county_geom,
        a.total_pop,
        a.median_age,
        a.median_income,
        
        /* Elderly if older than 65 */
        (a.female_65_to_66 + a.female_67_to_69 + a.female_70_to_74 +
         a.female_75_to_79 + a.female_80_to_84 + a.female_85_and_over) AS elderly_females,
        (a.male_65_to_66 + a.male_67_to_69 + a.male_70_to_74 +
         a.male_75_to_79 + a.male_80_to_84 + a.male_85_and_over)       AS elderly_males,
      
      FROM
        `bigquery-public-data.census_bureau_acs.county_2018_1yr` AS a
        INNER JOIN `bigquery-public-data.geo_us_boundaries.counties` AS b
      ON a.geo_id = b.geo_id
        INNER JOIN `bigquery-public-data.geo_us_boundaries.states` AS c
        ON b.state_fips_code = c.state_fips_code
      --WHERE b.county_fips_code = '36047'
  ),
  complete_county_pop_info AS
    (
      SELECT
        state_name,
        state_fips_code,
        county_name,
        county_fips_code,
        county_geom,
        total_pop                         AS county_population,
        median_age,
        median_income,
        (elderly_females + elderly_males) AS county_elderly_population
      FROM
        county_population_analysis
  ),

/* Calculate monthly COVID-19 case changes */
  latest_record_month AS
    (
      SELECT
        date,
        county_fips_code,
        county_name,
        state,
        state_fips_code,
        confirmed_cases,
        deaths,
        
        ROW_NUMBER() OVER (PARTITION BY county_fips_code
          ORDER BY date DESC) AS rn
      
      FROM
        `bigquery-public-data.covid19_usafacts.summary`
      --WHERE
        --county_fips_code = '36047'
  ),
  aggregate_cases AS
    (
      SELECT
        LAST_DAY(a.date, month)                    AS reporting_month,
        a.county_fips_code,
        a.county_name,
        a.state,
        a.state_fips_code,
        
        /* Numbers are cumulative so can take max for a month */
        MAX(a.confirmed_cases)                     AS cumulative_cases,
        MAX(a.deaths)                              AS cumulative_deaths,
        
        /* New case and death calculation */
        SUM(a.confirmed_cases - b.confirmed_cases) AS new_monthly_cases,
        SUM(a.deaths - b.deaths)                   AS new_monthly_deaths
      
      FROM
        latest_record_month AS a
        /* Determine new daily cases by joining previous day onto current day */
        INNER JOIN latest_record_month AS b
                   ON a.rn = (b.rn - 1)
                      AND a.county_fips_code = b.county_fips_code
      GROUP BY
        1,
        2,
        3,
        4,
        5
  ),
  complete_county_case_info AS
    (
      SELECT
        a.reporting_month,
        a.county_fips_code,
        a.county_name,
        a.state,
        a.state_fips_code,
        a.cumulative_cases,
        a.cumulative_deaths,
        a.new_monthly_cases,
        a.new_monthly_deaths,
        
        /* Previous month new cases and deaths to calculate % change */
        LAG(new_monthly_cases) OVER (PARTITION BY county_fips_code
          ORDER BY reporting_month ) AS lag_1m_new_monthly_cases,
        LAG(new_monthly_deaths) OVER (PARTITION BY county_fips_code
          ORDER BY reporting_month ) AS lag_1m_new_monthly_deaths
      
      FROM
        aggregate_cases AS a
  ),
  complete_county_med_info AS
    (
      SELECT
        a.state_name,
        a.county_fips_code,
        a.county_name,
        a.total_hospital_beds,
        a.num_airborne_infection_isolation_rooms,
        b.registered_nurses_ft AS registered_nurses,
      FROM
        `bigquery-public-data.covid19_aha.hospital_beds` AS a
        INNER JOIN `bigquery-public-data.covid19_aha.staffing` AS b
      ON a.county_fips_code = b.county_fips_code
      --WHERE a.county_fips_code = '36047'
  )
SELECT
  a.reporting_month,
  a.county_fips_code,
  a.county_name,
  b.state_name,
  a.state_fips_code,
  a.cumulative_cases,
  a.new_monthly_cases,
  a.lag_1m_new_monthly_cases,
  
  a.cumulative_deaths,
  a.new_monthly_deaths,
  a.lag_1m_new_monthly_deaths,
  
  b.county_geom,
  b.county_population,
  b.median_age,
  b.median_income,
  b.county_elderly_population,

  c.total_hospital_beds,
  c.registered_nurses,
  c.num_airborne_infection_isolation_rooms

FROM
  complete_county_case_info AS a
  INNER JOIN complete_county_pop_info AS b
             ON a.county_fips_code = b.county_fips_code
  INNER JOIN complete_county_med_info AS c
    ON a.county_fips_code = c.county_fips_code
WHERE a.reporting_month > '2020-03-31'
;
