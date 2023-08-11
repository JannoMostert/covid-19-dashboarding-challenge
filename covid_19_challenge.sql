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
        
        /* Previous day cases and deaths */
        LAG(confirmed_cases) OVER (PARTITION BY county_fips_code
          ORDER BY date ) AS lag_1d_confirmed_cases,
        LAG(deaths) OVER (PARTITION BY county_fips_code
          ORDER BY date ) AS lag_1d_deaths
      
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
        SUM(a.confirmed_cases - a.lag_1d_confirmed_cases) AS new_monthly_cases,
        SUM(a.deaths - a.lag_1d_deaths)                   AS new_monthly_deaths
      
      FROM
        latest_record_month AS a
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

        /* Data quality issues */
        CASE
          WHEN a.new_monthly_cases < 0
            THEN 0
          ELSE a.new_monthly_cases
        END AS new_monthly_cases,
        CASE
          WHEN a.new_monthly_deaths < 0
            THEN 0
          ELSE a.new_monthly_deaths
        END AS new_monthly_deaths
      
      FROM
        aggregate_cases AS a
  ),
  complete_county_med_info AS
    (
      SELECT
        a.state_name,
        a.county_fips_code,
        a.county_name,
        /* Remove duplicate data entries in the sources tables */
        MAX(a.total_hospital_beds)                    AS total_hospital_beds,
        MAX(a.num_airborne_infection_isolation_rooms) AS num_airborne_infection_isolation_rooms,
        MAX(b.registered_nurses_ft)                   AS registered_nurses,
      FROM
        `bigquery-public-data.covid19_aha.hospital_beds` AS a
        INNER JOIN `bigquery-public-data.covid19_aha.staffing` AS b
      ON a.county_fips_code = b.county_fips_code
      --WHERE a.county_fips_code = '16001'
      GROUP BY 1, 2, 3
  ),
  per_thousand_metrics AS 
    (
      SELECT
        a.reporting_month,
        a.county_fips_code,
        a.county_name,
        b.state_name,
        a.state_fips_code,

        a.cumulative_cases,
        a.new_monthly_cases,
        ((1000/b.county_population) * a.new_monthly_cases) AS new_monthly_cases_per_1000_people,
        
        a.cumulative_deaths,
        a.new_monthly_deaths,
        ((1000/b.county_population) * a.new_monthly_deaths) AS new_monthly_deaths_per_1000_people,
        
        b.county_geom,
        b.county_population,
        b.median_age,
        b.median_income,
        b.county_elderly_population,

        c.total_hospital_beds,
        ((1000/b.county_population) * c.total_hospital_beds) AS hospital_beds_per_1000_people,

        c.registered_nurses,
        ((1000/b.county_population) * c.registered_nurses) AS registered_nurses_per_1000_people,

        c.num_airborne_infection_isolation_rooms,
        ((1000/b.county_population) * 
          c.num_airborne_infection_isolation_rooms) AS infection_isolation_rooms_per_1000_people

      FROM
        complete_county_case_info AS a
        INNER JOIN complete_county_pop_info AS b
                  ON a.county_fips_code = b.county_fips_code
        INNER JOIN complete_county_med_info AS c
          ON a.county_fips_code = c.county_fips_code
      WHERE a.reporting_month > '2020-03-31'
  )
SELECT
  reporting_month,
  county_fips_code,
  county_name,
  state_name,
  state_fips_code,

  cumulative_cases,
  new_monthly_cases,
  new_monthly_cases_per_1000_people,
  PERCENT_RANK() OVER (PARTITION BY reporting_month 
    ORDER BY new_monthly_cases_per_1000_people ASC) AS percentile_new_monthly_cases_per_1000_people,

  cumulative_deaths,
  new_monthly_deaths,
  new_monthly_deaths_per_1000_people,
  PERCENT_RANK() OVER (PARTITION BY reporting_month 
    ORDER BY new_monthly_deaths_per_1000_people ASC) AS percentile_new_monthly_deaths_per_1000_people,

  county_geom,
  county_population,
  median_age,
  median_income,
  county_elderly_population,

  total_hospital_beds,
  hospital_beds_per_1000_people,
  PERCENT_RANK() OVER (PARTITION BY reporting_month 
    ORDER BY hospital_beds_per_1000_people ASC) AS percentile_hospital_beds_per_1000_people,

  registered_nurses,
  registered_nurses_per_1000_people,
  PERCENT_RANK() OVER (PARTITION BY reporting_month 
    ORDER BY registered_nurses_per_1000_people ASC) AS percentile_registered_nurses_per_1000_people,

  num_airborne_infection_isolation_rooms,
  infection_isolation_rooms_per_1000_people,
  PERCENT_RANK() OVER (PARTITION BY reporting_month 
    ORDER BY infection_isolation_rooms_per_1000_people ASC) AS percentile_infection_isolation_rooms_per_1000_people

FROM per_thousand_metrics
;
