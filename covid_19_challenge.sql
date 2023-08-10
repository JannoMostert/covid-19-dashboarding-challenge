WITH
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
      WHERE b.county_name = 'Kings' --Selecting Kings County
  )
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
;
