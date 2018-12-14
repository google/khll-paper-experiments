/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
WITH
source_table AS (
  SELECT
    TO_JSON_STRING([/* FIXME <values to group by> */]) AS value,
    TO_JSON_STRING([/* FIXME <user id> */]) AS id
  FROM /* FIXME <source data> */
),
/* KHLL is not (yet) available on BigQuery. We simulate the KHLL algorithm
 * with the following. Note that we set the precision of KHLL to have K=2048
 * and M=2^10. Free feel to replace these parameters accordingly. */
khll AS (
  /* Keep only the k lowest hashes of values, and join them with all IDs
   * associated to them. */
  SELECT
    khashes.h AS h,
    HLL_COUNT.INIT(ids.id, 10) AS hll
  FROM (
      SELECT FARM_FINGERPRINT(value) AS h
      FROM source_table
      GROUP BY h
      ORDER BY h
      LIMIT 2048
    ) AS khashes
  LEFT JOIN (
      SELECT FARM_FINGERPRINT(value) AS h, id
      FROM source_table
    ) AS ids
  ON khashes.h = ids.h
  GROUP BY h
  ORDER BY h ASC
),
num_hashes AS (
  /* Total number of hashes in the sketch. */
  SELECT COUNT(*) FROM khll
),
estimated_num_values AS (
  /* Estimated number of unique values in the database. See "On Synopses for
   * Distinct-Value Estimation Under Multiset Operations", by Kevin Beyer, Peter
   * J. Haas, Berthold Reinwald, Yannis Sismanis, and Rainer Gemulla. Note that
   * the formula has to be adapted for hashes stored on int64 and not uint32. */
  SELECT
    IF((SELECT * FROM num_hashes) < 2048,
      (SELECT * FROM num_hashes),
      (SELECT (2048-1) * (POW(2, 64) / MAX(h)+1+POW(2, 63)) FROM khll))
),
value_sampling_ratio AS (
  /* Estimated ratio of values that we captured in the sketch. */
  SELECT LEAST(1, 2048 / (SELECT * FROM estimated_num_values))
),
estimated_uniqueness_distribution AS (
  /* How many values associate with individual uniqueness levels. */
  SELECT
    HLL_COUNT.EXTRACT(hll) AS uniqueness,
    COUNT(*) / (SELECT * FROM value_sampling_ratio) AS estimated_value_count,
    COUNT(*) / (SELECT * FROM num_hashes) AS estimated_value_ratio
  FROM khll
  GROUP BY uniqueness
  ORDER BY uniqueness ASC
)
/* How many values associate with *upto* individual uniqueness levels. */
SELECT
  uniqueness,
  SUM(estimated_value_count) OVER(
      ORDER BY uniqueness ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS cumulative_value_count,
  SUM(estimated_value_ratio) OVER(
      ORDER BY uniqueness ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS cumulative_value_ratio
FROM estimated_uniqueness_distribution
