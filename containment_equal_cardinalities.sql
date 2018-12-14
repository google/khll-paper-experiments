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
constants AS (
  /* Fixed cardinality for experiment. Should be >> K=2048.
   * And number of seeds i.e., number of estimation rounds.
   * Note: one can reduce the number of seeds and run the experiments in parts
   * (with a different seed prefix) in case BigQuery runs out of memory. */
  SELECT
    10000000 as cardinality,
    101 as num_seeds
),
seeds AS (
  /* Create seed strings for running the estimation <num_seeds> times. */
  SELECT CONCAT('s-', CAST(s AS STRING)) as seed
  FROM UNNEST(GENERATE_ARRAY(1, (select num_seeds from constants))) AS s
),
intervals AS (
  /* Intervals on which we compute KMV sketches.
   * To compute containment estimation at equal cardinalities, and for
   * fixed containments ratio of 0%, 10%, and so on, we define intervals
   * from [1, cardinality], [cardinality/10+1, 11*cardinality/10], ...,
   * until [cardinality+1, 2*cardinality]. */
  SELECT
    i*(SELECT cardinality FROM constants)/10+1 AS start,
    (10+i)*(SELECT cardinality FROM constants)/10 AS finish
  FROM UNNEST(GENERATE_ARRAY(0, 10)) AS i
),
nums AS (
  /* Generate experiment data i.e., number in the range of [1, cardinality*2].
   * We could not simply GENERATE_ARRAY(1, cardinality*2) as there is
   * a limit in BigQuery on the size of dynamically created array. */
  SELECT CAST(a * (SELECT cardinality/1000 FROM constants) + b as INT64) AS num
  FROM UNNEST(GENERATE_ARRAY(0, 2*1000-1)) a
  CROSS JOIN UNNEST(
    GENERATE_ARRAY(1, (SELECT cardinality/1000-1 FROM constants))) b
),
hashes AS (
  /* Hash every value with each seed. */
  SELECT
    nums.num AS i,
    seed AS seed,
    FARM_FINGERPRINT(CONCAT(CAST(num AS STRING), seed)) AS h
  FROM nums
  CROSS JOIN seeds
),
intermediary_kmvs AS (
  /* Intermediary KMV sketches. This allows for efficient computation of the
   * interval KMVs (given both coding and memory restriction in SQL). */
  SELECT
    seed,
    MIN(i) AS start,
    MAX(i) AS finish,
    ARRAY_AGG(h ORDER BY h LIMIT 2048) AS kmv
  FROM hashes
  GROUP BY DIV(i-1, (SELECT CAST(cardinality/10 AS INT64) FROM constants)), seed
  ORDER BY start
),
intervals_kmvs AS (
  /* Merge intermediary sketches to obtain KMV sketches for each interval. */
  SELECT
    kmvs.seed,
    intervals.start,
    intervals.finish,
    ARRAY_AGG(DISTINCT h ORDER BY h LIMIT 2048) AS kmv
  FROM intermediary_kmvs kmvs, UNNEST(kmv) AS h
  CROSS JOIN intervals
  WHERE intervals.start <= kmvs.start
    AND kmvs.finish <= intervals.finish
  GROUP BY intervals.start, intervals.finish, kmvs.seed
),
last_hashes AS (
  /* Merge the first KMV sketch with every other sketch. And get the
   * K=2048th smallest hash of all sketches to estimate containment. */
  SELECT
    a.seed,
    1 - ((a.start-1) / (SELECT cardinality FROM constants)) AS real_containment,
    a.kmv[ORDINAL(2048)] AS leftLastHash,
    b.kmv[ORDINAL(2048)] AS rightLastHash,
    ARRAY(
      SELECT h
      FROM (SELECT h FROM UNNEST(ARRAY_CONCAT(a.kmv, b.kmv)) AS h)
      GROUP BY h
      ORDER BY h
      LIMIT 2048
    )[ORDINAL(2048)] AS mergedLastHash
  FROM intervals_kmvs a
  CROSS JOIN (SELECT * FROM intervals_kmvs WHERE start = 1) b
  WHERE a.seed = b.seed
),
estimated_cardinalities AS (
  /* Compute the cardinality estimation for all KMV sketches considered. */
  SELECT
    seed,
    real_containment,
    (2048-1) * (POW(2,64) / (POW(2,63)+1+leftLastHash)) AS leftCardinality,
    (2048-1) * (POW(2,64) / (POW(2,63)+1+rightLastHash)) AS rightCardinality,
    (2048-1) * (POW(2,64) / (POW(2,63)+1+mergedLastHash)) AS mergedCardinality
  FROM last_hashes
),
estimated_containments AS (
  /* Estimate containment from cardinality estimations, and compare it
   * to the real containment ratio. */
  SELECT
    seed,
    real_containment,
    GREATEST(
      (leftCardinality+rightCardinality-mergedCardinality)/leftCardinality, 0)
    AS estimated_containment
  FROM estimated_cardinalities
)
SELECT
  real_containment,
  ARRAY_AGG(estimated_containment ORDER BY estimated_containment)[
    OFFSET((SELECT CAST(num_seeds/100*5 AS INT64) FROM constants))] AS pc5,
  ARRAY_AGG(estimated_containment ORDER BY estimated_containment)[
    OFFSET((SELECT CAST(num_seeds/100*50 AS INT64) FROM constants))] AS median,
  ARRAY_AGG(estimated_containment ORDER BY estimated_containment)[
    OFFSET((SELECT CAST(num_seeds/100*95 AS INT64) FROM constants))] AS pc95
FROM estimated_containments
GROUP BY real_containment
ORDER BY real_containment
