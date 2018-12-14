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
  /* Base cardinality for experiment. Should be >> K=2048.
   * And the number of seeds i.e., number of estimation rounds.
   * Note: one can reduce the number of seeds and run the experiments in parts
   * (with a different seed prefix) in case BigQuery runs out of memory. */
  SELECT
    100000 as cardinality,
    101 as num_seeds
),
seeds AS (
  /* Create seed strings for running the estimation <num_seeds> times. */
  SELECT CONCAT('s-', CAST(s AS STRING)) as seed
  FROM UNNEST(GENERATE_ARRAY(1, (select num_seeds from constants))) AS s
),
intervals AS (
  /* Intervals on which we're computing KMV sketches. To compute containment
   * estimation at actual containment 50%, when one cardinality is fixed and the
   * other varies, we use intervals [1, cardinality], and we compare it with
   * [cardinality/2+1, cardinality], [cardinality/2+1, 2*cardinality], etc.,
   * until [cardinality/2+1, 20*cardinality]. */
  SELECT
    1 AS start,
    (SELECT cardinality FROM constants) AS finish
  UNION ALL
  SELECT
    (SELECT cardinality FROM constants)/2+1 AS start,
    i*(SELECT cardinality FROM constants)/2 AS finish
  FROM UNNEST(GENERATE_ARRAY(2, 40, 2)) AS i
),
nums AS (
  /* Generate experiment data i.e., number in the range of [1, cardinality*20].
   * We could not simply GENERATE_ARRAY(1, cardinality*20) as there is
   * a limit in BigQuery on the size of dynamically created array. */
  SELECT CAST(a * (SELECT cardinality/1000 FROM constants) + b as INT64) AS num
  FROM UNNEST(GENERATE_ARRAY(0, 20*1000-1)) a
  CROSS JOIN UNNEST(
    GENERATE_ARRAY(1, (SELECT cardinality/1000 FROM constants))) b
),
hashes AS (
  /* Hash every value with each seed. */
  SELECT
    nums.num AS i,
    seed,
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
  GROUP BY DIV(i-1, (SELECT CAST(cardinality/2 AS INT64) FROM constants)), seed
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
    (b.finish - a.start + 1)/(b.finish - b.start + 1) AS real_containment,
    (a.finish - a.start + 1)/(b.finish - b.start + 1) AS cardinality_ratio,
    a.kmv[ORDINAL(2048)] AS leftLastHash,
    b.kmv[ORDINAL(2048)] AS rightLastHash,
    ARRAY(
      SELECT h
      FROM UNNEST(ARRAY_CONCAT(a.kmv,b.kmv)) as h
      GROUP BY h
      ORDER BY h
      LIMIT 2048
    )[ORDINAL(2048)] AS mergedLastHash
  FROM (SELECT * FROM intervals_kmvs WHERE start != 1) a
  CROSS JOIN (SELECT * FROM intervals_kmvs WHERE start = 1) b
  WHERE a.seed = b.seed
),
estimated_cardinalities AS (
  /* Compute the cardinality estimation for all KMV sketches considered. */
  SELECT
    seed,
    real_containment,
    cardinality_ratio,
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
    cardinality_ratio,
    GREATEST(
      (leftCardinality+rightCardinality-mergedCardinality)/rightCardinality, 0)
    AS estimated_containment
  FROM estimated_cardinalities
)
SELECT
  real_containment,
  cardinality_ratio,
  ARRAY_AGG(estimated_containment ORDER BY estimated_containment)[
    OFFSET((SELECT CAST(num_seeds/100*5 AS INT64) FROM constants))] AS pc5,
  ARRAY_AGG(estimated_containment ORDER BY estimated_containment)[
    OFFSET((SELECT CAST(num_seeds/100*50 AS INT64) FROM constants))] AS median,
  ARRAY_AGG(estimated_containment ORDER BY estimated_containment)[
    OFFSET((SELECT CAST(num_seeds/100*95 AS INT64) FROM constants))] AS pc95
FROM estimated_containments
GROUP BY real_containment, cardinality_ratio
ORDER BY real_containment, cardinality_ratio
