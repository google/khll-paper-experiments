# KHLL Accuracy Experiments

This repository contains the code used for the accuracy experiments in the paper
*KHyperLogLog: Estimating Reidentifiability and Joinability of Large Data at
Scale*.

Note: The KHLL algorithm has been implemented in a proprietary environment, and
is not (yet) available on BigQuery. In this repository, we simulate how the KHLL
algorithm can be used to estimate `uniqueness` and `containment` using standard
SQL operations and existing algorithms such as HLL and KMV which KHLL builds on.

## Accuracy of Reidentifiability (uniqueness)

This section measures the accuracy of the estimated uniqueness distribution.

### Preparing the data

The 2010 US census data is readily available in BigQuery. We preprocess the data
so to estimate the population of individual age (rather than age buckets), and
create a pseudo-identifier for every person in the population.

```sql
WITH
census AS (
  SELECT
    zipcode,
    minimum_age AS min_age,
    IFNULL(maximum_age, 92) AS max_age,
    gender,
    population
  FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010`
  WHERE zipcode IS NOT NULL AND minimum_age IS NOT NULL AND gender != ""
),
census_by_age AS (
  SELECT
    census.zipcode AS zipcode,
    age,
    census.gender AS gender,
    CAST(CEIL(
      census.population/(census.max_age - census.min_age + 1)) AS INT64)
    AS estimated_population
  FROM census
  LEFT JOIN UNNEST(GENERATE_ARRAY(census.min_age, census.max_age)) as age
)
SELECT
  zipcode,
  age,
  gender,
  FORMAT("%s-%d-%s-%d", zipcode, age, gender, index) AS pseudo_identifier
FROM census_by_age
LEFT JOIN UNNEST(GENERATE_ARRAY(1, estimated_population)) AS index
```

Save the result of this query in a table of your choosing.

The source data from the Netflix Prize must be downloaded from
[Kaggle](https://www.kaggle.com/netflix-inc/netflix-prize-data). Use the
following Python script to convert the `combined_data_*.txt` files into a CSV
table, and upload it to BigQuery.

```
#!/bin/python3
import re

print("movie_id,user_id,rating,date")
f = open('data.txt')
line = f.readline()
currentID = ''
while line:
    match = re.search(r'(\d*):', line)
    if match:
        currentID = match.group(1)
    else:
        print("%s,%s" % (currentID, line[:-1]))
    line = f.readline()
```

### Running the experiments

The SQL query that computes the exact cumulative uniqueness distribution is in
`uniqueness_exact.sql` while the SQL query that computes the KHLL estimation is
in `uniqueness_khll.sql`.

To run the above two queries, fill in the `id` and `value` parameters:

-   for the US census, `value` should be `[zipcode, CAST(age AS STRING)]`, and
    `id` should be `pseudo_identifier`;
-   for Netflix, `value` should be either `[CAST(movie_id AS STRING)]`, or
    `[CAST(movie_id AS STRING), CAST(date AS STRING)]`, and the `id` should be
    `user_id`.

The exact and estimated cumulative distributions can be compared using the
following query:

```sql
SELECT
  IFNULL(khll.uniqueness, exact.uniqueness) AS uniqueness,
  khll.cumulative_value_ratio AS khll_cumulative_value_ratio,
  exact.cumulative_value_ratio AS exact_cumulative_value_ratio
FROM /* FIXME <khll estimated results> */ khll
FULL OUTER JOIN /* FIXME <exact results> */ exact
ON khll.uniqueness = exact.uniqueness
ORDER BY uniqueness
```

You can then export the results to CSV and draw the graph with the visualization
tool of your choice.

To estimate the uniqueness distribution for other values of K, you can run the
query in `uniqueness_khll.sql`, replacing 2048 by the desired value everywhere
in the query, and join the results obtained with a query similar to the above.

## Accuracy of Joinability (containment)

The containment estimation experiments are done in two steps: first, generate
the containment estimation on the same data with many different seeds. Since all
values are hashed, this is equivalent to taking random subsets of input data.
Second, compute the desired percentiles of containment estimation.

The SQL query that estimates the containment of two sets with the same
cardinality (but different containment ratios) can be found in
`containment_equal_cardinalities.sql`, while the SQL query that estimates the
containment ratio of two sets of varying cardinalities (but fixed containment
ratio of 50%) can be found in `containment_varying_cardinalities.sql`.

The experiment is simulated based on the KMV algorithm which KHLL builds on.

## Disclaimer

This is not an officially supported Google product.
