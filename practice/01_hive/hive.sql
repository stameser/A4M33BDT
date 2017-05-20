/*
  Create temporary external table
*/

create external table pocasi_tmp (
	stanice string,
	mesic int,
	den int,
	hodina int,
	teplota double,
	flag string,
	latitude double,
	longitude double,
	vyska double,
	stat string,
	nazev string)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH "/user/pascepet/teplota-usa/*" INTO TABLE pocasi_tmp

/*
  Create external table stored as parquet
*/

CREATE EXTERNAL TABLE IF NOT EXISTS pocasi (
  stanice string,
  mesic int,
  den int,
  hodina int,
  teplota double,
  flag string,
  latitude double,
  longitude double,
  vyska double,
  stat string,
  nazev string
)
STORED AS parquet;

/*
  Load data into table and convert Kelvins to Celsius
 */
INSERT OVERWRITE TABLE pocasi
SELECT
  stanice,
  mesic,
  den,
  hodina,
  ((teplota  / 10) - 32) * 5/9,
  flag,
  latitude,
  longitude,
  vyska,
  stat,
  nazev
FROM pocasi_tmp where mesic is not NULL;

/*
  Find state with highest temp in summer
*/
SELECT
  sub.stat,
  avg(sub.teplota) as avg_teplota
FROM (
  SELECT
    teplota,
    stat
  FROM pocasi
  WHERE mesic in (6, 7, 8)) sub
GROUP BY sub.stat
ORDER BY avg_teplota DESC
limit 1;

/*
  Highest average temperature per season
  state | season | avg_temp
*/
SELECT stat, sezona, avg_teplota
FROM (SELECT
        stat,
        sezona,
        avg_teplota,
        RANK() OVER (PARTITION BY sezona ORDER BY avg_teplota DESC) AS r
      FROM (
             SELECT
               avg(teplota) AS avg_teplota,
               stat,
               sezona
             FROM (
                    SELECT
                      CASE WHEN mesic IN (1, 2, 12)
                        THEN "zima"
                      WHEN mesic IN (3, 4, 5)
                        THEN "jaro"
                      WHEN mesic IN (6, 7, 8)
                        THEN "leto"
                      WHEN mesic IN (9, 10, 11)
                        THEN "podzim"
                      END AS sezona,
                      teplota,
                      stat
                    FROM pocasi) sub
             GROUP BY stat, sezona) sezona_avg) sezona_rank
WHERE r = 1