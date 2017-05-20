from pyspark.sql import functions as sqlf

"""
For each customer (frommsisdn) compute number of calls per day of week per record_type

frommsisdn | callForwarding_Monday | ... | mSTerminating_Sunday
"""

# read DataFrame from parquet file
df = sqlContext.read.parquet('spark_sql/cdr_sample/')

# add day of week column
df = df.withColumn('dayofweek',
                   sqlf.date_format(sqlf.from_unixtime(sqlf.unix_timestamp(df['date_key'], 'yyyyMMdd')), 'EEEE'))

# group data by calling number (frommsisdn) and pivot them by record type. 
# frommsisdn | #originating | #terminating | #forwarding

df = df.withColumn('pivot_col', sqlf.concat(df['record_type'], sqlf.lit('_'), df['dayofweek']))

metrics = df.groupby('frommsisdn').pivot('pivot_col').agg(sqlf.count('*'))

# create column with total counts
total_col = sum([metrics[col] for col in metrics.columns[1:]])

# frommsisdn | %_originating | %_terminating | %_forwarding
shares_df = metrics.select(
    ['frommsisdn'] + [(metrics[col] / total_col).alias(col + '_share') for col in metrics.columns[1:]])
