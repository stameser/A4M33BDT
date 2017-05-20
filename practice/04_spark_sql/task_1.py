from pyspark.sql import functions as sqlf

"""
For each customer (frommsisdn) compute number of calls per record_type and join this dataframe with fraud data frame.

frommsisdn | callForwarding | mSOriginating | mSTerminating | is_fraud

"""
# create DataFrame from csv file(s)
rdd = sc.textFile('spark_sql/fraud_cases/*')
rdd = rdd.map(lambda x: x.split(','))
fraud_df = sqlContext.createDataFrame(rdd, ['frommsisdn', 'is_fraud'])

# read DataFrame from parquet file
df = sqlContext.read.parquet('spark_sql/cdr_sample/')

# group data by calling number (frommsisdn) and pivot them by record type. 
# frommsisdn | #originating | #terminating | #forwarding
metrics = df.groupby('frommsisdn').pivot('record_type', ['callForwarding', 'mSOriginating', 'mSTerminating']).agg(
    sqlf.count('*'))

# create column with total counts
total_col = metrics['callForwarding'] + metrics['mSOriginating'] + metrics['mSTerminating']

# frommsisdn | %_originating | %_terminating | %_forwarding
shares = metrics.select(['frommsisdn',
                         (metrics['callForwarding'] / total_col).alias('forward_share'),
                         (metrics['mSOriginating'] / total_col).alias('originating_share'),
                         (metrics['mSTerminating'] / total_col).alias('terminating_share'),
                         ])

# frommsisdn | %_originating | %_terminating | %_forwarding | is_fraud
# use select to remove duplicated column `frommsisdn`
metrics_with_labels = shares.join(fraud_df, 'frommsisdn').select([shares['frommsisdn'],
                                                                  shares['forward_share'],
                                                                  shares['originating_share'],
                                                                  shares['terminating_share'],
                                                                  fraud_df['is_fraud']
                                                                  ])
