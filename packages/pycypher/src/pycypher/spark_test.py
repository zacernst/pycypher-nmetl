from pyspark.sql import SparkSession
from pyspark.sql import functions as F, col, countDistinct

MAX_SIGNAL_IDS = 128
MAX_PARTITIONS = 2000

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("SignalRepartitioner") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.ContainerCredentialsProvider") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.hadoop.fs.s3a.committer.name", "magic") \
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true") \
        .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", 
                "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory") \
        .config("spark.sql.shuffle.partitions", f"{MAX_PARTITIONS}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()

    # This is in place of reading the whole table. We read one day and take a small sample of rows
    all_data_df = spark.read.parquet("s3a://deva-signal-prod/signal-archive/") \
            .filter(
                (col('year') == 2026) &
                (col('month') == 2) &
                (col('day') == 8) &
                (col('hour') == 12)
            ) \
            .sample(.01)

    out_df = all_data_df.select(
        col('year'), col('month'), col('day'), col('hour'), col('signal_uuid'), col('signal_value')) \
            .groupBy('signal_uuid') \
            .agg(
                countDistinct('signal_value').alias('unique_values')
            ) \
            .filter(col('unique_values') > 1) \
            .select(col('signal_uuid'), col('unique_values')
        ).sort('unique_values', ascending=False).limit(MAX_SIGNAL_IDS) \
        .join(all_data_df, on=['signal_uuid'], how='left') \
        .select(
            F.array(
                'window_start_time_ms',
                'signal_value'
            ).alias('time_value')
        ) \
        .groupBy('signal_uuid') \
        .collect_list('time_value') \
        .alias('signal_value_list')
    out_df.write.mode('overwrite').parquet('s3://deva-signal-prod/ztest/combined_signals_test_df.parquet')