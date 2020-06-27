import sys
from datetime import datetime
import gdelt_schemas
from spark_job_functions import SparkJobFunctions
from database_functions import DatabaseFunctions

if __name__ == "__main__":

    sparkjob_parquet = SparkJobFunctions()
    dbfunction = DatabaseFunctions()

    # Get the year and month from command line arguement
    cli_date = datetime.strptime(sys.argv[1], '%Y-%m-%d %H:%M:%S')
    year = str(cli_date.year)
    month = str(cli_date.month)
    year_month_requested = year + month

    # 1. Read gdelt events data based on provided year and month. Write as parquet
    gdelt_df_events = sparkjob_parquet.sql_read(sparkjob_parquet.bucket_events_2019_onwards + year_month_requested + '*',
                                                gdelt_schemas.schemaGdelt)

    sparkjob_parquet.write_s3_parquet_events(gdelt_df_events, sparkjob_parquet.bucket_parquet_events)

    # 2. Read gdelt mentions data based on provided year and month. Write as parquet
    gdelt_df_mentions = sparkjob_parquet.sql_read(sparkjob_parquet.bucket_mentions_2019_onwards + year_month_requested + '*',
                                                  gdelt_schemas.schemaMentionsGdelt)

    sparkjob_parquet.write_s3_parquet_mentions(gdelt_df_mentions, sparkjob_parquet.bucket_parquet_mentions)

    # 3. Read gdelt parquet data
    gdelt_df_events_parquet = sparkjob_parquet.sql_read_parquet(sparkjob_parquet.bucket_parquet_events)
    gdelt_df_mentions_parquet = sparkjob_parquet.sql_read_parquet(sparkjob_parquet.bucket_parquet_mentions)

    # 4. Filter GDELT parquet events and mentions data
    filtered_events_parquet_df = sparkjob_parquet.filter_parquet_events(gdelt_df_events_parquet, year, month)
    filtered_mentions_parquet_df = sparkjob_parquet.filter_parquet_mentions(gdelt_df_mentions_parquet, year, month)

    # 5. Join both data set on common column: GlobalEventId
    joined_df = sparkjob_parquet.join_df(filtered_events_parquet_df, filtered_mentions_parquet_df)

    # 6. cached the joined result as it'll speed up following spark actions
    joined_df.cache()

    # 7. Perform group by year, month year, actortype etc and other aggregation functions on them.
    result_df = sparkjob_parquet.aggregate_job(joined_df)

    # 8. Write above summarised results to PostgreSQL
    dbfunction.write_to_db(result_df)

