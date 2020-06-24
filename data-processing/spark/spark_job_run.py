import gdelt_schemas
from spark_job_functions import SparkJobFunctions
from database_functions import DatabaseFunctions

if __name__ == "__main__":

    sparkjob_batch = SparkJobFunctions()
    dbfunction = DatabaseFunctions()

    # 1. Read historical gdelt events data sets, attach schema and combine them.
    gdelt_df_events_old = sparkjob_batch.sql_read(sparkjob_batch.bucket_gdelt_old_events,
                                         gdelt_schemas.schemaGdelt)
    gdelt_df_events_new = sparkjob_batch.sql_read(sparkjob_batch.bucket_events_2019_onwards,
                                         gdelt_schemas.schemaGdelt)
    gdelt_combined_events_df = sparkjob_batch.combine_dfs(gdelt_df_events_old, gdelt_df_events_new)

    # 2. Read historical gdelt mentions data sets, attach schema and combine them.
    gdelt_df_mentions_old = sparkjob_batch.sql_read(sparkjob_batch.bucket_gdelt_old_mentions,
                                                gdelt_schemas.schemaMentionsGdelt)
    gdelt_df_mentions_new = sparkjob_batch.sql_read(sparkjob_batch.bucket_mentions_2019_onwards,
                                                gdelt_schemas.schemaMentionsGdelt)
    gdelt_combined_mentions_df = sparkjob_batch.combine_dfs(gdelt_df_mentions_old, gdelt_df_mentions_new)

    # 3. Filter GDELT events and mentions data
    filtered_gdelt_events_df = sparkjob_batch.filter_gdelt_events(gdelt_combined_events_df)
    filtered_mentions_df = sparkjob_batch.filter_gdelt_mentions(gdelt_combined_mentions_df)

    # 4. Join both dataset on common column: Globaleventid
    joined_df = sparkjob_batch.join_df(filtered_gdelt_v2_df, filtered_mentions_df)

    # 5. cached the joined result as it'll speed up following spark actions
    joined_df.cache()

    # 6. Perform group by year, month year, actortype etc and other aggregation functions on them.
    result_df = sparkjob_batch.aggregate_job(joined_df)

    # 7. Write above summarised results to PostgreSQL
    dbfunction.write_to_db(result_df)
