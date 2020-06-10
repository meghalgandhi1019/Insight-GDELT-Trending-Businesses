# import gdelt_v1_schema
# import gdelt_v2_schema
import gdelt_schemas
from spark_job_functions import SparkJobFunctions
from database_functions import DatabaseFunctions

if __name__ == "__main__":

    sparkjob = SparkJobFunctions()
    dbfunction = DatabaseFunctions()

    # Attache schema to original data and read from S3 bucket
	# gdelt_df_1 = sparkjob.sql_read_gdelt(sparkjob.bucket1_gdelt,
    #                                      gdelt_schemas.schemaGdelt)

    gdelt_df_2 = sparkjob.sql_read(sparkjob.bucket2_gdelt, gdelt_schemas.schemaGdelt)
    gdelt_df_mentions = sparkjob.sql_read(sparkjob.bucket3_gdelt_mentions,
                                                gdelt_schemas.schemaMentionsGdelt)

    # Filter GDELT events and mentions data
    filtered_gdelt_v2_df = sparkjob.filter_gdelt_events(gdelt_df_2)
    filtered_mentions_df = sparkjob.filter_gdelt_mentions(gdelt_df_mentions)

    result_df = sparkjob.join_df(filtered_gdelt_v2_df, filtered_mentions_df)

	# dbfunction.write_to_db(gdelt_df_1)
	# dbfunction.write_to_db(gdelt_df_2)
    # dbfunction.write_to_db(gdelt_df_mentions)
