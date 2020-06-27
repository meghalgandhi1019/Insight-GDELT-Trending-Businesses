from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col, expr, when, broadcast
import configparser

class SparkJobFunctions(object):

	def __init__(self):
		'''
		It'll initialise spark context and spark sql context with provided spark configurations.
		These variables are initialised once and being used by whole spark job functions afterwards.
		'''
		# Read the property file.
		config = configparser.RawConfigParser()
		config.read('/usr/local/GDELT-Business_Influencers/src/spark/config.properties')
		spark_master = config.get('SparkConfig', 'spark.master')

		self.__session = SparkSession.builder \
			.master(spark_master) \
			.appName('GDELT-Trendustries') \
			.config('spark.jars', '/usr/local/spark/jars/postgresql-42.2.13.jar') \
			.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
			.getOrCreate()

		self.sqlcontext = SQLContext(self.__session)

		# Spark batch processing : S3 bucket addresses of GDELT data sets.
		self.bucket_gdelt_old_events = 's3a://gdelt-open-data/events/*.export.csv'
		self.bucket_gdelt_old_mentions = 's3a://gdelt-open-data/v2/mentions/*.mentions.csv'
		self.bucket_events_2019_onwards = 's3a://gdelt-2019-onwards/v2_events_2019_onwards/'
		self.bucket_mentions_2019_onwards = 's3a://gdelt-2019-onwards/mentions_2019_onwards/'

		# S3 bucket addresses for writing parquet files.
		self.bucket_parquet_events = 's3a://gdelt-parquet/v2_events/'
		self.bucket_parquet_mentions = 's3a://gdelt-parquet/mentions_data/'

	def sql_read(self, s3bucket, schema):
		'''
		This function is used to read GDELT data sets.
		:param s3bucket: S3 bucket address from where GDELT csv data will be loaded
		:param schema: Attach schema to GDELT data to make it structured and labelled
		:return: Labelled Gdelt data will be returned as dataframe
		'''

		gdelt_df = self.sqlcontext.read \
			.format('com.databricks.spark.csv') \
			.option('header', 'false') \
			.option('delimiter', "\t") \
			.option("mode", "DROPMALFORMED") \
			.schema(schema) \
			.load(s3bucket) \

		return gdelt_df

	def combine_dfs(self, input_df1, input_df2):
		'''
		This function is used to combine old and new gdelt historical data sets
		:param input_df1: GDELT old dataframe
		:param input_df2: GDELT new dataframe
		:return: Combine version of both input dataframes will be returned
		'''

		combined_df = input_df1.union(input_df2)

		return combined_df

	def filter_gdelt_events(self, df):
		'''
		This function is used to filter GDELT events data based on county 'US' and event's actor type
		:param df: structured and labelled gdelt events data frame will be provided
		:return: filtered event data frame will be returned
		'''

		df.registerTempTable("GdeltEventsTable")

		# Extract state and filter dataframe by interested business sectors and country 'US'.
		# Standardize GoldsteinScale using min and max values -10 and 10 respectively. Convert into percentage(%).
		events_df = self.sqlcontext.sql("SELECT GLOBALEVENTID, SQLDATE, MonthYear, Year, Actor1Code, Actor1Type1Code, "
										"Actor1Geo_CountryCode, Actor1Geo_ADM1Code, substring(Actor1Geo_ADM1Code, 3,2) state, "
										"GoldsteinScale, CAST((GoldsteinScale + 10) * 100 / 20 AS DOUBLE) GoldsteinScale_norm "
										"FROM GdeltEventsTable where  Actor1Geo_CountryCode='US' and "
										"Actor1Code != 'null' and "
										"Actor1Type1Code in ('COP', 'GOV', 'AGR', 'BUS', 'DEV', 'EDU', 'ENV', 'HLH', 'MED', 'NGO')")

		# Filter records where state is blank
		events_df = events_df.filter(events_df.state != '')

		return events_df

	def filter_gdelt_mentions(self, df):
		'''
		This function used to perform filtering and Group by on GlobalEventID & calculate avg confidence on mentions table.
		:param df: structured and labelled gdelt mentions data frame will be provided
		:return: filtered mentions data frame will be returned.
		'''
		df.registerTempTable("GdeltMentionsTable")

		# Filter to get required columns only #
		mentions_df = self.sqlcontext.sql("SELECT GLOBALEVENTID, avg(Confidence) as Avg_Confidence FROM GdeltMentionsTable GROUP BY GLOBALEVENTID")

		return mentions_df

	def join_df(self, events_df, mentions_df):
		'''
		This function is used to perform joining of mentions data with events data. Used Spark's broadcast join.
		:param events_df: filtered events data frame will be provided.
		:param mentions_df: filtered mentions data frame will be provided
		:return: joined data frame including GlobalEventId, Goldstein score and avg confidence will be returned.
		'''

		# Broadcast events data frame as it's smaller in size and perform join.
		final_df = mentions_df.join(broadcast(events_df), 'GLOBALEVENTID')

		return final_df

	def aggregate_job(self, df_joined):
		'''
		This function is used to perform group by and aggregation on joined data frame
		:param df_joined: joined events and mentions data frame will be provided
		:return: aggregated data frame will be returned.
		'''

		df_finalized = df_joined.groupby('state', 'Year', 'MonthYear', 'Actor1Type1Code') \
			.agg(
				functions.approx_count_distinct('GLOBALEVENTID').alias('events_count'),
				functions.sum('GoldsteinScale_norm').alias('GoldsteinScale_norm_sum'),
				functions.sum('Avg_Confidence').alias('confidence_sum')
				)
		df_finalized.show()
		return df_finalized

	def write_s3_parquet_events(self, events_df, s3bucket):
		'''
		This function is used to add additional columns and write events data frame in parquet format on S3 bucket.
		:param events_df: structured and labelled events data frame will be provided.
		:return: Nothing returned. Data will be written successfully.
		'''
		# Add required columns
		new_events_df = events_df.withColumn('Month', expr('substring(SQLDATE, 5, 2)')) \
			.withColumn('Day', expr('substring(SQLDATE, 7, 2)'))

		# Write to parquet in S3 with partitions
		cols = ['Year', 'Month', 'Day']
		new_events_df.repartition(*cols)\
			.write.mode('append')\
			.option("mode", "DROPMALFORMED")\
			.partitionBy(cols)\
			.parquet(s3bucket)
		print('..............................Written successfully on S3 parguet-events............................')
		return

	def write_s3_parquet_mentions(self, mentioned_df, s3bucket):
		'''
		This function is used to add additional columns and write mentions data frame in parquet format on S3 bucket.
		:param mentioned_df: structured and labelled mentions data frame will be provided.
		:return: Nothing returned. Data will be written successfully.
		'''

		# Add required columns
		new_filtered_mentioned_df = mentioned_df.withColumn('Year', expr('substring(MentionTimeDate, 1, 4)')) \
			.withColumn('Month', expr('substring(MentionTimeDate, 5, 2)')) \
			.withColumn('Day', expr('substring(MentionTimeDate, 7, 2)')) \
			.withColumn('Hour', expr('substring(MentionTimeDate, 9, 2)')) \
			.withColumn('Minute', expr('substring(MentionTimeDate, 11, 2)'))

		# Write to parquet in S3
		cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
		new_filtered_mentioned_df.repartition(*cols)\
			.write.mode('append')\
			.option("mode", "DROPMALFORMED")\
			.partitionBy(cols)\
			.parquet(s3bucket)
		print('..............................Written successfully on S3 parguet - mentions..........................')
		return


	def sql_read_parquet(self, s3bucket):
		'''
		This function is used to read parquet files from S3 bucket.
		:param s3bucket: Address of parquet S3 bucket
		:return: parquet data frame will be returned
		'''

		gdelt_parquet_df = self.sqlcontext.read.parquet(s3bucket)

		return gdelt_parquet_df


	def filter_parquet_events(self, df, year, month):
		'''
		This function is used to filter events df. Used parquet partitions
		:param df: parquet events df will be provided
		:param year: use partition parameter 'year' for filtering
		:param month: use partition parameter 'month' for filtering
		:return: filtered df will be returned
		'''
		df.registerTempTable("GdeltEventsParquet")

		# Take an advantage of partitions
		events_parquet_df =  self.sqlcontext.sql('SELECT * from GdeltEventsParquet WHERE Year=' + year + ' and Month=' + month)

		events_parquet_df.registerTempTable("FilteredParquetEvents")

		# Extract state and filter dataframe by interested business sectors, country 'US' and .
		# Standardize GoldsteinScale using min and max values -10 and 10 respectively. Convert into percentage(%).
		new_events_parquet_df = self.sqlcontext.sql("SELECT GLOBALEVENTID, SQLDATE, MonthYear, Year, Actor1Code, Actor1Type1Code, "
										"Actor1Geo_CountryCode, Actor1Geo_ADM1Code, substring(Actor1Geo_ADM1Code, 3,2) state, "
										"GoldsteinScale, CAST((GoldsteinScale + 10) * 100 / 20 AS DOUBLE) GoldsteinScale_norm "
										"FROM FilteredParquetEvents WHERE  Actor1Geo_CountryCode='US' and "
										"Actor1Code != 'null' and "
										"Actor1Type1Code in ('COP', 'GOV', 'AGR', 'BUS', 'DEV', 'EDU', 'ENV', 'HLH', 'MED', 'NGO')")

		# Filter records where state is blank
		new_events_parquet_df = new_events_parquet_df.filter(new_events_parquet_df.state != '')

		return new_events_parquet_df

	def filter_parquet_mentions(self, df, year, month):
		'''
		This function is used to filter mentions df. Used parquet partitions
		:param df: parquet mentions df will be provided
		:param year: use partition parameter 'year' for filtering
		:param month: use partition parameter 'month' for filtering
		:return: filtered df will be returned
		'''

		df.registerTempTable("GdeltMentionsParquet")

		# Take an advantage of partitions
		mentions_parquet_df = self.sqlcontext.sql('SELECT * from GdeltMentionsParquet WHERE Year=' + year + ' and Month=' + month)

		mentions_parquet_df.registerTempTable("FilteredParquetMentions")
		# Get avg confidence for GlobalEventId
		new_mentions_parquet_df = self.sqlcontext.sql("SELECT GLOBALEVENTID, avg(Confidence) as Avg_Confidence FROM FilteredParquetMentions GROUP BY GLOBALEVENTID")

		return new_mentions_parquet_df






