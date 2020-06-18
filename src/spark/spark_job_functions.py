from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql import functions
from pyspark.sql.functions import col, expr, when

class SparkJobFunctions(object):

	def __init__(self):
		'''
		It'll initialise spark context and spark sql context with required configurations.

			'''
		self.__session = SparkSession.builder \
			.master('spark://ec2-52-13-56-58.us-west-2.compute.amazonaws.com:7077') \
			.appName('GDELT-Business-Influencers') \
			.config('spark.jars', '/usr/local/spark/jars/postgresql-42.2.13.jar') \
			.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
			.getOrCreate()
		#.config('spark.executor.instances', 11) \
		# .config('spark.executors.memory', '12gb') \

		self.sqlcontext = SQLContext(self.__session)
		# self.bucket1_gdelt = 's3n://gdelt-open-data/events/[1,2][0-9]*[0-9][0-3|9].csv'
		# self.bucket2_gdelt = 's3n://gdelt-open-data/events/*.export.csv'

		# self.bucket1_gdelt = 's3a://gdelt-open-data/events/1992.csv'
		# self.bucket2_gdelt = 's3a://gdelt-open-data/events/20190415.export.csv'
		# #self.bucket3_gdelt_mentions = 's3a://gdelt-open-data/v2/mentions/20190415[0-9][0-9][0-9][0-9][0-9][0-9].mentions.csv'
		# self.bucket3_gdelt_mentions = 's3a://gdelt-open-data/v2/mentions/20190415*'

		self.bucket1_gdelt = 's3a://gdelt-open-data/events/1992.csv'
		# self.bucket2_gdelt = 's3a://gdelt-open-data/events/201505*.export.csv'
		# # self.bucket3_gdelt_mentions = 's3a://gdelt-open-data/v2/mentions/20190415[0-9][0-9][0-9][0-9][0-9][0-9].mentions.csv'
		# self.bucket3_gdelt_mentions = 's3a://gdelt-open-data/v2/mentions/201505*.mentions.csv'

		#self.bucket2_gdelt = 's3a://gdelt-open-data/events/2019*.export.csv'
		# self.bucket3_gdelt_mentions = 's3a://gdelt-open-data/v2/mentions/20190415[0-9][0-9][0-9][0-9][0-9][0-9].mentions.csv'
		self.bucket3_gdelt_mentions = 's3a://gdelt-open-data/v2/mentions/2019*.mentions.csv'

	def sql_read(self, s3bucket, schema):
		'''
		Read GDELT data and return schema attached df
		'''
		# gdelt_text_data = self.__session.sparkContext.textFile(s3bucket)
		# gdelt_with_schema = gdelt_text_data.map(lambda c: c.split("\t")).map(lambda c: schema(*c))
		# gdelt_df = self.sqlcontext.createDataFrame(gdelt_with_schema)
		gdelt_df = self.sqlcontext.read \
			.format('com.databricks.spark.csv') \
			.option('header', 'false') \
			.option('delimiter', "\t") \
			.option("mode", "DROPMALFORMED") \
			.schema(schema) \
			.load(s3bucket)
		#gdelt_df.show(n=10)
		return gdelt_df

	def sql_read_with_repartition(self,  s3bucket, schema, repartiton_col):
		#gdelt_df = self.sqlcontext.read.csv(s3bucket, schema)
		gdelt_df = self.sqlcontext.read \
			    .format('com.databricks.spark.csv') \
	            .option('header','false') \
	            .option('delimiter',"\t") \
				.option("mode", "DROPMALFORMED") \
				.schema(schema) \
	            .load(s3bucket).repartition(repartiton_col)
		#gdelt_df.show(n=10)
		return gdelt_df

	def filter_gdelt_events(self, df):
		'''

		Filter GDELT events data. Normalise Goldestein score
		'''

		df.registerTempTable("GdeltEventsTable")
		#roles_of_interest = ["COP", "GOV", "AGR", "BUS", "DEV", "EDU", "ENV", "HLH", "MED", "NGO"]


		# Filter by US location only, Normalize GoldsteinScale(min = -10, max = 10) and filter Actor1Type1Code #
		events_df = self.sqlcontext.sql("select GLOBALEVENTID, SQLDATE, MonthYear, Year, Actor1Code, Actor1Type1Code, "
										"ActionGeo_CountryCode, ActionGeo_ADM1Code, substring(ActionGeo_ADM1Code, 3,2) state,Actor1Geo_CountryCode, "
										"GoldsteinScale, CAST((GoldsteinScale + 10) * 100 / 20 AS DOUBLE) GoldsteinScale_norm "
										"from GdeltEventsTable where  ActionGeo_CountryCode='US' and "
										"Actor1Code != 'null' and "
										"Actor1Type1Code in ('COP', 'GOV', 'AGR', 'BUS', 'DEV', 'EDU', 'ENV', 'HLH', 'MED', 'NGO')" )
		# events_df.show(n=20)
		events_df = events_df.filter(events_df.state != '')
		return events_df

	def filter_gdelt_mentions(self, df):
		'''
		Perform Groupby on a mentioned table uisng GlobalEventID.
		'''
		df.registerTempTable("GdeltMentionsTable")

		# Filter to get required columns only #
		# mentions_df = self.sqlcontext.sql("select GLOBALEVENTID, avg(Confidence) as Avg_Confidence from GdeltMentionsTable group by GLOBALEVENTID")
		mentions_df = self.sqlcontext.sql(
			"select GLOBALEVENTID, MentionTimeDate, Confidence from GdeltMentionsTable")

		#mentions_df.show(n=20)
		return mentions_df

	def write_s3_parquet_events(self, filtered_events_df):
		# partiton by state, Year, Month, Day
		new_filtered_events_df = filtered_events_df.withColumn('Month', expr('substring(SQLDATE, 5, 2)'))\
													.withColumn('Day', expr('substring(SQLDATE, 7, 2)'))

		# Write to parquet in S3
		# cols = ['state', 'Year', 'Month', 'Day']
		cols = ['Year', 'Month', 'Day']
		new_filtered_events_df.repartition(*cols).write.mode('append').option("mode", "DROPMALFORMED").partitionBy(cols).parquet('s3a://gdelt-parquet/v2_events')
		print ('..............................Written successfully on S3 parguet-events............................')
		return

	def write_s3_parquet_mentions(self, filtered_mentioned_df):
		# partiton by Year, Month, Day, Hour, Min - Window
		# cond = """case when expr('substring(MentionTimeDate, 11, 2)') == 00 then '1'
        #     else case when expr('substring(MentionTimeDate, 11, 2)') == 15 then '2'
        #         else case when expr('substring(MentionTimeDate, 11, 2)') == 30 then '3'
        #             else case when expr('substring(MentionTimeDate, 11, 2)') == 45 > 0 then '4'
        #                 end
        #             end
        #         end
        #     end as Window"""

		new_filtered_mentioned_df = filtered_mentioned_df.withColumn('Year',  expr('substring(MentionTimeDate, 1, 4)')) \
													.withColumn('Month', expr('substring(MentionTimeDate, 5, 2)'))\
													.withColumn('Day', expr('substring(MentionTimeDate, 7, 2)')) \
													.withColumn('Hour', expr('substring(MentionTimeDate, 9, 2)')) \
																.withColumn('Minute', expr('substring(MentionTimeDate, 11, 2)'))

		# Write to parquet in S3
		cols = ['Year', 'Month', 'Day', 'Hour', 'Minute']
		new_filtered_mentioned_df.repartition(*cols).write.mode('append').option("mode", "DROPMALFORMED").partitionBy(cols).parquet('s3a://gdelt-parquet/mentions_data')
		print ('..............................Written successfully on S3 parguet - mentions..........................')
		return

	def join_df(self, events_df, mentions_df):
		'''
		Joining of events data with mentions data
		'''

		#final_df = events_df.join(mentions_df, on=['GLOBALEVENTID'], how='inner')
		#final_df = events_df.join(mentions_df, 'GLOBALEVENTID')
		final_df = mentions_df.join(broadcast(events_df), 'GLOBALEVENTID')
		#final_df.show(n=20)
		return final_df

	def aggregate_job(self, df_joined):

		df_finalized = df_joined.groupby('state', 'Year', 'MonthYear', 'Actor1Type1Code') \
			.agg(
				functions.approx_count_distinct('GLOBALEVENTID').alias('events_count'),
				functions.sum('GoldsteinScale_norm').alias('GoldsteinScale_norm_sum'),
				functions.sum('Avg_Confidence').alias('confidence_sum')
				)

		return df_finalized


