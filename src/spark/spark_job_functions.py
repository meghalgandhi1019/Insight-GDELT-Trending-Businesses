from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType

class SparkJobFunctions(object):

	def __init__(self):
		'''
		It'll initialise spark context and spark sql context with required configurations.

			'''
		self.__session = SparkSession.builder \
			.master('spark://ec2-52-13-56-58.us-west-2.compute.amazonaws.com:7077') \
			.appName('GDELT-Business-Influencers') \
			.config('spark.executors.memory', '12gb') \
			.config('spark.jars', '/usr/local/spark/jars/postgresql-42.2.13.jar') \
			.config('spark.executor.instances', 11) \
			.getOrCreate()

		self.sqlcontext = SQLContext(self.__session)
		# self.bucket1_gdelt = 's3n://gdelt-open-data/events/[1,2][0-9]*[0-9][0-3|9].csv'
		# self.bucket2_gdelt = 's3n://gdelt-open-data/events/*.export.csv'

		self.bucket1_gdelt = 's3a://gdelt-open-data/events/1992.csv'
		self.bucket2_gdelt = 's3a://gdelt-open-data/events/20190415.export.csv'
		#self.bucket3_gdelt_mentions = 's3a://gdelt-open-data/v2/mentions/20190415[0-9][0-9][0-9][0-9][0-9][0-9].mentions.csv'
		self.bucket3_gdelt_mentions = 's3a://gdelt-open-data/v2/mentions/20190415*'

	def sql_read(self, s3bucket, schema):
		'''
		Read GDELT data and return schema attached df
		'''
		gdelt_text_data = self.__session.sparkContext.textFile(s3bucket)
		gdelt_with_schema = gdelt_text_data.map(lambda c: c.split("\t")).map(lambda c: schema(*c))
		gdelt_df = self.sqlcontext.createDataFrame(gdelt_with_schema)
		return gdelt_df


	def filter_gdelt_events(self, df):
		'''

		Filter GDELT events data. Normalise Goldestein score
		'''

		df.registerTempTable("GdeltEventsTable")
		#roles_of_interest = ["COP", "GOV", "AGR", "BUS", "DEV", "EDU", "ENV", "HLH", "MED", "NGO"]


		# Filter by US location only, Normalize GoldsteinScale and filter Actor1Type1Code #
		events_df = self.sqlcontext.sql("select GLOBALEVENTID, SQLDATE, MonthYear, Year, Actor1Code, Actor1Type1Code, ActionGeo_FullName, "
										"ActionGeo_CountryCode, ActionGeo_ADM1Code, substring(ActionGeo_ADM1Code, 3,2) state,Actor1Geo_CountryCode, "
										"GoldsteinScale, CAST((GoldsteinScale + 10) * 100 / 20 AS DOUBLE) GoldsteinScale_norm "
										"from GdeltEventsTable where  ActionGeo_CountryCode='US' and "
										"Actor1Code != 'null' and "
										"Actor1Type1Code in ('COP', 'GOV', 'AGR', 'BUS', 'DEV', 'EDU', 'ENV', 'HLH', 'MED', 'NGO')" )
		events_df.show(n=20)
		return events_df

	def filter_gdelt_mentions(self, df):
		'''
		Perform Groupby on a mentioned table uisng GlobalEventID.
		'''
		df.registerTempTable("GdeltMentionsTable")

		# Filter to get required columns only #
		mentions_df = self.sqlcontext.sql("select GLOBALEVENTID, avg(Confidence) as Avg_Confidence from GdeltMentionsTable group by GLOBALEVENTID")
		mentions_df.show(n=20)
		return mentions_df

	def join_df(self, events_df, mentions_df):
		'''
		Joining of events data with mentions data
		'''

		final_df = events_df.join(mentions_df, on=['GLOBALEVENTID'], how='inner')
		final_df.show(n=20)
		return

	# def add_state(self, df):
	# 	name = 'ActionGeo_ADM1Code'
	# 	udf = UserDefinedFunction(lambda x: x[:2] + '-' + x[2:], StringType())
	# 	df_news = df_news.select(*[udf(column).alias(name) if column == name
	# 							   else column for column in df_news.columns])
	# 	split_col = F.split(df_news['ActionGeo_ADM1Code'], '-')
	# 	df_news = df_news.withColumn('action_state', split_col.getItem(1))
	#
	# 	return df_news





	# combine data frames - optional
	# filter data frames by us only and for specific type of sector - done
	# parse , add state column -
	# normalize golstein and confidence score

	# aggregated df and prepare it to write in database postgre sql

	# def write_to_db(self, dataframe):
	# 	__db_name = 'gdeltdatabase'
	# 	__db_user = 'postgres'
	# 	__db_pass = 'sai'
	# 	__db_host = 'ec2-52-43-146-226.us-west-2.compute.amazonaws.com'
	# 	__db_port = 5432
	#
	# 	__db_url = "jdbc:postgresql://" + __db_host + ':' + str(__db_port) + '/' + __db_name
	#
	# 	__table_name = 'gdelt'
	#
	# 	__properties = {
	# 		"driver": "org.postgresql.Driver",
	# 		"user": __db_user,
	# 		"password": __db_pass
	# 	}
	# 	__write_mode = 'append'
	# 	dataframe.write.jdbc(url=__db_url,
    #           table=__table_name,
	# 		  mode=__write_mode,
    #           properties=__properties)
	#
	# 	print('Success.....Done')
	#
	# 	return


# gdelt_df = self.sqlcontext.read \
#             .format('com.databricks.spark.csv') \
#             .options(header='false') \
#             .options(delimiter="\t") \
#             .load(s3bucket, schema=schema)