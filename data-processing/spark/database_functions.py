import configparser


class DatabaseFunctions(object):

    def __init__(self):
        '''
        It'll read database connection details during class initialization only once.
        '''

        # Read the property file.
        config = configparser.RawConfigParser()
        config.read('/usr/local/Insight-GDELT-Trending-Businesses/data-processing/spark/config.properties')

        # Read required database details from property file
        self.__db_name = config.get('PostgreSqlConnection', 'dbauth.dbname')
        self.__db_user = config.get('PostgreSqlConnection', 'dbauth.user')
        self.__db_pass = config.get('PostgreSqlConnection', 'dbauth.password')
        self.__db_host = config.get('PostgreSqlConnection', 'dbauth.host')
        self.__db_port = config.get('PostgreSqlConnection', 'dbauth.port')

        self.__table_name = 'gdeltdetails'
        self.__write_mode = 'append'

        self.__db_url = "jdbc:postgresql://" + self.__db_host + ':' + str(self.__db_port) + '/' + self.__db_name

        self.__properties = {
            "driver": "org.postgresql.Driver",
            "user": self.__db_user,
            "password": self.__db_pass
        }

    def write_to_db(self, dataframe):
        '''
        This function takes in an input dataframe and appends it to the PostgreSQL table
        :param dataframe: Input data frame whose content needs to be written
        :return: Nothing returned. content will be written successfully to PostgreSQL
        '''

        dataframe.write.jdbc(url=self.__db_url,
                              table=self.__table_name,
                              mode=self.__write_mode,
                              properties=self.__properties)

        print('Success ! Completed writing...')