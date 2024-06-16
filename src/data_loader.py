class DataLoader:
    """
    A class to handle loading data using Spark.

    Attributes:
    ----------
    spark : SparkSession
        The SparkSession instance used for data loading.
    config : dict
        The configuration dictionary containing paths and formats of input files.

    Methods:
    -------
    load_data(key):
        Loads data based on the provided key from the configuration.
    """
    def __init__(self, spark, config):
        #create spark session
        self.spark = spark
        self.config = config


    def load_data(self, key):
        """
        Loads data based on the provided key from the configuration.

        Parameters:
        ----------
        key : str
            The key in the configuration dictionary to access the file information.

        Returns:
        -------
        DataFrame
            A Spark DataFrame loaded from the specified file.
        """

        file_info = self.config['INPUT_FILES'][key]
        #get file path
        file_path = file_info['Path']
        #get file format
        file_format = file_info['Format']

        #read the file with inferSchema and header options as true
        df = self.spark.read.format(file_format)\
                            .option('inferSchema','true')\
                            .option("header", "true")\
                            .load(file_path)
        
        return df
