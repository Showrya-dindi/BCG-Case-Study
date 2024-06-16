import yaml

class LoadConfig:
    """
    A class to load a configuration file in YAML format.

    Methods:
    -------
    load(config_path):
        Loads the configuration from the specified YAML file path.

    """

    def load(config_path):
        """
        Loads the configuration from the specified YAML file path.

        Parameters:
        ----------
        config_path : str
            The path to the YAML configuration file.

        Returns:
        -------
        dict
            The configuration dictionary loaded from the YAML file.
        """
        #read the config yaml file 
        with open(config_path, 'r') as file:
            #load and store the data in a python dict 
            config = yaml.safe_load(file)
        return config



class SaveResult:
    """
    A class to save a DataFrame to a specified output path in a given format.

    Attributes:
    ----------
    output_path : str
        The directory path where the output file will be saved.

    Methods:
    -------
    write_output(df, write_format):
        Saves the DataFrame to the specified output path in the given format.
    
    """
    def __init__(self,output_path):
        self.output_path = output_path


    def write_output(self, df, write_format):
        """
        Writes the DataFrame to the specified output path in the given format.

        Parameters:
        ----------
        df : DataFrame
            The DataFrame to be saved.
        write_format : str
            The format in which the DataFrame should be saved (e.g., 'csv', 'parquet').

        Returns:
        -------
        True
        """

        #repartition the dataframe into a single partition (to write the file in 1 single file) 
        #and write the data to the output path
        # df = df.coalesce(1)
        df.repartition(1).write.format(write_format)\
            .mode("overwrite")\
            .option("header", "true")\
            .save(self.output_path+r'/')

        return True



