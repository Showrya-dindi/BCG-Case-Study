import sys
from src.utils import LoadConfig 
from src.data_loader import DataLoader
from src.analysis import Analysis
from pyspark.sql import SparkSession

def run_analysis(config_path):
    """
    Load configuration, initialize data loader and analysis, and run the analysis.
    
    Parameters:
    config_path (str): Path to the configuration file.
    
    """
    #Load config information from yaml file 
    config = LoadConfig.load(config_path)
    
    #Intialize DataLoader object by passing config dict (for input paths)
    data_loader = DataLoader(spark,config)
    
    #Intialize Analysis object by passing data_loader object (to read dataframes) and config (for output paths)
    analysis = Analysis(data_loader, config)
    
    #Call run_all method to run analysis1 to analysis10
    analysis.run_all()



if __name__ == "__main__":
    """
    Main entry point for the script. Expects two arguments: the path to pyspark script
    and the path to the configuration file.
    """
    #Check for 2 system args: pyspark script and config path
    if len(sys.argv) != 2:
        print("Usage: spark-submit scripts/run_analysis.py <config_path>")
        sys.exit(1)

    #Get the config path 
    config_path = sys.argv[1]

    #start the spark session
    spark = SparkSession.builder.master("local[1]").appName("AccidentAnalysis").getOrCreate()
    
    #call the run_analysis function
    run_analysis(config_path)
    
    #stop the spark session
    spark.stop()
