from pyspark.sql import functions as F
from pyspark.sql import Window
from src.utils import SaveResult
from datetime import datetime

class Analysis:
    def __init__(self, data_loader, config):
        self.data_loader = data_loader
        self.config = config

    def analysis_1(self):
        """
        Finds the number of crashes in which the number of males killed are greater than 2.
        """
        #load data using data_loader
        df_primary_person = self.data_loader.load_data('Primary_Person')

        #filter the DataFrame to include only rows where the person gender is "MALE" and 
        #where the injury severity is "KILLED"
        #group the filtered DataFrame by CRASH_ID
        #filter the aggregated data to include only crashes where more than 2 males were killed
        df_analysis_1 = df_primary_person.filter(F.col('PRSN_GNDR_ID') == "MALE")\
                                            .filter(F.col('PRSN_INJRY_SEV_ID') == "KILLED")\
                                            .groupBy('CRASH_ID')\
                                            .agg(F.count('CRASH_ID').alias('Number_of_Males_Killed'))\
                                            .filter(F.col('Number_of_Males_Killed') > 2)
        
        
        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_1']
        SaveResult(output_path).write_output(df_analysis_1,'csv')

        return df_analysis_1

    def analysis_2(self):
        """
        Finds the number of two wheelers that are booked for crashes
        """
        #load data using data_loader and drop duplicate values in df_units
        df_units = self.data_loader.load_data('Units').drop_duplicates()

        #filter the DataFrame to include only rows where the vehicle body style is "MOTORCYCLE" or "POLICE MOTORCYCLE"
        #aggregate the filtered data to count the number of crashed two-wheelers using 'VEH_BODY_STYL_ID'
        df_analysis_2 = df_units.filter((F.col('VEH_BODY_STYL_ID') == "MOTORCYCLE") |
                                        (F.col('VEH_BODY_STYL_ID') == "POLICE MOTORCYCLE"))\
                                .agg(F.count('VEH_BODY_STYL_ID').alias('Number_of_Crashed_TwoWheelers'))

        
        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_2']
        SaveResult(output_path).write_output(df_analysis_2,'csv')

        return df_analysis_2
    
    def analysis_3(self):
        """
        Determines the Top 5 Vehicle Makes of the cars present in the crashes 
        in which driver died and Airbags did not deploy
        """
        #load data using data_loader and drop duplicate values in df_units
        df_primary_person = self.data_loader.load_data('Primary_Person')
        df_units = self.data_loader.load_data('Units').drop_duplicates()

        #join units and primary_person and filter to include only rows where the person type is "DRIVER" and  injury severity is "KILLED"
        #filter to include rows where the airbag was not deployed also filter out the rows where vehicle make is "NA"
        #group the filtered DataFrame by vehicle make ID
        #order the results by the count of undeployed airbags in descending order 
        #and limit 5 records to get the top 5 vehile make
        df_analysis_3 = df_units.join(df_primary_person, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                                .filter(F.col('PRSN_TYPE_ID') == "DRIVER")\
                                .filter(F.col('PRSN_INJRY_SEV_ID') == "KILLED")\
                                .filter(F.col('PRSN_AIRBAG_ID') == "NOT DEPLOYED")\
                                .filter(F.col('VEH_MAKE_ID') != "NA")\
                                .groupby('VEH_MAKE_ID')\
                                .agg(F.count('CRASH_ID').alias('Number_of_Undeployed_Airbag'))\
                                .orderBy(F.col('Number_of_Undeployed_Airbag').desc())\
                                .limit(5)
        
        
        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_3']
        SaveResult(output_path).write_output(df_analysis_3,'csv')

        return df_analysis_3
        
    def analysis_4(self):
        """
        Determines the number of Vehicles with driver having valid licences involved in hit and run
        """
        #load data using data_loader and drop duplicate values in df_units
        df_primary_person = self.data_loader.load_data('Primary_Person')
        df_units = self.data_loader.load_data('Units').drop_duplicates()

        #join units and primary_person        
        #filter the DataFrame to include only rows where the vehicle hit-and-run flag is "Y"
        #filter the DataFrame to include only rows where the driver license type is valid i.e. either "DRIVER LICENSE" or "COMMERCIAL DRIVER LIC."
        df_analysis_4 = df_units.join(df_primary_person, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                                .filter(F.col('VEH_HNR_FL') == "Y")\
                                .filter(F.col('DRVR_LIC_TYPE_ID').isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))\
                                .select(F.count('CRASH_ID').alias('Number_of_ValidLicense_HitAndRun'))
        

        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_4']
        SaveResult(output_path).write_output(df_analysis_4,'csv')

        return df_analysis_4


    def analysis_5(self):
        """
        Determines the state which has the highest number of accidents in which females are not involved
        """
        #load data using data_loader
        df_primary_person = self.data_loader.load_data('Primary_Person')

        #filter the df_primary_person DataFrame to include only rows where the person gender is not "FEMALE" 
        #filter out the records where state is "NA","Unknown","Other"
        #group the filtered DataFrame by 'DRVR_LIC_STATE_ID'
        #order the results by the count of accidents with no females involved in descending order 
        #and limit 1 record to get the state which has the highest number of accidents in which females are not involved
        df_analysis_5 = df_primary_person.filter(F.col('PRSN_GNDR_ID') != "FEMALE")\
                                            .filter(~F.col('DRVR_LIC_STATE_ID').isin(["NA","Unknown","Other"]))\
                                            .groupby('DRVR_LIC_STATE_ID')\
                                            .agg(F.count('*').alias('Number_of_Accidents_No_Females'))\
                                            .orderBy(F.col('Number_of_Accidents_No_Females').desc())
        
        
        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_5']
        SaveResult(output_path).write_output(df_analysis_5,'csv')

        return df_analysis_5

    def analysis_6(self):
        """
        Determines the Top 3rd to 5th VEH_MAKE_IDs that contribute to largest number of injuries including death
        """
        #load data using data_loader and drop duplicate values in df_units
        df_units = self.data_loader.load_data('Units').drop_duplicates()

        #calculate 'Injuries_Deaths' column as the sum of 'TOT_INJRY_CNT' and 'DEATH_CNT'
        #filter out the records where vehicle make is "NA"
        #group the DataFrame by 'VEH_MAKE_ID'
        df_analysis_6 =  df_units.withColumn('Injuries_Deaths', F.col('TOT_INJRY_CNT') + F.col('DEATH_CNT'))\
                                    .filter(F.col('VEH_MAKE_ID') != "NA")\
                                    .groupby('VEH_MAKE_ID')\
                                    .agg(F.sum('Injuries_Deaths').alias('Number_of_Injuries_Deaths'))
        
        #define a window specification to order by 'Count_Injuries_Deaths' 
        window_spec = Window.orderBy(F.col('Number_of_Injuries_Deaths').desc())

        #apply row number over the window specification and filter the records where row_number is in [3,5]
        #later drop the row_number column
        df_analysis_6_final = df_analysis_6.withColumn("row_number", F.row_number().over(window_spec))\
                                            .filter((F.col('row_number') >= 3) & 
                                                    (F.col('row_number') <= 5))\
                                            .drop('row_number')
        

        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_6']
        SaveResult(output_path).write_output(df_analysis_6_final,'csv')

        return df_analysis_6_final

    def analysis_7(self):
        """
        Determines top ethnic user group of each unique body style, for all the body styles involved in crashes
        """
        #load data using data_loader and drop duplicate values in df_units
        df_units = self.data_loader.load_data('Units').drop_duplicates()
        df_primary_person = self.data_loader.load_data('Primary_Person')

        #join df_units and df_primary_person 
        #filter out the joined DataFrame to exclude rows where 'VEH_BODY_STYL_ID' and 'PRSN_ETHNICITY_ID' have misc. values
        #group the filtered DataFrame by 'VEH_BODY_STYL_ID' and 'PRSN_ETHNICITY_ID' to get the count of occurences
        df_analysis_7 = df_units.join(df_primary_person, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                                .filter(~F.col('VEH_BODY_STYL_ID').isin(
                                            ["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]))\
                                .filter(~F.col('PRSN_ETHNICITY_ID').isin(["NA", "UNKNOWN"]))\
                                .groupby('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')\
                                .agg(F.count('*').alias('Count_ethnic_user_group'))
        
        #define a window specification partitioned by 'VEH_BODY_STYL_ID'
        window_spec = Window.partitionBy('VEH_BODY_STYL_ID')

        #apply window function to find the maximum count of ethnic user groups within each vehicle body style
        df_analysis_7_final = df_analysis_7.withColumn("Max_count_eug", F.max('Count_ethnic_user_group').over(window_spec))\
                                            .filter(F.col('Count_ethnic_user_group') == F.col('Max_count_eug'))\
                                            .drop('max_count_eug','Count_ethnic_user_group')
        

        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_7']
        SaveResult(output_path).write_output(df_analysis_7_final,'csv')

        return df_analysis_7_final

    def analysis_8(self):
        """
        Determines the Top 5 Zip Codes with highest number car crashes 
        with alcohol as the contributing factor to a crash
        """
        #load data using data_loader and drop duplicate values in df_units
        df_units = self.data_loader.load_data('Units').drop_duplicates()
        df_primary_person = self.data_loader.load_data('Primary_Person')

        #join df_units and df_primary_person
        #exclude rows where 'DRVR_ZIP' is null
        #filter the DataFrame to include only rows where 'CONTRIB_FACTR_1_ID' or 'CONTRIB_FACTR_2_ID' contains "ALCOHOL"
        #group the DataFrame by 'DRVR_ZIP'
        #order the results by the count of alcohol-related crashes in descending order 
        #and limit the results to the top 5 ZIP codes
        df_analysis_8 = df_units.join(df_primary_person, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                                    .filter(F.col('DRVR_ZIP').isNotNull())\
                                    .filter(F.col('CONTRIB_FACTR_1_ID').contains("ALCOHOL")\
                                            | F.col('CONTRIB_FACTR_2_ID').contains("ALCOHOL"))\
                                    .groupby("DRVR_ZIP")\
                                    .agg(F.count('*').alias('Number_of_Alcohol_Crashes'))\
                                    .orderBy(F.col("Number_of_Alcohol_Crashes").desc())\
                                    .limit(5)
        

        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_8']
        SaveResult(output_path).write_output(df_analysis_8,'csv')

        return df_analysis_8

    def analysis_9(self):
        """
        Determines the count of distinct Crash IDs where No Damaged Property was observed 
        and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        """
        #load data using data_loader and drop duplicate values in df_units and df_damages
        df_damages = self.data_loader.load_data('Damages').drop_duplicates()
        df_units = self.data_loader.load_data('Units').drop_duplicates()

        #filter the joined DataFrame to include only rows where vehicle damage scales indicate damage level > 4
        #filter the DataFrame to include only rows where there is no damaged property
        #further filter the DataFrame to include only rows where vehicle is a car
        #filter the DataFrame to include only rows where financial responsibility type is "PROOF OF LIABILITY INSURANCE"
        #count distinct crash_id
        df_analysis_9 = df_damages.join(df_units, on=['CRASH_ID'], how="inner")\
                            .filter((F.col('VEH_DMAG_SCL_1_ID').isin(["DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST"]))
                                    | (F.col('VEH_DMAG_SCL_2_ID').isin(["DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST"])))\
                            .filter(F.col('DAMAGED_PROPERTY') == "NONE")\
                            .filter((F.col('VEH_BODY_STYL_ID').contains("CAR")) 
                                    |(F.col('VEH_BODY_STYL_ID') == "SPORT UTILITY VEHICLE") )\
                            .filter(F.col('FIN_RESP_TYPE_ID') == "PROOF OF LIABILITY INSURANCE")\
                            .select(F.countDistinct('CRASH_ID').alias('Count_HighDamage_NoPropDamage_Insured'))
        

        #get the output_path from config file and write the result
        output_path = self.config['OUTPUT_PATHS']['Analysis_9']
        SaveResult(output_path).write_output(df_analysis_9,'csv')

        return df_analysis_9

    def analysis_10(self):
        """
        Determines the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers,
        used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
        """
        #load data using data_loader and drop duplicate values in df_units
        df_charges = self.data_loader.load_data('Charges')
        df_units = self.data_loader.load_data('Units').drop_duplicates()
        df_primary_person = self.data_loader.load_data('Primary_Person')

        #get top 25 states with highest number of offences
        #remove integers in VEH_LIC_STATE_ID
        #group by VEH_LIC_STATE_ID and order by the count of occurences in descending order
        #limit the records to 25
        df_top_25_states = df_units.filter(F.col('VEH_LIC_STATE_ID').cast("int").isNull())\
                                    .groupby('VEH_LIC_STATE_ID')\
                                    .count()\
                                    .orderBy(F.col('count').desc())\
                                    .select('VEH_LIC_STATE_ID')\
                                    .limit(25)
        
        #get top 10 states with highest number of offences
        #remove "NA" in VEH_COLOR_ID
        #group by VEH_COLOR_ID and order by the count of occurences in descending order
        #limit the records to 10
        df_top_10_vehicle_colors = df_units.filter(F.col('VEH_COLOR_ID') != "NA")\
                                            .groupby('VEH_COLOR_ID')\
                                            .count()\
                                            .orderBy(F.col('count').desc())\
                                            .select('VEH_COLOR_ID')\
                                            .limit(10)
        
        #join df_charges, df_primary_person, df_units, df_top_25_states, df_top_10_vehicle_colors
        #filter the DataFrame to include only rows where the charge is speeding related
        #filter the DataFrame to include only rows where there is a valid license
        #group by VEH_MAKE_ID and order the count of ocurences in descending order
        #limit the records to 5 to get the top 5
        df_analysis_10 = df_charges.join(df_primary_person, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                                    .join(df_units, on=['CRASH_ID','UNIT_NBR'], how='inner')\
                                    .join(df_top_25_states,on=['VEH_LIC_STATE_ID'],how='inner')\
                                    .join(df_top_10_vehicle_colors,on=['VEH_COLOR_ID'],how='inner')\
                                    .filter(F.col('CHARGE').contains("SPEED"))\
                                    .filter(F.col('DRVR_LIC_TYPE_ID').isin(
                                        ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]))\
                                    .groupby('VEH_MAKE_ID')\
                                    .agg(F.count('VEH_MAKE_ID').alias('Number_of_Speeding_Offences'))\
                                    .orderBy(F.col("Number_of_Speeding_Offences").desc())\
                                    .limit(5)
        
        output_path = self.config['OUTPUT_PATHS']['Analysis_10']
        SaveResult(output_path).write_output(df_analysis_10,'csv')   

        return df_analysis_10


    def run_all(self):
        try:
            result_df1 = self.analysis_1()
            result_df1.show()
            print(f'\n{datetime.now()} - [*** Analysis 1 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 1 has Failed ***]\n')
            print(f'Error: {e}')

        try:
            result_df2 = self.analysis_2()
            result_df2.show()
            print(f'\n{datetime.now()} - [*** Analysis 2 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 2 has Failed ***]\n')
            print(f'Error: {e}')

        try:
            result_df3 = self.analysis_3()
            result_df3.show()
            print(f'\n{datetime.now()} - [*** Analysis 3 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 3 has Failed ***]\n')
            print(f'Error: {e}')
        try:
            result_df4 = self.analysis_4()
            result_df4.show()
            print(f'\n{datetime.now()} - [*** Analysis 4 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 4 has Failed ***]\n')
            print(f'Error: {e}')

        try:
            result_df5 = self.analysis_5()
            result_df5.show()
            print(f'\n{datetime.now()} - [*** Analysis 5 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 5 has Failed ***]\n')
            print(f'Error: {e}')


        try:
            result_df6 = self.analysis_6()
            result_df6.show()
            print(f'\n{datetime.now()} - [*** Analysis 6 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 6 has Failed ***]\n')
            print(f'Error: {e}')

        try:
            result_df7 = self.analysis_7()
            result_df7.show()
            print(f'\n{datetime.now()} - [*** Analysis 7 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 7 has Failed ***]\n')
            print(f'Error: {e}')
            
        try:
            result_df8 = self.analysis_8()
            result_df8.show()
            print(f'\n{datetime.now()} - [*** Analysis 8 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 8 has Failed ***]\n')
            print(f'Error: {e}')

        try:
            result_df9 = self.analysis_9()
            result_df9.show()
            print(f'\n{datetime.now()} - [*** Analysis 9 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 9 has Failed ***]\n')
            print(f'Error: {e}')

        try:
            result_df10 = self.analysis_10()
            result_df10.show()
            print(f'\n{datetime.now()} - [*** Analysis 10 Has Run Successfully ***]\n')
            
        except Exception as e:
            print(f'\n{datetime.now()} - [*** Analysis 10 has Failed ***]\n')
            print(f'Error: {e}')
        