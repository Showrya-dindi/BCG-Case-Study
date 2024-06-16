# Accident Analysis

## Project Structure

- `configs/`: Configuration files
- `data/`: Input data files
- `output/`: Output files for each analysis
- `scripts/`: Executable scripts
- `src/`: Source code
- `requirements.txt`: Dependencies
- `setup.py` : To setup custom package

## Running the Analysis

To run the application follow the below steps:

0. Install OpenJDK and add a system variable `JAVA_HOME` with the path to your OpenJDK installation

1. Go to the Project Directory: 
`cd BCG-CASE-STUDY`

2. Create a Virtual Environment:
`python3 -m venv venv`

3. Activate the Virtual Environment:
* On Windows:
`venv\Scripts\activate`

* On macOS and Linux:
`source venv/bin/activate`

4. Install the Dependencies:
`pip install -r requirements.txt`

5. Install the Custom Package (src)
`python3 -m pip install -e .`

6. Run the analysis:
`spark-submit scripts/run_analysis.py configs/config.yaml`

7. Deactivate the Virtual Environment
`deactivate`

## Note

1. The final output would be in the following file output/Analysis_x/part-00000-xxx000.csv
2. Transformation logic for each analysis is in src/analysis.py 
3. Use python/python3 and pip/pip3 according to the python version installed 
