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

Tp run the application follow the below steps

0. Go to the Project Directory: 
`cd BCG-CASE-STUDY`

1. Create a Virtual Environment:
`python3 -m venv venv`

2. Activate the Virtual Environment:
On Windows:
`venv\Scripts\activate`

On macOS and Linux:
`source venv/bin/activate`

3. Install the Dependencies:
`pip install -r requirements.txt`

4. Install the Custom Package (src)
`python3 -m pip install -e .`

4. Run the analysis:
`spark-submit scripts/run_analysis.py configs/config.yaml`

5. Deactivate the Virtual Environment
`deactivate`

## Note

1. The final output would be in the following file output/Analysis_x/part-00000-xxx000.csv
2. Transformation logic for each analysis is in src/analysis.py 
3. Use python/python3 and pip/pip3 according to the python version installed 
