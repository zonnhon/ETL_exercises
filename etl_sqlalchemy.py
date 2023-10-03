import os
import requests
import pandas as pd
from sqlalchemy import create_engine

import logging

logging.basicConfig(filename='etl_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s -%(message)s')
logger = logging.getLogger(__name__)

CWA_KEY = os.environ['CWA_KEY']


def etl_process_with_sqlalchemy(api_url, api_params, db_path, table_name):
    try:
        # Extract
        response = requests.get(api_url, params=api_params)
        # Check if the API request was successful
        response.raise_for_status()

        input_data = response.json()
        records = input_data['records']
        locations = records['location']

        # Transform
        all_data = []
        for location in locations:
            location_name = location['locationName']
            for element in location['weatherElement']:
                element_name = element['elementName']
                for time_period in element['time']:
                    start_time = time_period['startTime']
                    end_time = time_period['endTime']
                    parameter = time_period['parameter']
                    parameter_name = parameter.get('parameterName', None)
                    parameter_value = parameter.get('parameterValue', None)
                    parameter_unit = parameter.get('parameterUnit', None)
                    row_data = [location_name, element_name, start_time, end_time, parameter_name, parameter_value,
                                parameter_unit]
                    all_data.append(row_data)

        column_name = ['locationName', 'elementName', 'startTime', 'endTime', 'parameterName', 'parameterValue',
                       'parameterUnit']
        df = pd.DataFrame(all_data, columns=column_name)
        pivot_df = df.pivot_table(index=['locationName', 'startTime', 'endTime'],
                                  columns='elementName',
                                  values='parameterName',
                                  aggfunc='first').reset_index()

        #Load
        engine = create_engine(f'sqlite:///{db_path}')
        try:
            existing_data = pd.read_sql_table(table_name, engine)
        except ValueError:
            existing_data = pd.DataFrame()

        merged_df = pivot_df.merge(existing_data, how='left', indicator=True)
        new_rows = merged_df[merged_df['_merge'] == 'left_only']
        new_rows = new_rows.drop(columns=['_merge'])
        new_rows = new_rows.drop_duplicates()

        new_rows.to_sql(table_name, engine, if_exists='append', index=False)


    except requests.RequestException as e:
        print(f'API request error: {e}')
        logger.error(f'API request error: {e}')
    except ValueError as e:
        print(f'JSON parsing error: {e}')
        logger.error(f'JSON parsing error: {e}')
    except Exception as e:
        print(f'Unexpected error: {e}')
        logger.error(f'Unexpected error: {e}')
    finally:
        try:
            engine.dispose()
        except NameError:
            pass

if __name__ == "__main__":
    api_url = 'https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-C0032-001'
    api_params = {
        'Authorization': CWA_KEY,
    }
    db_path = 'db_opendata.sqlite'
    table_name = 'cwa_weather_forecast'

    etl_process_with_sqlalchemy(api_url, api_params, db_path, table_name)
