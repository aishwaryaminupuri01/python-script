from datetime import datetime

import requests
import mysql.connector

import config

connection = mysql.connector.connect(
        host=config.DATASOURCE_HOST,
        user=config.DATASOURCE_USERNAME,
        password=config.DATASOURCE_PASSWORD,
        database=config.DATASOURCE_DATABASE)

def truncateTable():
    cursor = connection.cursor()
    truncateStatement = f"TRUNCATE covid_stats"
    cursor.execute(truncateStatement)
    connection.commit()
    print(f"Data truncated successfully")
def fetchData(state):
    try:
        response = requests.get(config.COVID_TRACKING_API_URL.format(state=state.lower()))
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Ïssue in fetching data from Covid API")
    except Exception as e:
        print(e)
        print(f"Unable to fetch data from Covid API for the state : {state}")

def insertData(state):
    try:
        cursor = connection.cursor()

        data = fetchData(state)
        column_mapping = {'date': 'dt'}
        columns = ', '.join(column_mapping.get(key, key) for key in data.keys())
        dateModified = None if data['dateModified'] is None else datetime.strptime(data['dateModified'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        dateChecked = None if data['dateChecked'] is None else datetime.strptime(data['dateChecked'],'%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

        data['dateModified'] = dateModified
        data['dateChecked'] = dateChecked

        values = ', '.join([f"'{value}'" if isinstance(value,str) else str(value) if value is not None else 'NULL' for value in data.values()])
        query = f"INSERT INTO covid_stats({columns}) VALUES ({values})"
        print(f"Inserting data for {state}")
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        print(e)
        print(f"Unable insert data for {state}.")


def closeConnection():
    connection.close()

if __name__ == '__main__':
    try:
        truncateTable()
        for state in config.COVID_TRACKING_STATES:
            insertData(state)
    finally:
        closeConnection()