from models.db_connect import DbConnection
import pandas as pd
from sqlalchemy import text
import numpy as np

# Get the database connection
db_connection = DbConnection().get_server_connection()


def insert_into_database(db_table, session):
    # Read CSV file with low_memory=False to handle mixed data types
    csv_file = pd.read_csv("ampre_vow_condos_202501281819.csv", low_memory=False)

    # Convert all NaN/NA values to None (MySQL-friendly)
    csv_file = csv_file.replace({pd.NA: None, np.nan: None})

    columns = ",".join(csv_file.columns)
    parameters = ",".join([f":{col}" for col in csv_file.columns])  # Named placeholders

    sql_query = text(f"INSERT INTO {db_table} ({columns}) VALUES ({parameters})")  # Use named parameters

    try:
        with session.begin():
            data_list = csv_file.to_dict(orient="records")  # Convert rows to list of dictionaries
            session.execute(sql_query, data_list)  # Bulk insert

    except Exception as e:
        print(f"Error occurred while inserting into database: {e}")
        return False

    return True


def main():
    db_table = "ampre_vow_condos"
    session = db_connection.Session()  # Create a session

    success = insert_into_database(db_table, session)

    if not success:
        print("Failed to insert into database")
        return

    print("Data inserted successfully")


if __name__ == "__main__":
    main()
