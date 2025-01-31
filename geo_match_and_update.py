import os
import pandas as pd
from sqlalchemy import text
from models.db_connect import DbConnection
from datetime import datetime, timezone, timedelta

scraper_dir = os.path.dirname(os.path.abspath(__file__))
local_results_dir = os.path.join(scraper_dir, "results")
local_get_latlngstatus_data = os.path.join(local_results_dir, "get_latlngstatus_data.csv")

class GeoMatchAndUpdate:
    def __init__(self):
        self.db_connection = DbConnection()
        self.db_connection.get_server_connection()
        self.db_table = 'ampre_vow_condos'
        self.engine = self.db_connection.engine
        self.mls_num_list = []  # Initialize mls_num_list

    def get_data(self):
        query = text(f"""
            SELECT Addr, Walkscore, Walkscore_description, Snapped_lat, Snapped_lon,
                    Transit_score, Transit_description, Transit_summary, Bike_score, 
                    Bike_description, Ml_num, Lat, Lng, LatLng_status 
            FROM {self.db_table} 
            WHERE LatLng_status = '1'
        """)
        return pd.read_sql(query, self.engine)

    def filtering_db_data(self, data):
        """Filter the data based on the condition."""
        data['priority'] = data[['Lat', 'Lng', 'Walkscore']].isnull().any(axis=1)
        data.sort_values(by=['Addr', 'priority'], ascending=[True, True], inplace=True)
        data.drop_duplicates(subset=['Addr'], keep='first', inplace=True)
        data.drop(columns=['priority'], inplace=True)

        os.makedirs(local_results_dir, exist_ok=True)
        data.to_csv(local_get_latlngstatus_data, index=False)
        return data  # Return the filtered data for further processing

    def update_database(self, filtered_data):
        """Update the database with the filtered data based on Addr."""
        session = self.db_connection.Session()
        with session.begin():  # Use session to manage transactions
            for _, row in filtered_data.iterrows():
                values = {col: row[col] if pd.notna(row[col]) else None for col in [
                    'Addr', 'Walkscore', 'Walkscore_description', 'Snapped_lat', 
                    'Snapped_lon', 'Transit_score', 'Transit_description', 
                    'Transit_summary', 'Bike_score', 'Bike_description', 'Lat', 'Lng'
                ]}
                values['LatLng_status'] = '1'  # Set this to '1' after update

                # Update if Addr exists
                update_query = text(f"""
                    UPDATE {self.db_table} 
                    SET Walkscore=:Walkscore, Walkscore_description=:Walkscore_description,
                        Snapped_lat=:Snapped_lat, Snapped_lon=:Snapped_lon, 
                        Transit_score=:Transit_score, Transit_description=:Transit_description, 
                        Transit_summary=:Transit_summary, Bike_score=:Bike_score, 
                        Bike_description=:Bike_description, Lat=:Lat, Lng=:Lng, 
                        LatLng_status=:LatLng_status 
                    WHERE Addr=:Addr
                """)
                session.execute(update_query, values)
            session.commit()

    def update_elasticsearch(self):
        """Update Elasticsearch with the filtered data."""
        if self.mls_num_list:
            mls_num_list_batch_size = 150
            while self.mls_num_list:
                batch = self.mls_num_list[:mls_num_list_batch_size]
                self.condosElastic.index_selected_property_in_bulk(batch)
                self.mls_num_list = self.mls_num_list[mls_num_list_batch_size:]

    def main(self):
        # getting data from database with both lat and lng
        data = self.get_data()
        # the filtered data from csv -- does not contain duplicate address
        filtered_data = self.filtering_db_data(data)
        print(len(filtered_data))
        if not filtered_data.empty:
            # day_ago = (datetime.now(timezone.utc) - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
            # print(day_ago)
            # date to update 2 days data
            day_ago = "2025-01-21 23:59:05"
            # recent_data_query = text(f"SELECT Addr, Ml_num FROM {self.db_table} WHERE mage_status = 1 AND LatLng_status = 0 AND Lat IS NULL AND Lng IS NULL AND MlsStatus = 'New' AND (Status_aur != 'U' OR Status_aur is NULL ) AND (Property_type != 'Commercial' OR Property_type is NULL ) AND ModificationTimestamp > :day_ago")
            recent_data_query = text(f"SELECT Addr, Ml_num FROM {self.db_table} WHERE  Walkscore IS NULL AND Lat IS NULL AND Lng IS NULL AND ModificationTimestamp > :day_ago")
            recent_data = pd.read_sql(recent_data_query, self.engine, params={'day_ago': day_ago})
            print(f"recent data -- {len(recent_data['Ml_num'].tolist())}")
            if not recent_data.empty:
                print("recent-data", recent_data)
                # getting the data with unique address to update according to filtered data from database
                addresses_to_update = recent_data['Addr'].unique()
                print(f"address_to_update -- {len(addresses_to_update)}")
                # filtering the data to update that is innfiltered data which are unique in address 
                filtered_data_to_update = filtered_data[filtered_data['Addr'].isin(addresses_to_update)]
                filtered_data_to_update.to_csv("filtered_data_to_update.csv")
                print(f"filtered_data_to_update --{len(filtered_data_to_update['Ml_num'].tolist())}")
            
                
                # if not filtered_data_to_update.empty:
                #     self.update_database(filtered_data_to_update)
                #     self.update_elasticsearch()  # Call to update Elasticsearch
            else:
                print("recent data is empty")
        else:
            print("Failed to save filtered data to CSV")

def main():
    geo_match_and_update = GeoMatchAndUpdate()
    geo_match_and_update.main()

if __name__ == "__main__":
    main()