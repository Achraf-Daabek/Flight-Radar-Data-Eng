from FlightRadar24.api import FlightRadar24API
from pyspark.sql import functions as F
from pyspark.sql import Row
import logging
from databricks import DatabricksUtils


# Configuration de base pour le logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


#Instance API
fr_api = FlightRadar24API() 


#  Extraction - Verification des schema - Nettoyage des données 
class DataExtractor(DatabricksUtils):
    def __init__(self, fr_api: FlightRadar24API):
        self.fr_api = fr_api
        
    def extract_data(self):
        """A function to extract data from the API""" 
        flights = self.fr_api.get_flights() 
        airlines = self.fr_api.get_airlines()
        zones = self.fr_api.get_zones()
        return {
            "flights": flights,
            "airlines": airlines,
            "zones": zones
        }   
    def extract_data_from_dic(self):
        try:
            logging.info("Starting data extraction from API...")
            airports = self.fr_api.get_airports()
            zones = self.fr_api.get_zones()
            logging.info("Data extraction completed successfully.")

            return {
                "airports": airports,
                "zones": zones
            }
        except Exception as e:
            logging.error(f"Data extraction failed: {str(e)}")
            return None
        

    def convert_airports_to_df(self, api_data):
        airport_data = []

        for airport in api_data:
            country = str(airport.country)
            code_iata = str(airport.iata)
            code_icao = str(airport.icao)
            name = str(airport.name)
            altitude = int(airport.altitude)
            latitude = float(airport.latitude)
            longitude = float(airport.longitude)

            airport_data.append(Row(country, code_iata, code_icao, name, altitude, latitude, longitude))

        df = self.spark.createDataFrame(airport_data, ["Country", "IATA Code", "ICAO Code", "Name", "Altitude", "Latitude", "Longitude"])

        return df

    def convert_zones_to_df(self, zones):
        zone_data = []

        def process_zone(name, zone_info):
            tl_y = float(zone_info['tl_y'])
            tl_x = float(zone_info['tl_x'])
            br_y = float(zone_info['br_y'])
            br_x = float(zone_info['br_x'])
            zone_data.append(Row(name, tl_y, tl_x, br_y, br_x))

            if 'subzones' in zone_info:
                for subname, subzone_info in zone_info['subzones'].items():
                    process_zone(subname, subzone_info)

        for zone_name, zone_info in zones.items():
            process_zone(zone_name, zone_info)

        df = spark.createDataFrame(zone_data, ["Zone Name", "TL_Y", "TL_X", "BR_Y", "BR_X"])

        return df
    
    def clean(self, extracted_data):    
        """A function to celan the extracted data
        The result is a dictionary of dataframes """ 
        #Transformer les données extraites et les mettres dans dataframes Spark   
        flights_df = self.spark.createDataFrame(extracted_data["flights"])
        airlines_df = self.spark.createDataFrame(extracted_data["airlines"])
        #Ajouter un nouveau dataframe qui va correspondre entre les compagnies et les vols actifs
        flights_iata = flights_df.filter(flights_df['on_ground'] == 0).select('airline_iata', 'id') 
        flights_icao = flights_df.filter(flights_df['on_ground'] == 0).select('airline_icao', 'id')
        airlines_tmp = airlines_df.toDF(*['airline_iata', 'airline_icao', 'airline_name']) # rename airlines colums
        airlines_tmp_iata = airlines_tmp.join(flights_iata, 'airline_iata', how='left') # flights by iata 
        airlines_tmp_icao = airlines_tmp.join(flights_icao, 'airline_icao', how='left') # flights by icao
        airlines_tmp_icao = airlines_tmp_icao.select('airline_iata', 'airline_icao', 'airline_name', 'id')
        airlines_active_flights_df = airlines_tmp_iata.union(airlines_tmp_icao).drop_duplicates(subset=['id'])
        
        return {
            "flights": flights_df,
            "airlines": airlines_df,
            "airlines_flights": airlines_active_flights_df
        }

    def add_timestamp_columns(self, df):
        current_ts = F.current_timestamp()
        df = df.withColumn("tech_year", F.year(current_ts)) \
               .withColumn("tech_month", F.month(current_ts)) \
               .withColumn("tech_day", F.dayofmonth(current_ts)) \
               .withColumn("tech_time", F.date_format(current_ts, 'HH:mm:ss'))
        return df

    def create_dataframes(self, extracted_data):
        df_airports = self.convert_airports_to_df(extracted_data["airports"])
        df_zones = self.convert_zones_to_df(extracted_data["zones"])

        return df_airports, df_zones



if __name__ == "__main__":
    # Créer une instance de DataExtractor en passant fr_api comme argument
    data_extractor = DataExtractor(fr_api)

    # Appelez la méthode clean pour nettoyer les données et générer les nouveaux dataframes nettoyés
    cleaned_data = data_extractor.clean(extracted_data)

    logging.info("Transforming extracted data into DataFrames...")
    # Appelez la méthode create_dataframes pour créer des DataFrames à partir des données extraites
    df_airports, df_zones = data_extractor.create_dataframes(extracted_data_FLIGHT)

    # Appliquez d'autres transformations sur les DataFrames si nécessaire
    df_flights = data_extractor.add_timestamp_columns(df_flights)
    df_airlines = data_extractor.add_timestamp_columns(df_airlines)
    df_airlines_active_flights = data_extractor.add_timestamp_columns(df_airlines_active_flights)
    df_airports = data_extractor.add_timestamp_columns(df_airports)
    df_zones = data_extractor.add_timestamp_columns(df_zones)

    # Liste des noms des 5 continents que vous souhaitez conserver
    continents_a_garder = ["oceania", "asia", "africa", "atlantic", "northatlantic","europe"]

    # Appliquez le filtrage pour ne conserver que les lignes correspondant aux continents spécifiés
    df_zone_filtré = df_zones.filter(df_zones["Zone Name"].isin(continents_a_garder))

    ### Ecriture des données en parquet, partitionnement par date ###

    logging.info("Writing DataFrames to parquet...")
    def write_df_as_parquet(df, output_path):
        df.write.mode("overwrite").partitionBy("tech_year", "tech_month", "tech_day").parquet(output_path)
    
    output_directory = "/FileStore/Bronze/FlightsRadar_API"  
    write_df_as_parquet(df_flights, output_directory  + '/flights')
    write_df_as_parquet(df_airlines, output_directory + '/airlines')
    write_df_as_parquet(df_airlines_active_flights, output_directory + '/airlines_active_flights')
    write_df_as_parquet(df_airports, output_directory + '/airports')
    write_df_as_parquet(df_zone_filtré, output_directory +'/zones')
    
    logging.info("All tasks completed successfully.")