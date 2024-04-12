from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import country_converter as coco
from pyspark.sql.functions import col, count, row_number
from geopy.distance import great_circle
from pyspark.sql.types import FloatType , StringType
import logging
from geopy.distance import geodesic
from databricks import DatabricksUtils


# Configuration de base pour le logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Transformation(DatabricksUtils):
    def __init__(self, path_bronze: str):
        self.path_bronze = path_bronze
        self.df_flights = spark.read.parquet(f"{self.path_bronze}/flights")
        self.df_airlines = spark.read.parquet(f"{self.path_bronze}/airlines")
        self.df_airlines_active_flights = spark.read.parquet(f"{self.path_bronze}/airlines_active_flights")
        self.df_airports = spark.read.parquet(f"{self.path_bronze}/airports")
        self.df_zones = spark.read.parquet(f"{self.path_bronze}/zones")
    
    def get_max(self,df, col, by):
            """ 
            A function to return rows with the max value of a column by groups
            inputs : 
                    - df: dataframe
                    - col: a column of the df that we look for the max value
                    - by : column to group the dataframe by
            output : dataframe having the max value
            """
            if by == '':
                max_column = df.agg(F.max(col))\
                            .collect()[0]['max({})'.format(col)]
                df = df.filter(F.col(col) == max_column)
            else:
                w = Window.partitionBy(by)
                df = df.withColumn('max', F.max(col).over(w))\
                                        .filter(F.col(col) == F.col('max'))\
                                        .drop('max')

            return df
        
    def active_flights(self,df, cols):
                """ 
                A function to return active flights in a dataframe
                Considering that an active flight is a flight moving through the air --> "on_ground" == 0
                inputs : 
                        - df:  flights dataframe
                        - cols: dataframe columns to return
                output : the dataframe of active flights (selected by input columns)
                """
                return df.filter(F.col('on_ground') == 0).select(cols)

    def add_route_length_column(self, df_flights, df_airports):
        #les colonnes nécessaires dans df_flights et df_airports
        flights_tmp = df_flights.select('id', 'origin_airport_iata', 'destination_airport_iata')
        airports_coord = df_airports.select('IATA Code', 'Latitude', 'Longitude')
        origin_coord = airports_coord.toDF(*['origin_airport_iata', 'origin_airport_lat', 'origin_airport_lon'])
        destination_coord = airports_coord.toDF(*['destination_airport_iata', 'destination_airport_lat', 'destination_airport_lon'])

        #les coordonnées d'origine et de destination avec les vols
        flights_tmp = flights_tmp.join(origin_coord, 'origin_airport_iata', 'inner') \
                                 .join(destination_coord, 'destination_airport_iata', 'inner')

        # Définisser une UDF pour calculer la distance geodesique
        @udf(FloatType())
        def calculate_geodesic_distance(lat_flight, lon_flight, lat_origin, lon_origin):
            return geodesic((lat_flight, lon_flight), (lat_origin, lon_origin)).kilometers

        # Calculer la distance geodesique pour chaque ligne
        flights_tmp = flights_tmp.withColumn(
            "route_length",
            calculate_geodesic_distance(
                flights_tmp['destination_airport_lat'], flights_tmp['destination_airport_lon'],
                flights_tmp['origin_airport_lat'], flights_tmp['origin_airport_lon']
            )
        )

        # Sélectionner les colonnes pertinentes et rejoignez-les à df_flights
        flights_tmp = flights_tmp.select('id', 'route_length')
        df_flights = df_flights.join(flights_tmp, 'id', 'left')

        return df_flights

    def add_continent_columnn(self, df_airports, df_zones):
        """
        Ajoute une colonne 'continent' au DataFrame des aéroports en fonction des informations contenues dans df_zones.

        :param df_airports: DataFrame des aéroports
        :param df_zones: DataFrame contenant les informations des zones continentales
        :return: DataFrame des aéroports avec la colonne 'continent' mise à jour
        """

        # Initialisez la colonne 'continent' avec des valeurs nulles
        df_airports = df_airports.withColumn("continent", F.lit(None).cast(StringType()))
        # Créer une nouvelle variable pour stocker la version mise à jour du DataFrame des aéroports
        updated_airports_df = df_airports

        # Parcourir le DataFrame df_zones et ajoutez une colonne continent en fonction des coordonnées
        for continent_row in df_zones.collect():
            continent_name = continent_row['Zone Name']
            tl_x = continent_row['TL_X']
            tl_y = continent_row['TL_Y']
            br_x = continent_row['BR_X']
            br_y = continent_row['BR_Y']

            # Utiliser la fonction when pour effectuer la correspondance sur la nouvelle variable
            updated_airports_df = updated_airports_df.withColumn(
                "continent",
                F.when(
                    (updated_airports_df.Latitude >= tl_x) &
                    (updated_airports_df.Latitude  <= br_x) &
                    (updated_airports_df.Longitude >= br_y) &
                    (updated_airports_df.Longitude <= tl_y),
                    continent_name
                ).otherwise(updated_airports_df.continent)
            )

        return updated_airports_df

###############################################################################################################################################

    def airline_with_most_flights(self) -> DataFrame:
        window_on_ground = Window.partitionBy("callsign")
        df_flights_count = self.df_flights \
            .withColumn("on_ground_list", F.collect_set(F.col("on_ground")).over(window_on_ground)) \
            .filter(~F.array_contains(F.col("on_ground_list"), 1)) \
            .filter(col("airline_icao") != 'N/A')\
            .filter(col('on_ground') == 0)\
            .groupBy("airline_icao")\
            .agg(F.countDistinct("callsign").alias("number_of_flights"))\
            .sort(F.desc("number_of_flights"))
        top_airline = df_flights_count.limit(1)
        result = top_airline.join(self.df_airlines, top_airline.airline_icao == self.df_airlines.ICAO, 'left')\
                            .select("airline_icao", "Name", "number_of_flights")

        return result
    
    ###############################################################################################################################################
    def most_regional_active_flights_companies_by_continent(self):
        active_flights_df = self.active_flights(self.df_flights, ["id", "origin_airport_iata", "destination_airport_iata"])
        df_airports_temp = self.add_continent_columnn(self.df_airports, self.df_zones)
        airports_origin = df_airports_temp \
            .select("IATA Code", "continent") \
            .toDF(*["origin_airport_iata", "origin_continent"])
        airports_destination = df_airports_temp \
            .select("IATA Code", "continent")\
            .toDF(*["destination_airport_iata", "destination_continent"])
        active_flights_df = active_flights_df \
            .join(airports_origin, "origin_airport_iata", how='left') \
            .join(airports_destination, "destination_airport_iata", how='left') \
            .na.drop(subset=['origin_continent']) \
            .filter(F.col('origin_continent') != 'NaN') \
            .drop_duplicates()
        active_flights_df = active_flights_df \
            .select("id", "origin_airport_iata", "origin_continent", "destination_airport_iata", "destination_continent")
        regional_active_flights_df = active_flights_df \
            .filter(active_flights_df['origin_continent'] == active_flights_df['destination_continent'])
        regional_active_flights_df = active_flights_df \
            .join(self.df_airlines_active_flights, "id", how = "left")
        count_regional_active_flights_df = regional_active_flights_df \
            .groupBy(['origin_continent', 'airline_iata', 'airline_icao', 'airline_name']) \
            .agg(F.count('id').alias('active_flights'))
        most_active_airline_by_continent = self.get_max(count_regional_active_flights_df, 'active_flights','origin_continent' )
        most_active_airline_by_continent = most_active_airline_by_continent \
            .select('origin_continent', 'airline_iata', 'airline_icao', 'airline_name', 'active_flights')
        most_active_airline_by_continent = most_active_airline_by_continent \
            .toDF(*['continent', 'airline_iata', 'airline_icao', 'airline_name', 'active_flights'])
        most_active_airline_by_continent = most_active_airline_by_continent.sort(F.col('active_flights').desc())
        
        return most_active_airline_by_continent

    ###############################################################################################################################################
    def active_flight_with_longest_route(self):
        df_flights_temp = self.add_route_length_column(self.df_flights, self.df_airports)
        active_flights_df = self.active_flights(df_flights_temp, df_flights_temp.toPandas().columns.tolist())\
                            .filter(F.col('route_length') != 'NaN')
        active_flight_longest_route = self.get_max(active_flights_df, 'route_length', '')

        return active_flight_longest_route.select( "id","aircraft_code", "airline_icao", "callsign","route_length")

    ###############################################################################################################################################
    def average_flight_distance_by_continent(self):
        df_flights_temp = self.add_route_length_column(self.df_flights, self.df_airports)
        df_airports_temp = self.add_continent_columnn(self.df_airports, self.df_zones)
        flights_route_df = df_flights_temp.select("id", "origin_airport_iata", "route_length")\
                                .filter(F.col('route_length') != 'NaN')
        
        airports_origin = df_airports_temp.select("IATA Code", "continent")\
                                    .toDF(*["origin_airport_iata", "origin_continent"])    
        
        # Ajouter la colonne 'origin_continent' 
        flights_route_df = flights_route_df.join(airports_origin, "origin_airport_iata", how='inner')\
                                        .na.drop(subset=['origin_continent'])\
                                        .filter(F.col('origin_continent') != 'NaN')
        
        # Utiliser la fenêtre spécifiée pour agréger par continent
        window_spec = Window.partitionBy("origin_continent")
        avg_root_distance_by_continent = flights_route_df\
            .withColumn("average_route_distance", F.avg("route_length").over(window_spec))\
            .select("origin_continent", "average_route_distance")\
            .distinct()
        
        return avg_root_distance_by_continent
        
    ###############################################################################################################################################
    def get_constractor_active_flights(self):
        return True
        
    ###############################################################################################################################################
    def get_country_aircraft_model(self):
        _aeroports_origin = self.df_airports.select(F.col('IATA Code').alias('iata_origin'), F.col('Latitude').alias('latitude_origin'), F.col('Longitude').alias('longitude_origin'), F.col("Country").alias("country_origin"))
        result_df = self.df_flights \
            .join(
                _aeroports_origin,
                self.df_flights['origin_airport_iata'] == _aeroports_origin['iata_origin'],
                'left'
            )
        # Utilisation d'une fenêtre pour calculer le classement des modèles d'avion par pays
        window_spec = Window.partitionBy("country_origin", "aircraft_code")
        result_df = result_df \
            .withColumn("count_model", F.count("*").over(window_spec)) \
            .select("country_origin", "aircraft_code", "count_model") \
            .dropDuplicates()
        # Classer les modèles d'avion par pays en fonction de leur utilisation décroissante
        window_spec = Window.partitionBy("country_origin").orderBy(F.col("count_model").desc())
        ranked_df = result_df.withColumn("rank", F.row_number().over(window_spec))
        # Filtrer pour obtenir les trois premiers modèles d'avion par pays
        top_3_models_per_country = ranked_df.filter(F.col("rank") <= 3)
        top_3_models_per_country = top_3_models_per_country.na.drop()
        return top_3_models_per_country.select("country_origin", "aircraft_code", "count_model", "rank")
    
    
    ###############################################################################################################################################
    def airport_with_greatest_flights_difference(self):
        airports_df = self.df_airports.select("IATA Code", "Name").toDF(*["IATA Code", "airport_name"])
        inbound_flights = self.df_flights.select(["id", "origin_airport_iata"]).toDF(*['id', 'IATA Code'])
        outbound_flights = self.df_flights.select(["id", "destination_airport_iata"]).toDF(*['id', 'IATA Code'])

        inbound_flights = inbound_flights \
            .join(airports_df, 'IATA Code', how='left') \
            .groupBy(['IATA Code', 'airport_name']) \
            .agg(F.count("id").alias('active_flights_in')) 
        outbound_flights = outbound_flights \
            .join(airports_df , 'IATA Code', how='left') \
            .groupBy(['IATA Code', 'airport_name']) \
            .agg(F.count("id").alias('active_flights_out'))

        greatest_airport_df = inbound_flights \
            .join(outbound_flights, ['IATA Code', 'airport_name'], how='inner') \
            .withColumn('active_flights', F.abs(F.col('active_flights_in') - F.col('active_flights_out')))
        greatest_airport_df = self.get_max(greatest_airport_df, 'active_flights', '')

        
        return greatest_airport_df

    

if __name__ == "__main__":
    # Chemin vers le répertoire où les Parquets ont été enregistrés
    parquet_directory = "/FileStore/Bronze/FlightsRadar_API"

    # Créer une instance de Transformation en passant le chemin du répertoire
    transformer = Transformation(parquet_directory)

    logging.info("Preparing DataFrames...")
    # Question 1
    airline_df = transformer.airline_with_most_flights()
    # Question 2
    top_regional_df = transformer.most_regional_active_flights_companies_by_continent()
    # Question 3
    find_longest_flight_df = transformer.active_flight_with_longest_route()
    # Question 4
    average_flight_distance_by_continent_df = transformer.average_flight_distance_by_continent()
    # Question 6
    get_country_aircraft_model_df = transformer.get_country_aircraft_model()
    # Question Bonus
    airport_with_greatest_flights_difference_df = transformer.airport_with_greatest_flights_difference()
    logging.info("All tasks completed successfully.")

    logging.info("Writing DataFrames to parquet...")
    def write_df_as_parquet(df, output_path):
        df.write.mode("overwrite").parquet(output_path)
    
    output_directory = "/FileStore/Silver/FlightsRadar_API"  
    write_df_as_parquet(airline_df, output_directory  + '/most_airlines')
    write_df_as_parquet(top_regional_df, output_directory + '/top_regional')
    write_df_as_parquet(find_longest_flight_df, output_directory + '/find_longest_flight')
    write_df_as_parquet(average_flight_distance_by_continent_df, output_directory +'/average_flight_distance_by_continent')
    write_df_as_parquet(get_country_aircraft_model_df, output_directory +'/country_aircraft_model')
    write_df_as_parquet(airport_with_greatest_flights_difference_df, output_directory +'/greatest_flights_difference')
    
    logging.info("All tasks completed successfully.")
