from datetime import datetime
import logging
from databricks import DatabricksUtils


# Configuration de base pour le logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Load(DatabricksUtils):
    def __init__(self, path_silver: str):
        self.path_silver = path_silver
        self.airline_df = self.spark.read.parquet(f"{self.path_silver}/most_airlines")
        self.top_regional_df = self.spark.read.parquet(f"{self.path_silver}/top_regional")
        self.find_longest_flight_df = self.spark.read.parquet(f"{self.path_silver}/find_longest_flight")
        self.average_flight_distance_by_continent_df = self.spark.read.parquet(f"{self.path_silver}/average_flight_distance_by_continent")
        self.get_country_aircraft_model_df = self.spark.read.parquet(f"{self.path_silver}/country_aircraft_model")
        self.airport_with_greatest_flights_difference_df = self.spark.read.parquet(f"{self.path_silver}/greatest_flights_difference")

    @classmethod
    def create_gold_folder(cls, base_path):
        # Obtenir la date et l'heure actuelles
        current_datetime = datetime.now()

        # Créer le chemin du dossier Gold avec nomenclature horodatée
        gold_folder_path = f"{base_path}/tech_year={current_datetime.year}/tech_month={current_datetime.strftime('%Y-%m')}/tech_day={current_datetime.strftime('%Y-%m-%d')}"

        return gold_folder_path

    @classmethod
    def save_data_to_gold(cls, base_path, airline_df, top_regional_df, find_longest_flight_df, average_flight_distance_by_continent_df, get_country_aircraft_model_df,airport_with_greatest_flights_difference_df):
        # Créer le dossier Gold
        gold_folder_path = cls.create_gold_folder(base_path)

        # Enregistrer les DataFrames de la couche Gold au format CSV
        airline_df.write.mode("overwrite").csv(f"{gold_folder_path}/most_airlines.csv", header=True)
        top_regional_df.write.mode("overwrite").csv(f"{gold_folder_path}/top_regional.csv", header=True)
        find_longest_flight_df.write.mode("overwrite").csv(f"{gold_folder_path}/find_longest_flight.csv", header=True)
        average_flight_distance_by_continent_df.write.mode("overwrite").csv(f"{gold_folder_path}/average_flight_distance_by_continent.csv", header=True)
        get_country_aircraft_model_df.write.mode("overwrite").csv(f"{gold_folder_path}/country_aircraft_model.csv", header=True)
        airport_with_greatest_flights_difference_df.write.mode("overwrite").csv(f"{gold_folder_path}/greatest_flights_difference", header=True)


if __name__ == "__main__":
    # Chemin vers le répertoire où les Parquets ont été enregistrés
    parquet_directory = "/FileStore/Silver/FlightsRadar_API"

    # Créer une instance de Transformation en passant le chemin du répertoire
    Loader = Load(parquet_directory)
    # Spécifieer le chemin de base pour la couche Gold
    gold_base_path = "/FileStore/Gold/FlightsRadar_API"
    # Appeler la fonction pour créer le dossier Gold avec une nomenclature horodatée
    gold_folder_path = Load.create_gold_folder(gold_base_path)

    # Enregistrer les fichiers CSV dans le dossier Gold en passant les DataFrames comme arguments
    Loader.save_data_to_gold(
        gold_base_path,
        Loader.airline_df,
        Loader.top_regional_df,
        Loader.find_longest_flight_df,
        Loader.average_flight_distance_by_continent_df,
        Loader.get_country_aircraft_model_df,
        Loader.airport_with_greatest_flights_difference_df
    )
