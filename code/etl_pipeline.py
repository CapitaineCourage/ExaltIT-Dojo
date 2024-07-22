# etl_pipeline.py
import logging
from FlightRadar24 import FlightRadar24API
import requests
import pandas as pd
from sqlalchemy import create_engine
from config import API_URL, DB_CONNECTION_STRING

# Initialiser le logging
logging.basicConfig(filename='flight_etl.log', level=logging.INFO, format='%(asctime)s %(message)s')


# Transform Data
def extract_data():
    fr_api = FlightRadar24API()
    flights = fr_api.get_flights()
    return flights

def extract_data_legacy():
    # Utiliser les requests directement si nécessaire
    response = requests.get(f"{API_URL}/flights")
    if response.status_code == 200:
        return response.json()
    else:
        logging.error("Error fetching data from FlightRadar24 API")
        return None

# Transform Data
def transform_data(flights):
    df = pd.DataFrame(flights)

    # 1. La compagnie avec le plus de vols en cours
    top_airline = df['airline'].value_counts().idxmax()

    # 2. Pour chaque continent, la compagnie avec le plus de vols régionaux actifs
    regional_flights = df[df['origin_continent'] == df['destination_continent']]
    top_airline_by_continent = regional_flights.groupby('origin_continent')['airline'].value_counts().groupby(level=0).idxmax()

    # 3. Le vol en cours avec le trajet le plus long
    longest_flight = df.loc[df['distance'].idxmax()]

    # 4. Pour chaque continent, la longueur de vol moyenne
    avg_flight_length_by_continent = df.groupby('origin_continent')['distance'].mean()

    # 5. L'entreprise constructeur d'avions avec le plus de vols actifs
    top_manufacturer = df['aircraft_manufacturer'].value_counts().idxmax()

    # 6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
    top_models_by_country = df.groupby('airline_country')['aircraft_model'].value_counts().groupby(level=0).nlargest(3).reset_index(level=0, drop=True)

    return {
        'top_airline': top_airline,
        'top_airline_by_continent': top_airline_by_continent,
        'longest_flight': longest_flight,
        'avg_flight_length_by_continent': avg_flight_length_by_continent,
        'top_manufacturer': top_manufacturer,
        'top_models_by_country': top_models_by_country
    }


# Load data
def load_data(transformed_data):
    engine = create_engine(DB_CONNECTION_STRING)

    with engine.connect() as connection:
        for key, value in transformed_data.items():
            if isinstance(value, pd.DataFrame):
                value.to_sql(key, connection, if_exists='replace')
            else:
                # Convertir en DataFrame pour l'insertion
                df = pd.DataFrame(value)
                df.to_sql(key, connection, if_exists='replace')

    logging.info("Data loaded successfully")
