from FlightRadar24 import FlightRadar24API
import pandas as pd


fr_api = FlightRadar24API()
flights = fr_api.get_flights()
airports = fr_api.get_airports()
airlines = fr_api.get_airlines()

df_flights = pd.DataFrame(flights)
df_airlines = pd.DataFrame(airlines)
df_airports = pd.DataFrame(airports)

df_airlines.head()
# 1. La compagnie avec le plus de vols en cours
top_airline = df_airlines.value_counts().idxmax()


# 2. Pour chaque continent, la compagnie avec le plus de vols régionaux actifs
regional_flights = df[df['origin_continent'] == df['destination_continent']]
top_airline_by_continent = (
    regional_flights.groupby('origin_continent')['airline'].value_counts().groupby(level=0).idxmax())
# 3. Le vol en cours avec le trajet le plus long
longest_flight = df.loc[df['distance'].idxmax()]

# 4. Pour chaque continent, la longueur de vol moyenne
avg_flight_length_by_continent = df.groupby('origin_continent')['distance'].mean()

# 5. L'entreprise constructeur d'avions avec le plus de vols actifs
top_manufacturer = df['aircraft_manufacturer'].value_counts().idxmax()

# 6. Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage
top_models_by_country = (df.groupby('airline_country')['aircraft_model'].value_counts().groupby(level=0).nlargest(3)
                         .reset_index(level=0, drop=True))
