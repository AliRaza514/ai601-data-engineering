print("Yep, your environment works")
import requests
import pandas as pd
from datetime import datetime, timedelta

# Define the location for Lahore and the API URL
latitude = 31.5497
longitude = 74.3436
url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&daily=temperature_2m_max,temperature_2m_min,precipitation_sum&start={datetime.now() - timedelta(days=10):%Y-%m-%d}&end={datetime.now():%Y-%m-%d}&temperature_unit=celsius&timezone=Asia/Karachi"

# Send a GET request to fetch data
response = requests.get(url)
data = response.json()

# Parse the JSON data and prepare it for saving to CSV
weather_data = {
    'date': [],
    'temperature_max': [],
    'temperature_min': [],
    'precipitation': []
}

for day in data['daily']['time']:
    weather_data['date'].append(day)
    weather_data['temperature_max'].append(data['daily']['temperature_2m_max'][data['daily']['time'].index(day)])
    weather_data['temperature_min'].append(data['daily']['temperature_2m_min'][data['daily']['time'].index(day)])
    weather_data['precipitation'].append(data['daily']['precipitation_sum'][data['daily']['time'].index(day)])

# Save the data to CSV
df = pd.DataFrame(weather_data)
df.to_csv('weather_data.csv', index=False)

print("Weather data saved to 'weather_data.csv'")


import pandas as pd

# Load the CSV file
df = pd.read_csv('weather_data.csv')

# Example cleaning rules:
# 1. Fill missing temperature values with the mean of the respective columns
df['temperature_max'].fillna(df['temperature_max'].mean(), inplace=True)
df['temperature_min'].fillna(df['temperature_min'].mean(), inplace=True)

# 2. Remove rows with NaN values (optional, if you prefer to discard incomplete rows)
df.dropna(inplace=True)

# 3. Remove rows where temperature is unrealistic (for example, temperatures below -50°C)
df = df[df['temperature_max'] >= -50]

# 4. Ensure the 'date' column is in the correct format
df['date'] = pd.to_datetime(df['date'], errors='coerce')

# Remove rows where the date conversion failed
df.dropna(subset=['date'], inplace=True)

# Save the cleaned data to a new CSV file
df.to_csv('cleaned_data.csv', index=False)

print("Data cleaned and saved to 'cleaned_data.csv'")



# Load the cleaned dataset
df_cleaned = pd.read_csv('cleaned_data.csv')

# Compute summary statistics for temperature_max, temperature_min, and precipitation
summary_stats = df_cleaned.describe()

# Print the summary statistics to the console
print(summary_stats)

# You can also calculate specific statistics if needed
mean_temperature_max = df_cleaned['temperature_max'].mean()
median_temperature_max = df_cleaned['temperature_max'].median()
max_temperature_max = df_cleaned['temperature_max'].max()
min_temperature_max = df_cleaned['temperature_max'].min()

print(f"\nMean Max Temperature: {mean_temperature_max}")
print(f"Median Max Temperature: {median_temperature_max}")
print(f"Max Max Temperature: {max_temperature_max}")
print(f"Min Max Temperature: {min_temperature_max}")
