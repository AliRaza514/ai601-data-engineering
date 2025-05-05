import requests
import csv

# Open-Meteo API URL
URL = "https://api.open-meteo.com/v1/forecast?latitude=31.5&longitude=74.375&current_weather=true"

### Fetch Weather Data
def fetch_weather_data():
    """Fetches weather data for Lahore."""
    response = requests.get(URL)
    
    if response.status_code == 200:
        return response.json()  # Return JSON response
    else:
        print("Failed to retrieve data:", response.status_code)
        return None

### Save Data to CSV
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Temperature (°C)', 'Wind Speed (m/s)', 'Wind Direction (°)', 'Timestamp'])

        if data:
            temp = data['current_weather']['temperature']
            wind_speed = data['current_weather']['windspeed'] / 3.6  # Convert km/h to m/s
            wind_direction = data['current_weather']['winddirection']
            timestamp = data['current_weather']['time']
            writer.writerow([temp, wind_speed, wind_direction, timestamp])

### Clean Data
def clean_data(input_file, output_file):
    """Clean the data based on basic validation rules."""
    cleaned_data = []

    with open(input_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header

        for row in reader:
            print(f"Raw Row: {row}")  # Debug print
            try:
                temp = float(row[0])  # Temperature in °C
                wind_speed = float(row[1])  # Wind speed in m/s

                if 0 <= temp <= 60 and 0.83 <= wind_speed <= 41.67:  # Wind speed valid range
                    cleaned_data.append(row)
                else:
                    print(f" Row removed: {row}")  # Debug print
            except ValueError:
                print(f" Invalid data format: {row}")  # Debug print
                continue  # Skip invalid rows

    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(cleaned_data)

    print(f" Cleaned Data: {cleaned_data}")  # Debug print


### Summarize Data
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        data = list(reader)

        if not data:
            print("No data available to summarize.")
            return

        temperatures = [float(row[0]) for row in data if row[0]]
        wind_speeds = [float(row[1]) for row in data if row[1]]

        total_records = len(data)
        avg_temp = sum(temperatures) / total_records
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_wind_speed = sum(wind_speeds) / total_records

        print(" Weather Data Summary ")
        print(f"Total Records: {total_records}")
        print(f" Average Temperature: {avg_temp:.2f}°C")
        print(f" Max Temperature: {max_temp:.2f}°C")
        print(f" Min Temperature: {min_temp:.2f}°C")
        print(f" Average Wind Speed: {avg_wind_speed:.2f} m/s")

### Main Execution
if __name__ == "__main__":
    weather_data = fetch_weather_data()
    
    if weather_data:
        print(weather_data)  # Debug print
        save_to_csv(weather_data, "weather_data.csv")
        print("Weather data saved to weather_data.csv")

        clean_data("weather_data.csv", "cleaned_data.csv")
        print("Cleaned data saved to cleaned_data.csv")

        summarize_data("cleaned_data.csv")


# import requests
# import csv

# # Your API key here
# URL = "https://api.weatherapi.com/v1/current.json?key=YOUR_API_KEY&q=Lahore"

# ### Part 1. Read Operation (Extract)
# def fetch_weather_data():
#     """Fetches weather data for Lahore."""
#     response = requests.get(URL)
    
#     if response.status_code == 200:
#         # The response is in JSON format, so return it
#         return response.json()
#     else:
#         print("Failed to retrieve data:", response.status_code)
#         return None

# ### Part 2. Write Operation (Load)
# def save_to_csv(data, filename):
#     """Saves weather data to a CSV file."""
#     # Open the file in write mode ('w')
#     with open(filename, "w", newline='', encoding='utf-8') as file:
#         writer = csv.writer(file)
        
#         # Write the header
#         writer.writerow(['Temperature (°C)', 'Humidity (%)', 'Wind Speed (m/s)', 'Condition'])

#         # Write the weather data (body)
#         if data:
#             temp = data['current']['temp_c']
#             humidity = data['current']['humidity']
#             wind_speed = data['current']['wind_kph'] / 3.6  # Convert kph to m/s
#             condition = data['current']['condition']['text']
#             writer.writerow([temp, humidity, wind_speed, condition])

#         return None

# ### Part 3. Cleaning Operation (Transform)
# def clean_data(input_file, output_file):
#     """Clean the data based on the following rules:
#         1. Temperature should be between 0 and 60°C
#         2. Humidity should be between 0% and 80%
#         3. Wind speed should be between 3 and 150 m/s
#     """
#     cleaned_data = []

#     with open(input_file, 'r', encoding='utf-8') as file:
#         reader = csv.reader(file)
#         headers = next(reader)  # Skip header row
        
#         for row in reader:
#             temp = float(row[0])
#             humidity = float(row[1])
#             wind_speed = float(row[2])

#             # Apply cleaning rules
#             if 0 <= temp <= 60 and 0 <= humidity <= 80 and 3 <= wind_speed <= 150:
#                 cleaned_data.append(row)

#     # Save cleaned data to a new file
#     with open(output_file, 'w', newline='', encoding='utf-8') as file:
#         writer = csv.writer(file)
#         writer.writerow(headers)  # Write header
#         writer.writerows(cleaned_data)  # Write cleaned rows

#     print("Cleaned data saved to", output_file)

# ### Part 4. Aggregation Operation
# def summarize_data(filename):
#     """Summarizes weather data including averages and extremes."""
#     with open(filename, 'r', encoding='utf-8') as file:
#         reader = csv.reader(file)
#         headers = next(reader)  # Read header row
#         data = list(reader)  # Convert CSV data to list

#         # Ensure we have data
#         if not data:
#             print("No data available to summarize.")
#             return

#         # Extract values from columns
#         temperatures = [float(row[0]) for row in data if row[0]]
#         humidity_values = [float(row[1]) for row in data if row[1]]
#         wind_speeds = [float(row[2]) for row in data if row[2]]

#         # Compute statistics
#         total_records = len(data)
#         avg_temp = sum(temperatures) / total_records
#         max_temp = max(temperatures)
#         min_temp = min(temperatures)
#         avg_humidity = sum(humidity_values) / total_records
#         avg_wind_speed = sum(wind_speeds) / total_records

#         # Print summary
#         print("📊 Weather Data Summary 📊")
#         print(f"Total Records: {total_records}")
#         print(f"🌡️ Average Temperature: {avg_temp:.2f}°C")
#         print(f"🔥 Max Temperature: {max_temp:.2f}°C")
#         print(f"❄️ Min Temperature: {min_temp:.2f}°C")
#         print(f"💧 Average Humidity: {avg_humidity:.1f}%")
#         print(f"💨 Average Wind Speed: {avg_wind_speed:.2f} m/s")


# if __name__ == "__main__":
#     # Fetch weather data
#     weather_data = fetch_weather_data()
    
#     if weather_data:
#         # Save to CSV
#         save_to_csv(weather_data, "weather_data.csv")
#         print("Weather data saved to weather_data.csv")
        
#         # Clean the data and save to a new file
#         clean_data("weather_data.csv", "cleaned_data.csv")
        
#         # Summarize cleaned data
#         summarize_data("cleaned_data.csv")
