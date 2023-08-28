import argparse
import pandas as pd
import os
import boto3
from io import BytesIO
from io import StringIO
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def fetch_weather_data(station_id, year, month, day, timeframe):
    url = f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&Month={month}&Day={day}&timeframe={timeframe}"
    response = requests.get(url, verify=False)
    csv_data = response.text
    data_frame = pd.read_csv(StringIO(csv_data))
    return data_frame


def write_to_excel(data_frame):
    grouped_data = data_frame.groupby("Year")
    excel_writer = pd.ExcelWriter("C:/Users/H336013/PycharmProjects/WeatherData/filtered_weather_data.xlsx", engine="xlsxwriter")

    for year, group in grouped_data:
        group.to_excel(excel_writer, sheet_name=str(year), index=False)

    excel_writer.close()


def extract_weather_data(city, input_year):
    if input_year < 2018:
        print("Please provide a year that is greater than or equal to 2018.")
        return

    csv_directory = "C:/Users/H336013/PycharmProjects/WeatherData"
    station_inv_file = "Station_Inventory_EN.csv"
    station_inv_file_path = os.path.join(csv_directory, station_inv_file)

    # Read Station Inventory data
    station_inventory = pd.read_csv(station_inv_file_path, skiprows=3)

    # Get the station ID for the specified city
    station_id = station_inventory[station_inventory["Name"] == city]["Station ID"]

    if not station_id.empty:
        station_id = station_id.values[0]
    else:
        print(f"No station ID found for city: {city}")
        return

    '''    
            csv_files = [
                "en_climate_daily_ON_6158355_2018_P1D.csv",
                "en_climate_daily_ON_6158355_2019_P1D.csv",
                "en_climate_daily_ON_6158355_2020_P1D.csv"
            ]

            data_frames = []

            for csv_file in csv_files:
                csv_file_path = os.path.join(csv_directory, csv_file)
                data_frame = pd.read_csv(csv_file_path)
                data_frames.append(data_frame)

            combined_weather_data = pd.concat(data_frames, ignore_index=True)
        '''

    # Fetch weather data for the last 3 years based on input year
    combined_weather_data = pd.DataFrame()
    for year_offset in range(0, 3):
        year_to_fetch = input_year - year_offset
        weather_data = fetch_weather_data(station_id, year_to_fetch, month=1, day=14, timeframe=2)
        combined_weather_data = pd.concat([combined_weather_data, weather_data], ignore_index=True)

    return combined_weather_data, station_inventory


def main():
    parser = argparse.ArgumentParser(description="Process weather data based on city and year")
    parser.add_argument("--city", type=str, help="City name")
    parser.add_argument("--year", type=int, help="Year (YYYY)")

    args = parser.parse_args()

    city = args.city.upper()
    input_year = args.year

    # Extract Weather Data
    combined_weather_data, station_inventory = extract_weather_data(city, input_year)

    ''' 
            output_csv_path = os.path.join(csv_directory, "combined_weather_data.csv")
            combined_weather_data.to_csv(output_csv_path, index=False)
            print("combined weather data has been written to:", output_csv_path)
    '''

    # Join Weather,Station Data
    station_inventory['Climate ID'] = station_inventory['Climate ID'].astype(str)
    combined_weather_data['Climate ID'] = combined_weather_data['Climate ID'].astype(str)
    merged_data = pd.merge(station_inventory, combined_weather_data, on="Climate ID")

    '''
        output_csv_path = os.path.join(csv_directory, "merged_station_weather_data.csv")
        merged_data.to_csv(output_csv_path, index=False)
        print("Merged station and weather data has been written to:", output_csv_path)
    '''

    # Cleanup
    merged_data['Date/Time'] = pd.to_datetime(merged_data['Date/Time'])
    current_date = pd.to_datetime("today").date()
    merged_data = merged_data[(merged_data["Date/Time"].dt.date <= current_date)]
    filtered_data = merged_data.dropna(subset=["Max Temp (°C)", "Min Temp (°C)"])
    columns_to_keep = [
        "Station Name",
        "Province",
        "Station ID",
        "Climate ID",
        "Longitude (x)",
        "Latitude (y)",
        "Date/Time",
        "Year",
        "Month",
        "WMO ID",
        "TC ID",
        "Max Temp (°C)",
        "Min Temp (°C)",
        "Mean Temp (°C)"
    ]
    filtered_data = filtered_data[columns_to_keep]

    # upload to S3
    csv_buffer = StringIO()
    filtered_data.to_csv(csv_buffer, index=False)
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    s3_client = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)
    s3_bucket_name = "weather-wave"
    s3_folder_name = "wave"
    s3_file_name = f"filtered_data.csv"
    s3_path = f"{s3_folder_name}/{city}/{input_year}/{s3_file_name}"
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=s3_bucket_name, Key=s3_path)

    print("Filtered data has been uploaded to S3.")

    '''
        output_csv_path = os.path.join(csv_directory, "filtered_data.csv")
        filtered_data.to_csv(output_csv_path, index=False)
        print("Filtered data has been written to:", output_csv_path)
    '''

    # Write to Excel
    write_to_excel(filtered_data)

    print("Final Excel file containing weather information segregated by Year has been generated.")

    # Connect to S3 and read CSV data
    response = s3_client.get_object(Bucket="weather-wave", Key=s3_path)
    csv_data = response['Body'].read()
    data_frame = pd.read_csv(BytesIO(csv_data))
    # Filter data for the specified year and city
    queried_data = data_frame[(data_frame["Year"] == input_year) & (data_frame["Station Name"] == city)]

    # Max temperature for the Year
    max_temp = queried_data['Max Temp (°C)'].max()
    # Min temperature for the Year
    min_temp = queried_data['Min Temp (°C)'].min()

    # Print results
    print(f"Max temperature for {input_year} in {city}: {max_temp} °C")
    print(f"Min temperature for {input_year} in {city}: {min_temp} °C")

    # Calculate the average temperature for the specified year
    avg_temp_year = queried_data['Mean Temp (°C)'].mean()

    # Calculate the average temperature for the previous two years
    avg_temp_prev_years = data_frame[(data_frame["Year"] == input_year - 1) | (data_frame["Year"] == input_year - 2)][
        'Mean Temp (°C)'].mean()

    # Calculate the percentage difference
    if avg_temp_prev_years != 0:
        percentage_difference = ((avg_temp_year - avg_temp_prev_years) / avg_temp_prev_years) * 100
    else:
        percentage_difference = 0

    # Print results
    print(f"Average temperature for {input_year} in {city}: {avg_temp_year:.2f} °C")
    print(f"Average temperature for the previous two years: {avg_temp_prev_years:.2f} °C")
    print(f"Percentage difference: {percentage_difference:.2f}%")

    # Calculate the average temperature per month
    avg_temp_per_month = queried_data.groupby("Month")['Mean Temp (°C)'].mean()

    # Calculate the difference between consecutive months
    monthly_difference = avg_temp_per_month.diff()

    # Print results
    print(f"Average temperature per month for {input_year} in {city}:\n{avg_temp_per_month}")
    print(f"Difference between average temperatures per month:\n{monthly_difference}")


if __name__ == "__main__":
    main()
