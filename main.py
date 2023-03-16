import datetime
import zipfile
import json
import os
import shutil

from flask import Flask, jsonify, request
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from models import WeatherData
from database import Session

app = Flask(__name__)


def insert_weather_data(json_file):
    with open(json_file) as f:
        data = json.load(f)
    r = []
    weather_data = {'external_temperature_c': data['rows'][1][1], 'wind_speed_unmuted_m_s': data['rows'][2][1],
                    'wind_speed_m_s': data['rows'][3][1], 'wind_direction_degrees': data['rows'][4][1],
                    'wind_direction_compass': data['rows'][5][1]['value'],
                    'radiation_intensity_unmuted_w_m2': data['rows'][6][1],
                    'radiation_intensity_w_m2': data['rows'][7][1],
                    'standard_radiation_intensity_w_m2': data['rows'][8][1], 'radiation_sum_j_cm2': data['rows'][9][1],
                    'radiation_from_plant_w_m2': data['rows'][10][1], 'precipitation': data['rows'][11][1],
                    'relative_humidity_perc': data['rows'][12][1], 'moisture_deficit_g_kg': data['rows'][13][1],
                    'moisture_deficit_g_m3': data['rows'][14][1], 'dew_point_temperature_c': data['rows'][15][1],
                    'abs_humidity_g_kg': data['rows'][16][1], 'enthalpy_kj_kg': data['rows'][17][1],
                    'enthalpy_kj_m3': data['rows'][18][1], 'atmospheric_pressure_hpa': data['rows'][19][1],
                    'status_meteo_station': data['rows'][20][1]['value'],
                    'status_meteo_station_communication': data['rows'][21][1]['value']}

    timestamp_str = data['ts']
    timestamp_obj = datetime.datetime.strptime(timestamp_str[:-6],
                                               '%Y-%m-%dT%H:%M:%S')  # remove timezone offset and parse timestamp
    formatted_timestamp_str = timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')  # format timestamp
    weather_data['timestamp'] = formatted_timestamp_str
    r.append(weather_data)
    json_data = json.dumps(data)
    try:
        session = Session()
        new_data = WeatherData(**weather_data)
        session.add(new_data)
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error occurred while inserting data: {str(e)}")
        raise
    finally:
        session.close()
    return json_data

#An API endpoint to consume the raw data from the greenhouse climate computer
@app.route('/consume-raw-data', methods=['POST'])
def consume_raw_data():
    folder_path = request.json.get('folder_path')
    if not folder_path:
        return jsonify({'message': 'Folder path not provided.'}), 400

    if not os.path.exists(folder_path):
        return jsonify({'message': f'Folder path {folder_path} does not exist.'}), 400

    try:
        extract_folder_path = os.path.join(os.getcwd(), "dumps")
        files_path = os.path.join(extract_folder_path, 'may')
        with zipfile.ZipFile(folder_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder_path)

        # Remove the __MACOSX folder from the extract directory
        mac_folder_path = os.path.join(extract_folder_path, '__MACOSX')
        if os.path.exists(mac_folder_path):
            shutil.rmtree(mac_folder_path)

        response = ''
        for root, dirs, files in os.walk(files_path):
            for file in files:
                if file.endswith('.json'):
                    json_file = os.path.join(root, file)
                    try:
                        response += insert_weather_data(json_file)
                    except Exception as e:
                        error_message = f'Error occurred while processing file {json_file}: {str(e)}\n'
                        print(error_message)
                        response += error_message

        shutil.rmtree(files_path)

        return jsonify({'message': 'Raw data consumed successfully.'}), 200
    except Exception as e:
        error_message = f'Error occurred while processing folder {folder_path}: {str(e)}\n'
        print(error_message)
        return jsonify({'message': error_message}), 500


# Expose the latest weather conditions (i.e. show what's happening now)
@app.route('/api/weather-latest_modifications', methods=['GET'])
def weather_modifications():
    try:
        session = Session()
        latest_data = session.query(WeatherData).order_by(WeatherData.timestamp.desc()).first()
        session.close()
        if latest_data is None:
            return jsonify({'message': 'No data available.'}), 404
        data = {
            'timestamp': str(latest_data.timestamp),
            'external_temperature_c': latest_data.external_temperature_c,
            'wind_speed_unmuted_m_s': latest_data.wind_speed_unmuted_m_s,
            'wind_speed_m_s': latest_data.wind_speed_m_s,
            'wind_direction_degrees': latest_data.wind_direction_degrees,
            'wind_direction_compass': {
                'type': 'hortimax.synopta.enum',
                'key': 8787,
                'value': latest_data.wind_direction_compass
            },
            'radiation_intensity_unmuted_w_m2': latest_data.radiation_intensity_unmuted_w_m2,
            'radiation_intensity_w_m2': latest_data.radiation_intensity_w_m2,
            'standard_radiation_intensity_w_m2': latest_data.standard_radiation_intensity_w_m2,
            'radiation_sum_j_cm2': latest_data.radiation_sum_j_cm2,
            'radiation_from_plant_w_m2': latest_data.radiation_from_plant_w_m2,
            'precipitation': latest_data.precipitation,
            'relative_humidity_perc': latest_data.relative_humidity_perc,
            'moisture_deficit_g_kg': latest_data.moisture_deficit_g_kg,
            'moisture_deficit_g_m3': latest_data.moisture_deficit_g_m3,
            'dew_point_temperature_c': latest_data.dew_point_temperature_c,
            'abs_humidity_g_kg': latest_data.abs_humidity_g_kg,
            'enthalpy_kj_kg': latest_data.enthalpy_kj_kg,
            'enthalpy_kj_m3': latest_data.enthalpy_kj_m3,
            'atmospheric_pressure_hpa': latest_data.atmospheric_pressure_hpa,
            'status_meteo_station': {
                'type': 'hortimax.synopta.enum',
                'key': 8789,
                'value': latest_data.status_meteo_station
            },
            'status_meteo_station_communication': {
                'type': 'hortimax.synopta.enum',
                'key': 8796,
                'value': latest_data.status_meteo_station_communication
            },
        }
        return json.dumps(data)
    except Exception as e:
        return jsonify({'message': 'An error occurred while fetching the latest weather data.', 'error': str(e)}), 500


# Expose the development of the weather parameters over the last 24h in 15 min increments
@app.route('/api/last_day_weather', methods=['GET'])
def get_weather_data():
    try:
        session = Session()
        last_timestamp = session.query(func.max(WeatherData.timestamp)).scalar()
        session.close()

        if not last_timestamp:
            return jsonify({'message': 'No weather data found.'}), 404

        end_time = last_timestamp
        start_time = end_time - datetime.timedelta(hours=24)
        session.close()
        current_time = start_time
        parameters_list = []
        while current_time <= end_time:
            with Session() as session:
                result = session.query(WeatherData).filter(WeatherData.timestamp == current_time).order_by(
                    WeatherData.timestamp.asc()).first()
                if not result:
                    return jsonify({'message': 'Weather data not found for the requested time interval.'}), 404
            parameters = {
                'external_temperature_c': result.external_temperature_c,
                'wind_speed_unmuted_m_s': result.wind_speed_unmuted_m_s,
                'wind_speed_m_s': result.wind_speed_m_s,
                'wind_direction_degrees': result.wind_direction_degrees,
                'wind_direction_compass': {
                    'type': 'hortimax.synopta.enum',
                    'key': 8787,
                    'value': result.wind_direction_compass[0]},
                'radiation_intensity_unmuted_w_m2': result.radiation_intensity_unmuted_w_m2,
                'radiation_intensity_w_m2': result.radiation_intensity_w_m2,
                'standard_radiation_intensity_w_m2': result.standard_radiation_intensity_w_m2,
                'radiation_sum_j_cm2': result.radiation_sum_j_cm2,
                'radiation_from_plant_w_m2': result.radiation_from_plant_w_m2,
                'precipitation': result.precipitation,
                'relative_humidity_perc': result.relative_humidity_perc,
                'moisture_deficit_g_kg': result.moisture_deficit_g_kg,
                'moisture_deficit_g_m3': result.moisture_deficit_g_m3,
                'dew_point_temperature_c': result.dew_point_temperature_c,
                'abs_humidity_g_kg': result.abs_humidity_g_kg,
                'enthalpy_kj_kg': result.enthalpy_kj_kg,
                'enthalpy_kj_m3': result.enthalpy_kj_m3,
                'atmospheric_pressure_hpa': result.atmospheric_pressure_hpa,
                'status_meteo_station':
                    {
                        'type': 'hortimax.synopta.enum',
                        'key': 8789,
                        'value': result.status_meteo_station[0]
                    },

                'status_meteo_station_communication':
                    {
                        'type': 'hortimax.synopta.enum',
                        'key': 8796,
                        'value': result.status_meteo_station_communication[0]
                    },
                'timestamp': str(result.timestamp)
            }
            parameters_list.append({f"parameter {current_time}": parameters})
            interval = request.json.get('interval')
            print("interval is", interval)
            if interval is None or interval == '':
                raise Exception('Interval value is missing')
            if interval % 5 != 0:
                return jsonify({'message': 'The interval must be divisible by 5 since we have minimum increments of 5 '
                                           'minutes'}), 404

            current_time += datetime.timedelta(minutes=interval)

        return jsonify(parameters_list)
    except Exception as e:
        return jsonify({'message': 'An error occurred while fetching the last day weather.', 'error': str(e)}), 500

# Expose the development of the weather parameters over the last 7 days in 1 day increments (average per day)
@app.route('/api/avg-for-several-days-with-one-day-increment', methods=['GET'])
def avg_for_several_days_with_one_day_increment():
    try:
        session = Session()
        increment_interval = request.json.get('increment_interval')
        total_days = request.json.get('total_days')
        if increment_interval != 1:
            return jsonify({'message': 'Please add increment_interval 1'}), 400
        if total_days > 14:
            return jsonify({'message': 'Please add total_days lesser than 14'}), 400
        time_stamp = session.query(func.max(WeatherData.timestamp)).scalar()
        end_time = time_stamp - datetime.timedelta(days=total_days - 1)
        start_time = end_time - datetime.timedelta(days=increment_interval)
        print(f"start_time : {start_time}")
        print(f"end_time : {end_time}")
        daily_averages_list = {}
        for i in range(total_days):
            avg_external_temperature_c = session.query(func.avg(WeatherData.external_temperature_c)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_wind_speed_unmuted_m_s = session.query(func.avg(WeatherData.wind_speed_unmuted_m_s)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_wind_speed_m_s = session.query(func.avg(WeatherData.wind_speed_m_s)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_wind_direction_degrees = session.query(func.avg(WeatherData.wind_direction_degrees)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()
            avg_wind_direction_compass = session.query(WeatherData.wind_direction_compass) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time) \
                .group_by(WeatherData.wind_direction_compass) \
                .order_by(func.count().desc()) \
                .first()

            avg_radiation_intensity_unmuted_w_m2 = session.query(func.avg(WeatherData.radiation_intensity_unmuted_w_m2)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_radiation_intensity_w_m2 = session.query(func.avg(WeatherData.radiation_intensity_w_m2)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_standard_radiation_intensity_w_m2 = session.query(
                func.avg(WeatherData.standard_radiation_intensity_w_m2)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_radiation_sum_j_cm2 = session.query(func.avg(WeatherData.radiation_sum_j_cm2)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_radiation_from_plant_w_m2 = session.query(func.avg(WeatherData.radiation_from_plant_w_m2)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_precipitation = session.query(func.avg(WeatherData.precipitation)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_relative_humidity_perc = session.query(func.avg(WeatherData.relative_humidity_perc)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_moisture_deficit_g_kg = session.query(func.avg(WeatherData.moisture_deficit_g_kg)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_moisture_deficit_g_m3 = session.query(func.avg(WeatherData.moisture_deficit_g_m3)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_dew_point_temperature_c = session.query(func.avg(WeatherData.dew_point_temperature_c)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_abs_humidity_g_kg = session.query(func.avg(WeatherData.abs_humidity_g_kg)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_enthalpy_kj_kg = session.query(func.avg(WeatherData.enthalpy_kj_kg)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_enthalpy_kj_m3 = session.query(func.avg(WeatherData.enthalpy_kj_m3)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_atmospheric_pressure_hpa = session.query(func.avg(WeatherData.atmospheric_pressure_hpa)) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

            avg_status_meteo_station = session.query(WeatherData.status_meteo_station) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time) \
                .group_by(WeatherData.status_meteo_station) \
                .order_by(func.count().desc()) \
                .first()

            avg_status_meteo_station_communication = session.query(WeatherData.status_meteo_station_communication) \
                .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time) \
                .group_by(WeatherData.status_meteo_station_communication) \
                .order_by(func.count().desc()) \
                .first()

            daily_averages = {
                'external_temperature_c': avg_external_temperature_c,
                'wind_speed_unmuted_m_s': avg_wind_speed_unmuted_m_s,
                'wind_speed_m_s': avg_wind_speed_m_s,
                'wind_direction_degrees': avg_wind_direction_degrees,
                'wind_direction_compass': {
                    'type': 'hortimax.synopta.enum',
                    'key': 8787,
                    'value': avg_wind_direction_compass[0]},
                'radiation_intensity_unmuted_w_m2': avg_radiation_intensity_unmuted_w_m2,
                'radiation_intensity_w_m2': avg_radiation_intensity_w_m2,
                'standard_radiation_intensity_w_m2': avg_standard_radiation_intensity_w_m2,
                'radiation_sum_j_cm2': avg_radiation_sum_j_cm2,
                'radiation_from_plant_w_m2': avg_radiation_from_plant_w_m2,
                'precipitation': avg_precipitation,
                'relative_humidity_perc': avg_relative_humidity_perc,
                'moisture_deficit_g_kg': avg_moisture_deficit_g_kg,
                'moisture_deficit_g_m3': avg_moisture_deficit_g_m3,
                'dew_point_temperature_c': avg_dew_point_temperature_c,
                'abs_humidity_g_kg': avg_abs_humidity_g_kg,
                'enthalpy_kj_kg': avg_enthalpy_kj_kg,
                'enthalpy_kj_m3': avg_enthalpy_kj_m3,
                'atmospheric_pressure_hpa': avg_atmospheric_pressure_hpa,
                'status_meteo_station':
                    {
                        'type': 'hortimax.synopta.enum',
                        'key': 8789,
                        'value': avg_status_meteo_station[0]
                    },

                'status_meteo_station_communication':
                    {
                        'type': 'hortimax.synopta.enum',
                        'key': 8796,
                        'value': avg_status_meteo_station_communication[0]
                    }
            }
            daily_averages_list[f"average from {start_time} to {end_time}"] = daily_averages

            end_time = end_time + datetime.timedelta(days=increment_interval)
            start_time = start_time + datetime.timedelta(days=increment_interval)
            print(f"start_time : {start_time}")
            print(f"end_time : {end_time}")

        session.close()
        return jsonify(daily_averages_list)
    except Exception as e:
        return jsonify({'message': 'An error occurred while fetching the averages .', 'error': str(e)}), 500


# Expose the average of the weather parameters over the last 7 days
@app.route('/api/avg-for-several-days', methods=['GET'])
def avg_for_several_days():
    try:
        session = Session()
        total_days = request.json.get('total_days')
        if total_days > 14:
            return jsonify({'Please add total days lesser or equal to 14'}), 400

        end_time = session.query(func.max(WeatherData.timestamp)).scalar()
        start_time = end_time - datetime.timedelta(days=total_days)

        avg_external_temperature_c = session.query(func.avg(WeatherData.external_temperature_c)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_wind_speed_unmuted_m_s = session.query(func.avg(WeatherData.wind_speed_unmuted_m_s)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_wind_speed_m_s = session.query(func.avg(WeatherData.wind_speed_m_s)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_wind_direction_degrees = session.query(func.avg(WeatherData.wind_direction_degrees)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_wind_direction_compass = session.query(WeatherData.wind_direction_compass) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time) \
            .group_by(WeatherData.wind_direction_compass) \
            .order_by(func.count().desc()) \
            .first()

        avg_radiation_intensity_unmuted_w_m2 = session.query(func.avg(WeatherData.radiation_intensity_unmuted_w_m2)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_radiation_intensity_w_m2 = session.query(func.avg(WeatherData.radiation_intensity_w_m2)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_standard_radiation_intensity_w_m2 = session.query(func.avg(WeatherData.standard_radiation_intensity_w_m2)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_radiation_sum_j_cm2 = session.query(func.avg(WeatherData.radiation_sum_j_cm2)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_radiation_from_plant_w_m2 = session.query(func.avg(WeatherData.radiation_from_plant_w_m2)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_precipitation = session.query(func.avg(WeatherData.precipitation)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_relative_humidity_perc = session.query(func.avg(WeatherData.relative_humidity_perc)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_moisture_deficit_g_kg = session.query(func.avg(WeatherData.moisture_deficit_g_kg)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_moisture_deficit_g_m3 = session.query(func.avg(WeatherData.moisture_deficit_g_m3)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_dew_point_temperature_c = session.query(func.avg(WeatherData.dew_point_temperature_c)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_abs_humidity_g_kg = session.query(func.avg(WeatherData.abs_humidity_g_kg)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_enthalpy_kj_kg = session.query(func.avg(WeatherData.enthalpy_kj_kg)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_enthalpy_kj_m3 = session.query(func.avg(WeatherData.enthalpy_kj_m3)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_atmospheric_pressure_hpa = session.query(func.avg(WeatherData.atmospheric_pressure_hpa)) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time).scalar()

        avg_status_meteo_station = session.query(WeatherData.status_meteo_station) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time) \
            .group_by(WeatherData.status_meteo_station) \
            .order_by(func.count().desc()) \
            .first()

        avg_status_meteo_station_communication = session.query(WeatherData.status_meteo_station_communication) \
            .filter(WeatherData.timestamp >= start_time, WeatherData.timestamp <= end_time) \
            .group_by(WeatherData.status_meteo_station_communication) \
            .order_by(func.count().desc()) \
            .first()

        session.close()
        total_average = []
        averages = {
            'external_temperature_c': avg_external_temperature_c,
            'wind_speed_unmuted_m_s': avg_wind_speed_unmuted_m_s,
            'wind_speed_m_s': avg_wind_speed_m_s,
            'wind_direction_degrees': avg_wind_direction_degrees,
            'wind_direction_compass': {
                'type': 'hortimax.synopta.enum',
                'key': 8787,
                'value': avg_wind_direction_compass[0]},
            'radiation_intensity_unmuted_w_m2': avg_radiation_intensity_unmuted_w_m2,
            'radiation_intensity_w_m2': avg_radiation_intensity_w_m2,
            'standard_radiation_intensity_w_m2': avg_standard_radiation_intensity_w_m2,
            'radiation_sum_j_cm2': avg_radiation_sum_j_cm2,
            'radiation_from_plant_w_m2': avg_radiation_from_plant_w_m2,
            'precipitation': avg_precipitation,
            'relative_humidity_perc': avg_relative_humidity_perc,
            'moisture_deficit_g_kg': avg_moisture_deficit_g_kg,
            'moisture_deficit_g_m3': avg_moisture_deficit_g_m3,
            'dew_point_temperature_c': avg_dew_point_temperature_c,
            'abs_humidity_g_kg': avg_abs_humidity_g_kg,
            'enthalpy_kj_kg': avg_enthalpy_kj_kg,
            'enthalpy_kj_m3': avg_enthalpy_kj_m3,
            'atmospheric_pressure_hpa': avg_atmospheric_pressure_hpa,
            'status_meteo_station':
                {
                    'type': 'hortimax.synopta.enum',
                    'key': 8789,
                    'value': avg_status_meteo_station[0]
                },

            'status_meteo_station_communication': {
                'type': 'hortimax.synopta.enum',
                'key': 8796,
                'value': avg_status_meteo_station_communication[0]
            }
        }
        total_average.append({f"average from {start_time} to {end_time}": averages})
        return jsonify(total_average)
    except Exception as e:
        return jsonify(
            {'message': 'An error occurred while fetching the average of several days .', 'error': str(e)}), 500


if __name__ == '__main__':
    # serve(app, host='0.0.0.0', port=8000, threads=1)
    app.run(host='0.0.0.0', port=8000, debug=True)
