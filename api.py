import os
import pytz
import psycopg2
from flask import Flask, request, jsonify
from config import Configuration

import datetime

app = Flask(__name__)
app.config.from_object(Configuration)

conn = psycopg2.connect(
    dbname=os.environ.get('DATABASE_NAME', default='events_db'),
    user=os.environ.get('DATABASE_USER', default='postgres'),
    password=os.environ.get('DATABASE_PASSWORD', default='postgres_password'),
    host=os.environ.get('DATABASE_HOST', default='localhost'),
    port=os.environ.get('DATABASE_PORT', default='5432')
)


@app.route('/user_stats', methods=['GET'])
def get_user_stats():
    date = None
    # check input data
    if 'user_id' not in request.json:
        return "Missing user_id", 400
    if 'date' in request.json:
        try:
            date = datetime.date.fromisoformat(request.json['date'])
        except ValueError:
            return "Invalid date format, try YYYY-MM-DD", 400

        start_date = datetime.date(year=2024, month=10, day=9)
        end_date = datetime.date(year=2024, month=11, day=3)
        if not (start_date <= date <= end_date):
            return f"Date must be between bounds {start_date} and {end_date} inclusive", 400

    # create answers:
    result = {}

    # get registration data
    with conn.cursor() as user_cursor:
        user_cursor.execute("SELECT country_id, timezone, event_timestamp, u.user_id FROM country.country c "
                            "JOIN events.registration r ON c.country_id=r.country_code "
                            "JOIN users.user u ON u.user_id=r.user_id JOIN events.event e ON e.event_id=r.event_id "
                            "WHERE u.user_name = %s::TEXT", (request.json['user_id'],))
        country_data = user_cursor.fetchone()
    if not country_data:
        return f"No user with given id {request.json['user_id']} in the system", 400
    country_id = country_data[0]
    country_timezone = country_data[1]
    registration_timestamp = country_data[2]
    user_id = country_data[3]


    result['country_id'] = country_id
    result['country_timezone'] = country_timezone
    result['timestamp_local'] = str(registration_timestamp.astimezone(pytz.timezone(country_timezone)))

    # find last login
    if date is not None:
        with conn.cursor() as last_login_cursor:
            last_login_cursor.execute("SELECT s.user_id, MIN(%s::DATE - e.event_timestamp::DATE) "
                                      "FROM events.session s JOIN events.event e ON s.event_id = e.event_id "
                                      "WHERE s.user_id = %s AND s.is_start = 1 AND e.event_timestamp::DATE <= %s::DATE "
                                      "GROUP BY s.user_id ", (date, user_id, date))
            last_login_data = last_login_cursor.fetchone()
        if last_login_data is not None:
            last_login_days = last_login_data[1]
            result['days_since_last_login'] = last_login_days
        else:
            result['days_since_last_login'] = "No last login."

    return jsonify(result), 200



@app.route("/game_stats", methods=['GET'])
def get_game_stats():
    date = None
    if 'date' in request.json:
        try:
            date = datetime.date.fromisoformat(request.json['date'])
        except ValueError:
            return "Invalid date format, try YYYY-MM-DD", 400


if __name__ == '__main__':
    app.run()