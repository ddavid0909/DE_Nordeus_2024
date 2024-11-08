import os
import psycopg2
import config
import json

from flask import Flask

api = Flask(__name__)
api.config.from_object(config.Configuration)

conn = psycopg2.connect(
    dbname=os.environ.get('DATABASE_NAME', default='events_db'),
    user=os.environ.get('DATABASE_USER', default='postgres'),
    password=os.environ.get('DATABASE_PASSWORD', default='postgres_password'),
    host=os.environ.get('DATABASE_HOST', default='localhost'),
    port=os.environ.get('DATABASE_PORT', default='5432')
)


def match(opened_cursor, event_data, event_id):
    if ('match_id' not in event_data
            or 'home_user_id' not in event_data
            or 'away_user_id' not in event_data
            or 'home_goals_scored' not in event_data
            or 'away_goals_scored' not in event_data):
        return 1
    opened_cursor.execute(
        "INSERT INTO events.match(event_id, match_id, home_user_id, away_user_id, home_goals_scored, away_goals_scored)"
        "SELECT event_id, match_id, home_user_id, away_user_id, home_goals_scored::INTEGER, away_goals_scored::INTEGER "
        "FROM ("
        "SELECT %s as event_id, %s as match_id, "
        "(SELECT(user_id) FROM users.user WHERE user_name=%s) as home_user_id,"
        "(SELECT(user_id) FROM users.user WHERE user_name=%s) as away_user_id,"
        "%s as home_goals_scored, "
        "%s as away_goals_scored)"
        "WHERE home_user_id IS NOT NULL AND away_user_id IS NOT NULL AND match_id IS NOT NULL "
        "ON CONFLICT(event_id) DO NOTHING "
        "RETURNING *"
        , (event_id, event_data['match_id'],
           event_data['home_user_id'], event_data['away_user_id'],
           event_data['home_goals_scored'], event_data['away_goals_scored']))
    if opened_cursor.fetchone():
        return 0
    return 2

def registration(opened_cursor, event_data, event_id):
    if 'user_id' not in event_data or 'country' not in event_data or 'device_os' not in event_data:
        return 1
    opened_cursor.execute("INSERT INTO users.user(user_name) VALUES (%s) ON CONFLICT(user_name) DO NOTHING",
                          (event_data['user_id'],))
    opened_cursor.execute("INSERT INTO events.registration(event_id, user_id, device_id, country_code) "
                          "SELECT %s , user_id, device_id, country_id "
                          "FROM (SELECT "
                          "(SELECT user_id FROM users.user WHERE user_name = %s) as user_id, "
                          "(SELECT device_id FROM device.device WHERE device_os = LOWER(%s)) as device_id, "
                          "(SELECT country_id FROM country.country WHERE country_id = UPPER(%s)) as country_id)"
                          "WHERE user_id IS NOT NULL AND device_id IS NOT NULL AND country_id IS NOT NULL "
                          "ON CONFLICT(event_id) DO NOTHING "
                          "RETURNING *",
                          (event_id, event_data['user_id'], event_data['device_os'], event_data['country']))
    if opened_cursor.fetchone():
        return 0
    return 2


def session_ping(opened_cursor, event_data, event_id):
    return 0


event_data = {"session_ping": session_ping, "registration": registration, "match": match}


def insert_into_country():
    with open('timezones.jsonl', 'r') as timezones, conn.cursor() as insert_timezones:
        line = timezones.readline()
        while line:
            line = json.loads(line)
            try:
                insert_timezones.execute("INSERT INTO country.Country(country_id, timezone) "
                                         "VALUES (%s, %s) ON CONFLICT (country_id) DO NOTHING RETURNING *",
                                         (line.get('country').upper(), line.get('timezone')))
                conn.commit()
                print(insert_timezones.fetchall())
            except Exception as e:
                print(e)
                conn.rollback()
            line = timezones.readline()


def insert_into_events():
    with open('events.jsonl', 'r') as events, conn.cursor() as insert_events:
        line = events.readline()
        while line:
            line = json.loads(line)
            conn.rollback()
            insert_events.execute("INSERT INTO events.Event(event_id, event_timestamp, event_type_id) "
                                  "VALUES(%s, to_timestamp(%s), (SELECT type_id FROM events.Type WHERE UPPER(type_name)=%s))"
                                  "ON CONFLICT(event_id) DO NOTHING RETURNING *",
                                  (line.get('event_id'), line.get('event_timestamp'), line.get('event_type').upper()))
            if not event_data[line.get('event_type', 'session_ping')](insert_events, line.get('event_data'),
                                                                      line.get('event_id')):
                conn.commit()
            #print(line)
            line = events.readline()


if __name__ == '__main__':
    insert_into_country()
    insert_into_events()
