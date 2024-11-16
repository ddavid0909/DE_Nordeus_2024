import datetime
import os
import sys
import time

import psycopg2
import json

conn = psycopg2.connect(
    dbname=os.environ.get('DATABASE_NAME', default='events_db'),
    user=os.environ.get('DATABASE_USER', default='postgres'),
    password=os.environ.get('DATABASE_PASSWORD', default='postgres_password'),
    host=os.environ.get('DATABASE_HOST', default='localhost'),
    port=os.environ.get('DATABASE_PORT', default='5432')
)

def match(opened_cursor, event_data, event_id, event_timestamp):
    # missing values check
    if ('match_id' not in event_data
            or 'home_user_id' not in event_data
            or 'away_user_id' not in event_data
            or 'home_goals_scored' not in event_data
            or 'away_goals_scored' not in event_data):
        print(f"Values missing from event data upon attempt to insert match {event_id}")
        return 1
    # users must be different
    if event_data['home_user_id'] == event_data['away_user_id']:
        print(f"Match of the user against himself {event_id}")
        return 2
    # either no goals are set or both goals are set
    if event_data['home_goals_scored'] is not None or event_data['away_goals_scored'] is not None:
        if event_data['home_goals_scored'] is None or event_data['away_goals_scored'] is None:
            print(f"Either no goals should be set or both goals should be set {event_id}")
            return 3
        # update the existing match
        opened_cursor.execute("SELECT home.user_name, away.user_name, e.event_timestamp::TIMESTAMP, m.event_id_end "
                              "FROM events.match m  JOIN users.user home ON m.home_user_id = home.user_id "
                              "JOIN users.user away ON m.away_user_id = away.user_id "
                              "JOIN events.event e ON e.event_id = m.event_id_start "
                              "WHERE m.match_id = %s", (event_data['match_id'],))
        match_data = opened_cursor.fetchone()
        # match end without start
        if not match_data:
            print(f"Match end without match start. Event id:{event_id}, Match id:{event_data['match_id']}")
            return 4
        match_start_home_user = match_data[0]
        match_start_away_user = match_data[1]
        match_start_timestamp = match_data[2]
        match_start_end_event_id = match_data[3]

        #match already ended.
        if match_start_end_event_id is not None:
            print(f"Match already over. {event_id} is therefore invalid for match id:{event_data['match_id']}")
            return 5
        #different users in the same match. Error. Right now, the policy is to refuse the row. May change if required.
        if match_start_home_user != event_data['home_user_id'] or match_start_away_user != event_data['away_user_id']:
            print(f"Users in match end differ from users in match start. "
                  f"Event id:{event_id}, Match id:{event_data['match_id']} refused.")
            return 6
        #end before start. Mistake.
        if datetime.datetime.fromtimestamp(event_timestamp) < match_start_timestamp:
            print(f"End time is before start time. Event id:{event_id}, Match id:{event_data['match_id']}")
            return 7
        # the match has started, and this is its ending
        opened_cursor.execute("UPDATE events.match "
                              "SET event_id_end=%s, home_goals_scored=%s, away_goals_scored=%s "
                              "WHERE match_id=%s "
                              "RETURNING * ",
                              (event_id, event_data['home_goals_scored'],
                               event_data['away_goals_scored'], event_data['match_id']))
        if not opened_cursor.fetchone():
            print(f"Fatal error for event_id {event_id}. Match_id:{event_data['match_id']}.")
            return 8
        return 0

    opened_cursor.execute(
        "INSERT INTO events.match(event_id_start, match_id, home_user_id, away_user_id)"
        "SELECT event_id, match_id, home_user, away_user "
        "FROM ("
        "SELECT %s as event_id, %s as match_id, "
        "(SELECT(user_id) FROM users.user WHERE user_name=%s) as home_user, "
        "(SELECT(user_id) FROM users.user WHERE user_name=%s) as away_user ) "
        "WHERE home_user IS NOT NULL AND away_user IS NOT NULL AND match_id IS NOT NULL "
        "ON CONFLICT(match_id) DO NOTHING "
        "RETURNING *", (event_id, event_data['match_id'], event_data['home_user_id'], event_data['away_user_id']))

    #this is not the first insertion, although it should be.
    if not opened_cursor.fetchone():
        print(f"The data may have non-existent home_user, "
              f"away_user, match_id, or be the duplicate match_id insertion attempt. "
              f"Event {event_id} match {event_data['match_id']}")
        return 9
    return 0

'''        
def match(opened_cursor, event_data, event_id, event_timestamp):
    #missing values check
    if ('match_id' not in event_data
            or 'home_user_id' not in event_data
            or 'away_user_id' not in event_data
            or 'home_goals_scored' not in event_data
            or 'away_goals_scored' not in event_data):
        return 1
    #users must be different
    if event_data['home_user_id'] == event_data['away_user_id']:
        return 2
    # either no goals are set or both goals are set
    match_start = True
    if event_data['home_goals_scored'] or event_data['away_goals_scored']:
        if not (event_data['home_goals_scored'] and event_data['away_goals_scored']):
            return 3
        match_start = False
    #for a given match, find the rows inserted
    opened_cursor.execute("SELECT m.home_goals_scored, m.away_goals_scored, e.timestamp "
                          "FROM events.match m JOIN events.event e ON m.event_id = e.event_id "
                          "WHERE m.match_id = %s", (event_data['match_id'],))
    # fetch at least one
    retrieved_values = opened_cursor.fetchone()
    # forbid ending a match that has not started
    if not retrieved_values and not match_start:
        return 4
    # forbid starting a match that has already started
    if retrieved_values and match_start:
        return 5
    # timestamps are not good. start cannot happen before end.
    # shall just ignore values for now
    # may solve by inverting the values
    if retrieved_values and event_timestamp < retrieved_values[2]:
        return 6

    # there are already two rows, which means this match has started and ended already.
    if opened_cursor.fetchone():
        return 7

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
    #insertion was attempted but one or both users do not exist.
    return 7
'''

def registration(opened_cursor, event_data, event_id, event_timestamp):
    # refuse incomplete data
    if 'user_id' not in event_data or 'country' not in event_data or 'device_os' not in event_data:
        print(f"Values missing from event data upon attempt to insert registration {event_id}")
        return 1
    # attempt adding new user with this registration
    opened_cursor.execute("INSERT INTO users.user(user_name) VALUES (%s) ON CONFLICT(user_name) DO NOTHING RETURNING *",
                          (event_data['user_id'],))
    # if insertion failed, rollback the entire insertion
    if not opened_cursor.fetchone():
        print(f"User {event_data['user_id']} is already registered. Event {event_id}")
        return 2
    # insert into registration.
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
    print(f"Registration failed for event {event_id}. Check that device {event_data['device_os']} "
          f"and country {event_data['country']} exist. Conflict may have occurred.")
    return 3

'''
def session_ping(opened_cursor, event_data, event_id, event_timestamp):
    
    if 'user_id' not in event_data:
        print(f"Values missing from event data upon attempt to insert session {event_id}")
        return 1
    opened_cursor.execute("INSERT INTO events.session(event_id, user_id) "
                          "SELECT %s, u.user_id "
                          "FROM users.user u "
                          "WHERE u.user_name = %s and u.user_name IS NOT NULL "
                          "ON CONFLICT (event_id) DO NOTHING RETURNING *", (event_id, event_data['user_id']))
    if opened_cursor.fetchone():
        return 0
    print(f"Insertion failed for event {event_id}. Check that user {event_data['user_id']} exists "
          f"and that event_id is not duplicate.")
    return 2
'''

def session_ping(opened_cursor, event_data, event_id, event_timestamp):
    #missing data
    if 'user_id' not in event_data:
        print(f"Missing user_id in event_data for session_ping with event_id {event_id}")
        return 1
    #check if belongs to started session
    opened_cursor.execute("SELECT s.session_user_id "
                          "FROM events.session s JOIN events.event e ON s.event_id = e.event_id "
                          "JOIN users.user u ON u.user_id = s.user_id "
                          "WHERE u.user_name = %s AND e.event_timestamp + INTERVAL '1 minute' = %s ",
                          (event_data['user_id'], datetime.datetime
                           .fromtimestamp(event_timestamp).astimezone(datetime.timezone.utc)))
    session_data = opened_cursor.fetchone()
    #if session_user_id is null, this is the start of new session. must insert new row
    if session_data is None:
        opened_cursor.execute("INSERT INTO events.session (event_id, user_id, session_user_id, is_start) "
                              "SELECT %s, u.user_id, COALESCE(MAX(session_user_id)+1, 1), 1 "
                              "FROM users.user u LEFT JOIN events.session s ON u.user_id = s.user_id "
                              "WHERE u.user_name = %s "
                              "GROUP BY u.user_id "
                              "ON CONFLICT DO NOTHING "
                              "RETURNING *", (event_id, event_data['user_id']))
        if not opened_cursor.fetchone():
            print(f"User {event_data['user_id']} is most likely mnot registered and therefore cannot have a session")
            return 2
        return 0
    session_user_id = session_data[0]
    opened_cursor.execute("SELECT u.user_id, ARRAY_AGG(e.event_id ORDER BY e.event_timestamp) "
                          "FROM events.session s JOIN events.event e ON s.event_id = e.event_id "
                          "JOIN users.user u ON u.user_id = s.user_id "
                          "WHERE u.user_name = %s AND s.session_user_id = %s "
                          "GROUP BY u.user_id ", (event_data['user_id'], session_user_id))
    agg_data = opened_cursor.fetchone()
    user_id = agg_data[0]
    last_sessions = agg_data[1]
    #print(last_sessions)
    if len(last_sessions) > 1:
        opened_cursor.execute("DELETE FROM events.event WHERE event.event_id = %s ", (last_sessions[-1],))

    opened_cursor.execute("INSERT INTO events.session(event_id, user_id, session_user_id, is_start) "
                          "VALUES(%s, %s, %s, 0) "
                          "ON CONFLICT DO NOTHING RETURNING *", (event_id, user_id, session_user_id))
    if not opened_cursor.fetchone():
        print(f"Fatal error. Something went wrong with insertion. Check duplicate event_id {event_id}")
        return 3
    return 0

def empty_func(opened_cursor, event_data, event_id, event_timestamp):
    return 0


event_data_functions = {"session_ping": session_ping, "registration": registration, "match": match, "none": empty_func}


def insert_into_country(filename):
    with open(filename, 'r') as timezones, conn.cursor() as insert_timezones:
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


def insert_into_events(filename):
    date_start = datetime.datetime(year=2024, month=10, day=7, hour=0, minute=0, second=0, microsecond=0)
    date_end = datetime.datetime(year=2024, month=11, day=3, hour=0, minute=0, second=0, microsecond=0)

    unix_date_start = time.mktime(date_start.timetuple())
    unix_date_end = time.mktime(date_end.timetuple())

    with open(filename, 'r') as events, conn.cursor() as insert_events:
        line = events.readline()
        while line:
            line = json.loads(line)
            # refuse incomplete data
            if 'event_id' not in line or 'event_data' not in line or 'event_timestamp' not in line or 'event_type' not in line:
                continue
            # database-independent check should be done before accessing the database
            if not (unix_date_start <= line['event_timestamp'] <= unix_date_end):
                line = events.readline()
                continue
            # clean everything that may have been added to the connection but not committed
            conn.rollback()
            # attempt to insert a new event. upon failure no rows shall be returned and the transaction will be rolled back
            insert_events.execute("INSERT INTO events.Event(event_id, event_timestamp, event_type_id) "
                                  "VALUES(%s, to_timestamp(%s), (SELECT type_id FROM events.Type WHERE UPPER(type_name)=%s))"
                                  "ON CONFLICT(event_id) DO NOTHING RETURNING *",
                                  (line.get('event_id'), line.get('event_timestamp'), line.get('event_type').upper()))
            if insert_events.fetchone():
                # use of Strategy design pattern to define multiple ways this insertion can go
                # see dictionary event_data_functions, it contains necessary callable objects.
                if event_data_functions[line.get('event_type', 'none').lower()](insert_events, line.get('event_data'),
                                                                            line.get('event_id'), line.get('event_timestamp')) == 0:
                    # only if complete insertion works, the transaction is committed.
                    conn.commit()
            # print(line)
            else:
                print(f"Event {line.get('event_id')} is not inserted. It is likely duplicate or of non-existent type {line.get('type')}")
            line = events.readline()

#data cleansing after data collection
def delete_bad_matches():
    with conn.cursor() as delete_cursor:
        delete_cursor.execute("DELETE FROM events.event WHERE event_id IN (SELECT event_id_start FROM events.match WHERE event_id_end IS NULL)")
        conn.commit()

'''
def delete_unnecessary_session_pings(free_memory=True):
    query = """
    WITH RECURSIVE solution AS (
        SELECT e.event_id AS event_id, e.event_timestamp AS row_timestamp, e.event_timestamp AS session_timestamp, s.user_id
        FROM events.event e 
        JOIN events.session s ON e.event_id = s.event_id
        WHERE NOT EXISTS (
            SELECT * 
            FROM events.session s1 
            JOIN events.event e1 ON s1.event_id = e1.event_id
            WHERE s1.user_id = s.user_id 
            AND e.event_timestamp - e1.event_timestamp = INTERVAL '1 minute'
        )

        UNION ALL

        SELECT e.event_id, e.event_timestamp, sol.session_timestamp, s.user_id
        FROM events.event e 
        JOIN events.session s ON e.event_id = s.event_id
        JOIN solution sol ON sol.row_timestamp + INTERVAL '1 minute' = e.event_timestamp 
        AND sol.user_id = s.user_id
    ), session_starts_ends AS (
        SELECT user_id, session_timestamp, MIN(row_timestamp) AS edge_time
        FROM solution
        GROUP BY user_id, session_timestamp

        UNION ALL

        SELECT user_id, session_timestamp, MAX(row_timestamp)
        FROM solution
        GROUP BY user_id, session_timestamp
    ), valid_pings AS (
        SELECT e.event_id
        FROM events.session s 
        JOIN events.event e ON s.event_id = e.event_id
        WHERE (s.user_id, e.event_timestamp) IN (SELECT user_id, edge_time FROM session_starts_ends)
    )

    DELETE FROM events.event 
    WHERE event_id NOT IN (SELECT event_id FROM valid_pings) AND event_type_id = (SELECT type_id FROM events.type WHERE type_name='session_ping')
    RETURNING *
    """
    with conn.cursor() as delete_cursor:
        delete_cursor.execute(query)
    conn.commit()
    if free_memory:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("VACUUM FULL events.event")
            cursor.execute("VACUUM FULL events.match")
        conn.autocommit = False
'''

def delete_sessions_with_no_valid_end():
    with conn.cursor() as delete_cursor:
        delete_cursor.execute("DELETE FROM events.event "
                              "WHERE event_id IN "
                              "(SELECT event_id FROM events.session "
                              "WHERE (session_user_id, user_id) IN "
                              "(SELECT s1.session_user_id, s1.user_id "
                              "FROM events.session s1 JOIN events.session s2 "
                              "ON s1.session_user_id = s2.session_user_id "
                              "AND s1.user_id = s2.user_id AND s1.is_start = s2.is_start "
                              "GROUP BY s1.session_user_id, s1.user_id  "
                              "HAVING COUNT(*) != 2))"
                              )
        conn.commit()

def vacuum():
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute("VACUUM FULL events.event")
        cursor.execute("VACUUM FULL events.match")
        cursor.execute("VACUUM FULL events.session")
    conn.autocommit = False

if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise Exception('Missing command line parameters: file towards timezones and file towards events')
    timezones = sys.argv[1]
    events = sys.argv[2]
    insert_into_country(timezones)
    print("Country insertion successful")
    insert_into_events(events)
    print("Event insertion successful")
    delete_sessions_with_no_valid_end()
    print("Session deletion successful")
    delete_bad_matches()
    print("Match deletion is successful")
    vacuum()
    conn.close()