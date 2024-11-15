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

        start_date = datetime.date(year=2024, month=10, day=7)
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
    with conn.cursor() as last_login_cursor:
        last_login_cursor.execute("SELECT s.user_id, MIN(%s::DATE - e.event_timestamp::DATE) "
                                  "FROM events.session s JOIN events.event e ON s.event_id = e.event_id "
                                  "WHERE s.user_id = %s AND s.is_start = 1 AND e.event_timestamp::DATE <= %s::DATE "
                                  "GROUP BY s.user_id ", (date if date is not None else datetime.datetime.now(),
                                                          user_id, date if date is not None else datetime.datetime.now()))
        last_login_data = last_login_cursor.fetchone()
    if last_login_data is not None:
        last_login_days = last_login_data[1]
        result['days_since_last_login'] = last_login_days
    else:
        result['days_since_last_login'] = "No last login."

    #number_of_sessions
    with conn.cursor() as session_num_cursor:
        query = ("SELECT COALESCE(SUM(s.is_start), 0) FROM events.event e "
                 "JOIN events.session s ON e.event_id = s.event_id "
                 "WHERE s.user_id = %s ")
        if date:
            query += "AND e.event_timestamp::DATE = %s::DATE"
            session_num_cursor.execute(query, (user_id, date))
        else:
            session_num_cursor.execute(query, (user_id, ))
        sessions_number = session_num_cursor.fetchone()[0]
        result['sessions_number'] = sessions_number

    #time spent
    with conn.cursor() as time_spent_cursor:
        if not date:
            time_spent_cursor.execute("SELECT COALESCE(SUM(EXTRACT (EPOCH FROM e2.event_timestamp-e1.event_timestamp)),0) "
                                      "FROM events.event e1 JOIN events.session s1 ON  e1.event_id = s1.event_id "
                                      "JOIN events.session s2 ON s1.session_user_id = s2.session_user_id "
                                      "AND s1.user_id = s2.user_id "
                                      "JOIN events.event e2 ON e2.event_id = s2.event_id "
                                      "WHERE s1.is_start = 1 AND s2.is_start = 0 AND s1.user_id = %s ", (user_id,))
        else:
            time_spent_cursor.execute("SELECT COALESCE(SUM("
                 "CASE "
                 "WHEN e2.event_timestamp::DATE = e1.event_timestamp::DATE AND e2.event_timestamp::DATE = %s::DATE "
                    "THEN EXTRACT (EPOCH FROM e2.event_timestamp-e1.event_timestamp) "
                 "WHEN e2.event_timestamp::DATE = %s::DATE "
                    "THEN EXTRACT (EPOCH FROM e2.event_timestamp-DATE_TRUNC('day', e2.event_timestamp)) "
                 "WHEN e1.event_timestamp::DATE = %s::DATE "
                    "THEN EXTRACT (EPOCH FROM DATE_TRUNC('day', e1.event_timestamp) + INTERVAL '1 day' - e1.event_timestamp)"
                 "ELSE 0 END), 0) "
                 "FROM events.event e1 JOIN events.session s1 ON  e1.event_id = s1.event_id "
                 "JOIN events.session s2 ON s1.session_user_id = s2.session_user_id and s1.user_id = s2.user_id "
                 "JOIN events.event e2 ON e2.event_id = s2.event_id "
                 "WHERE s1.is_start = 1 AND s2.is_start = 0 AND s1.user_id = %s ", (date, date, date, user_id))

        time_spent = time_spent_cursor.fetchone()[0]
        result['time_spent'] = int(time_spent) #the precision should be to seconds.

    # total_won
    with conn.cursor() as total_won_cursor:
        if not date:
            total_won_cursor.execute("SELECT COALESCE(SUM(CASE "
                                     "WHEN home_user_id = %s AND home_goals_scored > away_goals_scored THEN 3 "
                                     "WHEN home_user_id = %s AND home_goals_scored = away_goals_scored THEN 1 "
                                     "ELSE 0 END), 0),"
                                     "COALESCE(SUM(CASE "
                                     "WHEN away_user_id = %s AND away_goals_scored > home_goals_scored THEN 3 "
                                     "WHEN away_user_id = %s AND away_goals_scored = home_goals_scored THEN 1 "
                                     "ELSE 0 END), 0)  "
                                     "FROM events.match WHERE home_user_id = %s OR away_user_id = %s ",
                                     (user_id, user_id, user_id, user_id, user_id, user_id))
        else:
            total_won_cursor.execute("SELECT COALESCE(SUM(CASE "
                                     "WHEN m.home_user_id = %s AND m.home_goals_scored > m.away_goals_scored THEN 3 "
                                     "WHEN m.home_user_id = %s AND m.home_goals_scored = m.away_goals_scored THEN 1 "
                                     "ELSE 0 END), 0),"
                                     "COALESCE(SUM(CASE "
                                     "WHEN m.away_user_id = %s AND m.away_goals_scored > m.home_goals_scored THEN 3 "
                                     "WHEN m.away_user_id = %s AND m.away_goals_scored = m.home_goals_scored THEN 1 "
                                     "ELSE 0 END), 0)  "
                                     "FROM events.match m JOIN events.event e ON m.event_id_start = e.event_id "
                                     "WHERE (m.home_user_id = %s OR m.away_user_id = %s) "
                                     "AND e.event_timestamp::DATE = %s::DATE",
                                     (user_id, user_id, user_id, user_id, user_id, user_id, date))
        home_goal_score, away_goal_score = total_won_cursor.fetchone()
        result['score_home'] = home_goal_score
        result['score_away'] = away_goal_score


    # bonus
    with conn.cursor() as match_time_cursor:
        if not date:
            match_time_cursor.execute("WITH match_time(match_time) AS ( "
                                      "SELECT COALESCE(SUM(EXTRACT (EPOCH FROM e2.event_timestamp-e1.event_timestamp)),0) "
                                      "FROM events.match m JOIN events.event e1 ON m.event_id_start = e1.event_id "
                                      "JOIN events.event e2 ON m.event_id_end = e2.event_id "
                                      "WHERE m.home_user_id = %s OR m.away_user_id = %s "
                                      ")"
                                      "SELECT ((SELECT match_time FROM match_time)*1.0/"
                                      "COALESCE(SUM(EXTRACT (EPOCH FROM e2.event_timestamp-e1.event_timestamp)),1))*100 "
                                      "FROM events.event e1 JOIN events.session s1 ON e1.event_id = s1.event_id "
                                      "JOIN events.session s2 ON s2.session_user_id = s1.session_user_id "
                                      "AND s2.user_id = s1.user_id JOIN events.event e2 ON s2.event_id = e2.event_id "
                                      "WHERE s1.user_id = %s AND s1.is_start = 1 AND s2.is_start = 0",
                                      (user_id, user_id, user_id))
        else:
            match_time_cursor.execute("WITH match_time(match_time) AS ( "
                                      "SELECT COALESCE(SUM("
                                      "CASE WHEN e1.event_timestamp::DATE = e2.event_timestamp::DATE AND e1.event_timestamp::DATE = %s::DATE "
                                            "THEN EXTRACT (EPOCH FROM e2.event_timestamp-e1.event_timestamp)"
                                      "WHEN e2.event_timestamp::DATE = %s::DATE "
                                            "THEN EXTRACT (EPOCH FROM e2.event_timestamp-DATE_TRUNC('day', e2.event_timestamp))"
                                      "WHEN e1.event_timestamp::DATE = %s::DATE "
                                            "THEN EXTRACT (EPOCH FROM DATE_TRUNC('day', e1.event_timestamp)+ INTERVAL '1 day' - e1.event_timestamp) END "
                                      "),0) "
                                      "FROM events.match m JOIN events.event e1 ON m.event_id_start = e1.event_id "
                                      "JOIN events.event e2 ON m.event_id_end = e2.event_id "
                                      "WHERE (m.home_user_id = %s OR m.away_user_id = %s) "
                                      "AND (e1.event_timestamp::DATE = %s::DATE OR e2.event_timestamp::DATE = %s::DATE)"
                                      ")"
                                      "SELECT ((SELECT match_time FROM match_time)*1.0/"
                                      "COALESCE(SUM("
                                      "CASE "
                                      "WHEN e2.event_timestamp::DATE = e1.event_timestamp::DATE AND e2.event_timestamp::DATE = %s::DATE "
                                            "THEN EXTRACT (EPOCH FROM e2.event_timestamp-e1.event_timestamp) "
                                      "WHEN e2.event_timestamp::DATE = %s::DATE "
                                            "THEN EXTRACT (EPOCH FROM e2.event_timestamp-DATE_TRUNC('day', e2.event_timestamp)) "
                                      "WHEN e1.event_timestamp::DATE = %s::DATE "
                                            "THEN EXTRACT (EPOCH FROM DATE_TRUNC('day', e1.event_timestamp) + INTERVAL '1 day' - e1.event_timestamp)"
                                      "ELSE NULL END), 1))*100 "
                                      "FROM events.event e1 JOIN events.session s1 ON  e1.event_id = s1.event_id "
                                      "JOIN events.session s2 ON s1.session_user_id = s2.session_user_id and s1.user_id = s2.user_id "
                                      "JOIN events.event e2 ON e2.event_id = s2.event_id "
                                      "WHERE s1.is_start = 1 AND s2.is_start = 0 AND s1.user_id = %s ",
                                      (date, date, date, user_id, user_id, date, date, date, date, date, user_id))
        result['active_time_played_percentage'] = match_time_cursor.fetchone()[0]

    return jsonify(result), 200


@app.route("/game_stats", methods=['GET'])
def get_game_stats():
    date = None
    if 'date' in request.json:
        try:
            date = datetime.date.fromisoformat(request.json['date'])
        except ValueError:
            return "Invalid date format, try YYYY-MM-DD", 400

        start_date = datetime.date(year=2024, month=10, day=7)
        end_date = datetime.date(year=2024, month=11, day=3)
        if not (start_date <= date <= end_date):
            return f"Date must be between bounds {start_date} and {end_date} inclusive", 400

    result = {}

    #dau
    with conn.cursor() as dau_cursor:
        query = ("SELECT COALESCE(COUNT(DISTINCT(s1.user_id)),0) "
                 "FROM events.event e1 JOIN events.session s1 ON  e1.event_id = s1.event_id "
                 "JOIN events.session s2 ON s1.session_user_id = s2.session_user_id and s1.user_id = s2.user_id "
                 "JOIN events.event e2 ON e2.event_id = s2.event_id "
                 "WHERE s1.is_start = 1 AND s2.is_start = 0 ")
        if date:
            query = query + " AND %s::DATE BETWEEN e1.event_timestamp::DATE AND e2.event_timestamp::DATE "
            dau_cursor.execute(query, (date, ))
        else:
            dau_cursor.execute(query)

        dau = dau_cursor.fetchone()[0]
        result['dau'] = dau

    #number of sessions
    with conn.cursor() as number_sessions_cursor:
        query = "SELECT COALESCE(SUM(s1.is_start), 0) FROM events.session s1 "
        if date:
            query = query + (" JOIN events.event e1 ON s1.event_id = e1.event_id "
                             " JOIN events.session s2 ON s1.session_user_id = s2.session_user_id AND "
                             "s1.user_id = s2.user_id JOIN events.event e2 ON s2.event_id = e2.event_id "
                             "WHERE s1.is_start = 1 AND s2.is_start = 0 "
                             "AND %s::DATE BETWEEN e1.event_timestamp::DATE AND e2.event_timestamp::DATE")
            number_sessions_cursor.execute(query, (date,))
        else:
            number_sessions_cursor.execute(query)
        result['number_of_sessions'] = number_sessions_cursor.fetchone()[0]

    # average number of sessions per user
    with (conn.cursor() as average_cursor):
        if not date:
            average_cursor.execute("WITH sessions_per_user(user_id, session_num) AS (SELECT user_id, SUM(is_start) FROM events.session GROUP BY user_id)"
                 "SELECT COALESCE(AVG(session_num),0) FROM sessions_per_user")
        else:
            average_cursor.execute(
                "WITH sessions_per_user(user_id, session_num) "
                "AS (SELECT s1.user_id, SUM(s1.is_start) "
                "FROM events.session s1 JOIN events.event e1 "
                "ON s1.event_id = e1.event_id JOIN events.session s2 ON s1.session_user_id = s2.session_user_id "
                "AND s1.user_id = s2.user_id JOIN events.event e2 ON e2.event_id = s2.event_id "
                "WHERE s1.is_start = 1 AND s2.is_start = 0 AND %s::DATE BETWEEN e1.event_timestamp::DATE AND e2.event_timestamp::DATE "
                "GROUP BY s1.user_id) "
                "SELECT COALESCE(AVG(session_num),0) FROM sessions_per_user", (date,))
        result['average_sessions_number'] = average_cursor.fetchone()[0]

    # user, users with most points afterall
    with conn.cursor() as most_points_cursor:
        if not date:
            most_points_cursor.execute("WITH user_points(user_id, points) AS ("
                                        "SELECT user_id, COALESCE(SUM(points),0)"
                                        "FROM"
                                        "(SELECT home_user_id as user_id, CASE "
                                        "WHEN home_goals_scored > away_goals_scored THEN 3 "
                                        "WHEN home_goals_scored = away_goals_scored THEN 1 "
                                        "ELSE 0 END as points "
                                        "FROM events.match "
                                       
                                        "UNION ALL "
                                       
                                        "SELECT away_user_id, CASE "
                                        "WHEN away_goals_scored > home_goals_scored THEN 3 "
                                        "WHEN away_goals_scored = home_goals_scored THEN 1 "
                                        "ELSE 0 END  "
                                        "FROM events.match"
                                       
                                         ") scores "
                                        "GROUP BY user_id"
                                       ")"
                                       "SELECT u.user_name "
                                       "FROM user_points up JOIN users.user u ON u.user_id = up.user_id "
                                       "WHERE points = (SELECT MAX(points) FROM user_points)"
                                       "")
            result['max_points_users'] = []
            for row in most_points_cursor.fetchall():
                result['max_points_users'].append(row[0])

    return jsonify(result), 200

if __name__ == '__main__':
    app.run(host= app.config.get('HOST'))