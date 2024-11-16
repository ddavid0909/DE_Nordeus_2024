# Requirements
In order to run the code, you will need the following three:
- Docker
- Python
- Postman (for testing the API)

## API 
The API to access the persisted data contains only two get requests:
- /game_stats
- /user_stats

See deployment below for explanations how to run the app.

The server should run at localhost:5000, so your get requests should be aiming for addresses:
http://localhost:5000/game_stats and http://localhost:5000/user_stats.
I used Postman to test the API, mainly because I opted for JSON as both input and output data format. 

### game_stats
Pass JSON with optional key date and appropriate value. The value must be in the required format of YYYY-MM-DD and it must be a date between 2024-10-07 and 2024-11-03 inclusive.
Any other dates will be refused. You will get a JSON output with keys:

    "average_sessions_number": average sessions of all users that played that day

    "dau": daily active users

    "max_points_users": array of all users that had the most points

    "number_of_sessions": number of sessions

### user_stats
Pass JSON with optional key date and required key user. The date must be in the required format of YYYY-MM-DD and it must be a date between 2024-10-07 and 2024-11-03 inclusive.
Any other dates will be refused. You will get a JSON output with keys:

    "active_time_played_percentage": This is the percentage of time the user actively played. Adapted to calculate only for specific date in case of the additional parameter.
    
    "country_id": Passes the two-letter code that was contained in timezones.
    
    "country_timezone": Timezone as presented. No checks on what was inserted as timezones, but may be added in the future.
    
    "days_since_last_login": Calculates number of days since last login
    
    "score_away": Scores the given user made as away_user

    "score_home": Scores the given user made as home_user

    "sessions_number": Number of sessions this user had.
    
    "time_spent": Time spent in game (in seconds)
    
    "timestamp_local": Local time of registration.

## Deployment
You need to get the postgres image first: 
>docker pull postgres

Use commands below to create the image of the api and run.

> docker image build -f api.dockerfile -t api .

After that, you must start the containers specified in the yaml file.
IDEs like PyCharm let you just click on the double play sign next to the "services" line, or
you can run a command:
> docker-compose -f deployment.yaml up

Bear in mind that the database still leaves open connection to the outside world (port:5432) which means **you will need to
free up that port**. 

This is needed because you will still need to run [data_collection.py](data_collection.py) separately in order to fill the database. 
In case you want to change this, you will need to change outer port (before the :) for the service database in [deployment.yaml](deployment.yaml), and 
connection for the cursor in [data_collection.py](data_collection.py).

> pip install -r requirements.txt
> 
> python3 data_collection.py *timezones_json* *events_json*

You will not need to run [api.py](api.py), since it will be automatically run with docker-compose.
Be careful to write the path relative to the running script [data_collection.py](data_collection.py)

# Project description

This project is separated in three parts:
1. Database schema projecting
2. Data cleaning and insertion
3. API creation for easy retrieval

## Data storage
Postgres database is used for storing the data.
File [deployment.yaml](deployment.yaml) can be used to create a database image.

The database script is given in [init.sql](database/init.sql). This script creates tables
Event, Match, Registration, Type, Country, Device and User.

It fills Type and Device tables with predefined values.
Those two tables are very small, but are used to normalize the database and make sure that 
no duplicates or wrong values are inserted for type and device. **In case of new type of event or device, 
insertion in these tables is necessary**.
This script will run instantly when the postgres container is created, 
but take care if you modify the script that any error in the init script will require you to delete and recreate the containers entirely.
Adminer is also available in order to allow direct queries to the database.
Bear in mind that **port 5432 must be free**. This port is taken up by real Postgres (5432).
In case you need to run real postgres simultaneously, simply modify the first port number (before the : in the yaml, ports section of the service) to 
a free port.

**If you use [development.yaml](development.yaml) and login to adminer, you will not see the tables in the public schema. 
They are separated to different schemas, as you can check in the [script](database/init.sql).**

### Database structure discussion
1. events schema
The first schema for events contains five tables: event, type, registration, session_ping and match
- event
This table contains all events and data they share: type of event, event timestamp and event id. The main reason I 
included this table is to quickly check for duplicate event ids as it is the primary key. The downside to this approach
is visible in the API code as many joins are required.
- type
I created a separate table for types to normalize the data. In case new types of events appear for analysis in the future,
it will be easy to add them.
- registration
This table contains registration data, as per the problem statement.
- session_ping
This table contains the session data. Bear in mind that it is extended with two new fields - session_user_id 
and is_start. They are used in the algorithm to dynamically save only the first session ping and the last arrived up to
the given moment for a given user. is_start is added to simplify future queries. This could have been accomplished
the way it is done in the match table - by connecting a session_ping with two event_ids directly.
You will also find a recursive query that cleans the session data AFTER data collection, which is admittedly worse than
DURING data collection. Due to efficiency, I gave up on using that function.
- match
Match table is connected to two events - match start and match end. Everything else is similar to the problem statement.
This way of storage turned out to be easier for querying than what is done in session_ping.
2. users schema
This schema contains users. I once again added user_id a surrogate key simply to make checks on primary keys faster, 
although that has also resulted in more difficult queries later on. This effect was reduced by retrieving the user_id 
once and using it in future queries. (visible in get_user_stats)

## Data collection and cleaning
The data from the file is processed and inserted into the database, as is required by the challenge.
I used psycopg2 for connection to the database.
The downside is the difficulty of changing the database management system, since some queries may be PostgreSQL-specific.
Module [data_collection.py](data_collection.py) contains functions that read and clean the data from files 
[timezones.jsonl](timezones.jsonl) and [events.jsonl](events.jsonl). (You can specify different file paths as needed. See [Deployment](#Deployment))


### Data cleaning rules
All validity checks that can be performed outside the database should be performed before transaction commencement. 
This mostly regards the timestamp validity check.
1. Event_id field is unique. Any duplicates are **discarded**, without update. The first value is considered to be correct, the others an error.
2. Event_date must be within the limits of (07. Oct. - 03. Nov.) of 2024. The timestamp is saved in postgres as GMT (UTC timezone), converted to timestamp instead of int.
3. The user that is being registered must not already exist in the system. The first value is considered to be correct, the others an error.
4. The user cannot play a match with himself.
5. Same users must be in match_start and match_end, and errors here are not subject to correction. In case another match_end does not appear, the match will be removed.
6. Users playing must be registered. This is guaranteed by database referential integrity.
7. Tolerance to case errors is enforced, by checking for equality after either UPPER or LOWER functions performed on strings.

### Data cleaning during collection
Data cleaning is mostly done during collection. Referential integrity ensures majority of constraints are satisfied. 
The first session ping is always recorded, and along with it only the last corresponding session ping.
**It is required for session pings to be exactly 60 seconds apart as I assumed the ping is generated on the server so there is no network latency.**
You can change "= 60" to "<= 60", but you may have to do it in multiple places.

### Data cleaning after collection
Useless data about matches that did not end and sessions that did not end is cleaned from the system after data collection.
You can see two functions in the [data_collection.py](data_collection.py). 
These functions delete redundant rows from matches and sessions, while vacuum function will free up the physical space.
I opted for conservative policy and did not delete users that only registered and never participated.