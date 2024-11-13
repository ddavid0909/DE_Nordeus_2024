CREATE SCHEMA users;
CREATE SCHEMA events;
CREATE SCHEMA country;
CREATE SCHEMA device;

CREATE TABLE users.User (
    user_id BIGSERIAL PRIMARY KEY,
    user_name TEXT UNIQUE NOT NULL
);

CREATE TABLE device.Device (
    device_id BIGSERIAL PRIMARY KEY,
    device_os VARCHAR(15) NOT NULL
);

CREATE TABLE country.Country (
    country_id CHAR(2) PRIMARY KEY,
    timezone VARCHAR(50) NOT NULL
);

CREATE TABLE events.Type (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(20)
);

CREATE TABLE events.Event (
    event_id BIGINT PRIMARY KEY,
    event_timestamp TIMESTAMPTZ,
    event_type_id INT REFERENCES events.Type(type_id)
);

CREATE TABLE events.Registration (
    event_id BIGINT PRIMARY KEY REFERENCES events.Event (event_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users.User (user_id),
    device_id BIGINT NOT NULL REFERENCES device.Device(device_id),
    country_code CHAR(2) NOT NULL REFERENCES country.Country(country_id)
);

CREATE TABLE events.Match (
  match_id TEXT PRIMARY KEY NOT NULL,
  event_id_start BIGINT NOT NULL REFERENCES events.Event (event_id) ON DELETE CASCADE,
  event_id_end BIGINT REFERENCES events.Event(event_id),
  home_user_id BIGINT NOT NULL REFERENCES users.User(user_id),
  away_user_id BIGINT NOT NULL REFERENCES users.User(user_id),
  home_goals_scored INT,
  away_goals_scored INT
);

CREATE TABLE events.Session (
    event_id BIGINT PRIMARY KEY REFERENCES events.Event(event_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users.User(user_id),
    session_user_id BIGINT NOT NULL,
    is_start SMALLINT NOT NULL CHECK(is_start = 1 OR is_start = 0)
);

INSERT INTO events.Type(type_name) VALUES ('registration'), ('session_ping'), ('match');
INSERT INTO device.Device(device_os) VALUES ('ios'), ('android'), ('web');