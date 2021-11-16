-- Covid Star

DROP TABLE IF EXISTS fact_covid;
DROP TABLE IF EXISTS dim_test;
DROP TABLE IF EXISTS dim_deaths;
DROP TABLE IF EXISTS dim_infections;

CREATE TABLE dim_test (
  test_key    SERIAL PRIMARY KEY,
  positive    INTEGER,
  negative    VARCHAR(50),
  total       INTEGER,
  positivity_rate FLOAT,
  source          TEXT,
  created_date VARCHAR(50)
);

CREATE TABLE dim_deaths (
  deaths_key SERIAL PRIMARY KEY,
  amount INTEGER,
  canton VARCHAR(50),
  created_date VARCHAR(50)
);

CREATE TABLE dim_infections (
  infection_key SERIAL PRIMARY KEY,
  confirmed INTEGER,
  canton VARCHAR(50),
  created_date VARCHAR(50)
);


CREATE TABLE fact_covid (
    covid_key     SERIAL PRIMARY KEY,
    dim_test      INTEGER,
    dim_deaths    INTEGER,
    dim_infections INTEGER,
    created_date TIMESTAMP NOT NULL DEFAULT current_timestamp,

    CONSTRAINT fk_test
      FOREIGN KEY(dim_test)
	  REFERENCES dim_test(test_key),

    CONSTRAINT fk_deaths
      FOREIGN KEY(dim_deaths)
    REFERENCES dim_deaths(deaths_key),

    CONSTRAINT fk_infections
      FOREIGN KEY(dim_infections)
	  REFERENCES dim_infections(infection_key)
);


-- Traffic Star

DROP TABLE IF EXISTS dim_travel_time;
DROP TABLE IF EXISTS dim_speed;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS fact_traffic;

CREATE TABLE dim_travel_time (
  travel_key SERIAL PRIMARY KEY,
  current_travel_time FLOAT,
  free_flow_travel_time FLOAT
);


CREATE TABLE dim_speed (
  speed_key SERIAL PRIMARY KEY,
  current_speed FLOAT,
  free_flow_speed FLOAT
);

CREATE TABLE dim_location (
  loc_key SERIAL PRIMARY KEY,
  city VARCHAR(50),
  canton VARCHAR(50),
  latitude FLOAT,
  longitude FLOAT
);

CREATE TABLE fact_traffic (
  traffic_key SERIAL PRIMARY KEY,
  dim_speed INTEGER,
  dim_location INTEGER,
  dim_travel_time INTEGER,
  created_date TIMESTAMP NOT NULL DEFAULT current_timestamp,

  CONSTRAINT fk_speed
    FOREIGN KEY(dim_speed)
  REFERENCES dim_speed(speed_key),

  CONSTRAINT fk_location
    FOREIGN KEY(dim_location)
  REFERENCES dim_location(loc_key),

  CONSTRAINT fk_travel_time
    FOREIGN KEY(dim_travel_time)
  REFERENCES dim_travel_time(travel_key)
);
