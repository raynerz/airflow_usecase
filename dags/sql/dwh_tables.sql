DROP TABLE IF EXISTS fact_covid;
DROP TABLE IF EXISTS dim_test;
DROP TABLE IF EXISTS dim_deaths;
DROP TABLE IF EXISTS dim_municipality;
DROP TABLE IF EXISTS dim_canton;
DROP TABLE IF EXISTS dim_infections;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_test (
  test_key    SERIAL PRIMARY KEY,
  positive    INTEGER,
  negative    INTEGER,
  total       INTEGER,
  positivity_rate INTEGER,
  week_number     INTEGER,
  source          TEXT

);

CREATE TABLE dim_deaths (
  deaths_key SERIAL PRIMARY KEY,
  amount INTEGER,
  canton INTEGER
);

CREATE TABLE dim_municipality(
  postal_code_key INTEGER PRIMARY KEY,
  place VARCHAR(50)
);

CREATE TABLE dim_canton(
  canton_key SERIAL PRIMARY KEY,
  place VARCHAR(50)
);

CREATE TABLE dim_infections (
  infection_key SERIAL PRIMARY KEY,
  confirmed INTEGER,
  postal_code INTEGER,
  canton INTEGER
);

CREATE TABLE dim_date (
  date_key SERIAL PRIMARY KEY,
  day INTEGER,
  month INTEGER,
  year INTEGER,
  hour INTEGER,
  minute INTEGER,
  second INTEGER
);

CREATE TABLE fact_covid (
    covid_key     SERIAL PRIMARY KEY,
    dim_test      INTEGER,
    dim_deaths    INTEGER,
    dim_municipality  INTEGER,
    dim_canton    INTEGER,
    dim_infections INTEGER,
    dim_date      INTEGER,
    created_date TIMESTAMP NOT NULL DEFAULT current_timestamp,

    CONSTRAINT fk_test
      FOREIGN KEY(dim_test)
	  REFERENCES dim_test(test_key),

    CONSTRAINT fk_deaths
      FOREIGN KEY(dim_deaths)
    REFERENCES dim_deaths(deaths_key),

    CONSTRAINT fk_municipality
      FOREIGN KEY(dim_municipality)
	  REFERENCES dim_municipality(postal_code_key),

    CONSTRAINT fk_canton
      FOREIGN KEY(dim_canton)
	  REFERENCES dim_canton(canton_key),

    CONSTRAINT fk_infections
      FOREIGN KEY(dim_infections)
	  REFERENCES dim_infections(infection_key),

    CONSTRAINT fk_date
      FOREIGN KEY(dim_date)
    REFERENCES dim_date(date_key)

);
