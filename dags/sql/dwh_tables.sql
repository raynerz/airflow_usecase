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
  confirmed REAL,
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
