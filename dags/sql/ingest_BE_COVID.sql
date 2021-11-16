DO $$

DECLARE dim_test INTEGER;
DECLARE dim_deaths INTEGER;
DECLARE dim_infections INTEGER;

BEGIN

  INSERT INTO dim_deaths VALUES
      (DEFAULT,
      {{ task_instance.xcom_pull(task_ids='process_BE_cases', key='deaths')}},
      '{{ task_instance.xcom_pull(task_ids='process_BE_cases', key='canton')}}',
      '{{ task_instance.xcom_pull(task_ids='process_BE_cases', key='date')}}'
    ) RETURNING "deaths_key" INTO dim_deaths;

  INSERT INTO dim_infections VALUES
      (DEFAULT,
      {{ task_instance.xcom_pull(task_ids='process_BE_cases', key='infections')}},
      '{{ task_instance.xcom_pull(task_ids='process_BE_cases', key='canton')}}',
      '{{ task_instance.xcom_pull(task_ids='process_BE_cases', key='date')}}'
    ) RETURNING "infection_key" INTO dim_infections;

  INSERT INTO dim_test VALUES
      (DEFAULT,
      {{ task_instance.xcom_pull(task_ids='process_BE_tests', key='positive')}},
      '{{ task_instance.xcom_pull(task_ids='process_BE_tests', key='negative')}}',
      {{ task_instance.xcom_pull(task_ids='process_BE_tests', key='total')}},
      {{ task_instance.xcom_pull(task_ids='process_BE_tests', key='positivity_rate')}},
      '{{ task_instance.xcom_pull(task_ids='process_BE_tests', key='source')}}',
      {{ task_instance.xcom_pull(task_ids='process_BE_tests', key='date')}}
    ) RETURNING "test_key" INTO dim_test;

  INSERT INTO fact_covid VALUES
      (DEFAULT,
      dim_test,
      dim_deaths,
      dim_infections,
      DEFAULT
    );
  END $$;
