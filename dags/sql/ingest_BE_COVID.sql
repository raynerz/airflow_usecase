INSERT INTO dim_deaths VALUES
    (DEFAULT,
    {{ task_instance.xcom_pull(task_ids='process_BE_cases', key='deaths')}},
    '{{ task_instance.xcom_pull(task_ids='process_BE_cases', key='canton')}}',
    '{{ task_instance.xcom_pull(task_ids='process_BE_cases', key='date')}}'
  );

INSERT INTO dim_infections VALUES
    (DEFAULT,
    {{ task_instance.xcom_pull(task_ids='process_BE_cases', key='infections')}},
    '{{ task_instance.xcom_pull(task_ids='process_BE_cases', key='canton')}}',
    '{{ task_instance.xcom_pull(task_ids='process_BE_cases', key='date')}}'
  );
