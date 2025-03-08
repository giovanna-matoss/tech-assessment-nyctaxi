up-streaming:
	docker-compose -f apps/flask/docker-compose.flask.yaml up -d

down-streaming:
	docker-compose -f apps/flask/docker-compose.flask.yaml down

up-processing:
	docker-compose -f apps/airflow/docker-compose.airflow.yaml -f apps/spark/docker-compose.spark.yaml up -d

down-processing:
	docker-compose -f apps/airflow/docker-compose.airflow.yaml -f apps/spark/docker-compose.spark.yaml down

up-storage:
	docker-compose --env-file .env -f apps/minio/docker-compose.minio.yaml up -d

down-storage:
	docker-compose --env-file .env -f apps/minio/docker-compose.minio.yaml down

up-infra:
	docker-compose -f apps/flask/docker-compose.flask.yaml -f apps/spark/docker-compose.spark.yaml -f apps/airflow/docker-compose.airflow.yaml -f apps/minio/docker-compose.minio.yaml up -d

down-infra:
	docker-compose -f apps/flask/docker-compose.flask.yaml -f apps/spark/docker-compose.spark.yaml -f apps/airflow/docker-compose.airflow.yaml -f apps/minio/docker-compose.minio.yaml down