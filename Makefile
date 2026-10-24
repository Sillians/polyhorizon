# local setup
up:
	docker compose up -d --build --scale spark-worker=3
	docker login && docker compose up -d --build --scale spark-worker=3
down:
	docker compose down 

logs:
	docker compose logs -f spark-master

down-volume:
	docker compose down -v

remove-spark-custom-image:
	docker image rm spark-custom:3.5.7

	docker compose build spark-master spark-worker
	docker compose up -d --scale spark-worker=3


# Prod setup
TRAINING_IMAGE ?= polyhorizon-training:latest
PREFECT_FILE ?= prefect.yaml

training-build:
	docker build -f docker/training/Dockerfile -t $(TRAINING_IMAGE) .

prefect-pool:
	prefect work-pool create --type docker training-docker-pool || true

prefect-pool-serving:
	prefect work-pool create --type docker serving-docker-pool || true

prefect-pool-monitoring:
	prefect work-pool create --type docker monitoring-docker-pool || true

prefect-pool-streaming:
	prefect work-pool create --type docker streaming-docker-pool || true

prefect-pool-features:
	prefect work-pool create --type docker features-docker-pool || true

prefect-pools: prefect-pool prefect-pool-serving prefect-pool-monitoring prefect-pool-streaming prefect-pool-features

training-deploy:
	prefect deploy --all --prefect-file $(PREFECT_FILE)

training-build-deploy: training-build prefect-pool training-deploy

serving-build:
	docker build -f docker/serving/Dockerfile -t polyhorizon-serving:latest .

serving-deploy:
	bash scripts/deploy/serving_rolling_update.sh

serving-build-deploy: serving-build serving-deploy

monitoring-build:
	docker build -f docker/monitoring/Dockerfile -t polyhorizon-monitoring:latest .

all-build: training-build serving-build monitoring-build

all-deploy: prefect-pools training-deploy

ingestion-build:
	docker build -f docker/ingestion/Dockerfile -t polyhorizon-ingestion:latest .

ingestion-deploy: prefect-pools
	prefect deploy --all --prefect-file $(PREFECT_FILE)

ingestion-build-deploy: ingestion-build ingestion-deploy

# Features
features-build:
	docker build -f docker/features/Dockerfile -t polyhorizon-features:latest .

features-deploy: prefect-pools
	prefect deploy --all --prefect-file $(PREFECT_FILE)

features-build-deploy: features-build features-deploy

# Streaming
streaming-dry-run:
	uv run python -m polyhorizon.streaming.src.streaming_job --config polyhorizon/streaming/configs/spark_features.yaml --dry-run --skip-preflight

streaming-test:
	uv run pytest tests/streaming/feature_engineering

streaming-submit:
	docker compose up spark-submit

streaming-build:
	docker compose build spark-master spark-worker

frontend-build:
	docker build -f docker/frontend/Dockerfile -t polyhorizon-frontend:latest .

frontend-deploy:
	bash scripts/deploy/frontend_rolling_update.sh

frontend-build-deploy: frontend-build frontend-deploy

# Production deploy
ENV_FILE ?= .env.prod
COMPOSE_FILE ?= docker-compose.prod.yaml

deploy-prod:
	COMPOSE_FILE=$(COMPOSE_FILE) ENV_FILE=$(ENV_FILE) scripts/deploy/prod_deploy.sh

postgres-backup:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) --profile operations run --rm postgres-backup

postgres-restore-verify:
	test -n "$(BACKUP_PATH)"
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) --profile operations run --rm \
		-e BACKUP_PATH=$(BACKUP_PATH) --entrypoint bash postgres-backup /scripts/restore_verify.sh
