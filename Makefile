SHELL := /bin/sh

HOSTNAME ?= asia.gcr.io
IMAGE ?= go-quant-tick
TAG ?= latest
INSTANCE ?= go-quant-tick
SERVICE_ACCOUNT ?=
MACHINE_TYPE ?= e2-micro
ZONE ?= asia-northeast1-a
SCOPES ?= cloud-platform
TOPICS ?= raw-trades aggregated-trades significant-trades
CREATE_SUBSCRIPTION ?= 1
MESSAGE_RETENTION_DURATION ?=
RETAIN_ACKED_MESSAGES ?= false
CONTAINER_ENV ?=

define load_env
set -a; \
if [ -f .env ]; then \
	tmp=$$(mktemp); \
	sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$$/d' -e '/^[[:space:]]*\[/d' .env > "$$tmp"; \
	. "$$tmp"; \
	rm -f "$$tmp"; \
fi; \
set +a
endef

.PHONY: test race build container-name build-container push-container deploy-container update-container create-pubsub

test:
	go test ./...

race:
	go test -race ./...

build:
	go build -o bin/quanttick ./cmd/quanttick

container-name:
	@$(load_env); \
	project_id="$(PROJECT_ID)"; \
	if [ -z "$$project_id" ]; then project_id="$$PROJECT_ID"; fi; \
	test -n "$$project_id" || { echo "PROJECT_ID is required"; exit 1; }; \
	printf '%s\n' "$(HOSTNAME)/$$project_id/$(IMAGE):$(TAG)"

build-container:
	@$(load_env); \
	project_id="$(PROJECT_ID)"; \
	if [ -z "$$project_id" ]; then project_id="$$PROJECT_ID"; fi; \
	test -n "$$project_id" || { echo "PROJECT_ID is required"; exit 1; }; \
	docker build -t "$(HOSTNAME)/$$project_id/$(IMAGE):$(TAG)" .

push-container:
	@$(load_env); \
	project_id="$(PROJECT_ID)"; \
	if [ -z "$$project_id" ]; then project_id="$$PROJECT_ID"; fi; \
	test -n "$$project_id" || { echo "PROJECT_ID is required"; exit 1; }; \
	docker push "$(HOSTNAME)/$$project_id/$(IMAGE):$(TAG)"

deploy-container:
	@$(load_env); \
	project_id="$(PROJECT_ID)"; \
	if [ -z "$$project_id" ]; then project_id="$$PROJECT_ID"; fi; \
	service_account="$(SERVICE_ACCOUNT)"; \
	if [ -z "$$service_account" ]; then service_account="$$SERVICE_ACCOUNT"; fi; \
	container_env="$(CONTAINER_ENV)"; \
	if [ -z "$$container_env" ]; then container_env="$$CONTAINER_ENV"; fi; \
	test -n "$$project_id" || { echo "PROJECT_ID is required"; exit 1; }; \
	test -n "$$service_account" || { echo "SERVICE_ACCOUNT is required"; exit 1; }; \
	container_image="$(HOSTNAME)/$$project_id/$(IMAGE):$(TAG)"; \
	if [ -z "$$container_env" ]; then container_env="PROJECT_ID=$$project_id"; fi; \
	gcloud compute instances create-with-container "$(INSTANCE)" \
		--machine-type "$(MACHINE_TYPE)" \
		--zone "$(ZONE)" \
		--container-image "$$container_image" \
		--container-env "$$container_env" \
		--service-account "$$service_account" \
		--scopes "$(SCOPES)"

update-container: build-container push-container
	@$(load_env); \
	gcloud compute instances reset "$(INSTANCE)" --zone "$(ZONE)"

create-pubsub:
	@$(load_env); \
	project_id="$(PROJECT_ID)"; \
	if [ -z "$$project_id" ]; then project_id="$$PROJECT_ID"; fi; \
	test -n "$$project_id" || { echo "PROJECT_ID is required"; exit 1; }; \
	set -e; \
	for topic in $(TOPICS); do \
		if ! gcloud pubsub topics describe "$$topic" --project "$$project_id" >/dev/null 2>&1; then \
			gcloud pubsub topics create "$$topic" --project "$$project_id"; \
		fi; \
		if [ "$(CREATE_SUBSCRIPTION)" = "1" ]; then \
			if ! gcloud pubsub subscriptions describe "$$topic" --project "$$project_id" >/dev/null 2>&1; then \
				cmd="gcloud pubsub subscriptions create $$topic --topic $$topic --project $$project_id --enable-message-ordering"; \
				if [ -n "$(MESSAGE_RETENTION_DURATION)" ]; then \
					cmd="$$cmd --message-retention-duration $(MESSAGE_RETENTION_DURATION)"; \
				fi; \
				if [ "$(RETAIN_ACKED_MESSAGES)" = "true" ]; then \
					cmd="$$cmd --retain-acked-messages"; \
				fi; \
				$$cmd; \
			fi; \
		fi; \
	done
