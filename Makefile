SHELL := /bin/sh

INSTANCE ?= go-quant-tick
SERVICE_ACCOUNT ?=
MACHINE_TYPE ?= e2-micro
ZONE ?= asia-northeast1-a
SCOPES ?= cloud-platform
BOOT_DISK_SIZE ?= 10GB
BOOT_DISK_TYPE ?= pd-balanced
IMAGE_FAMILY ?= debian-12
IMAGE_PROJECT ?= debian-cloud
ARTIFACT_LOCATION ?= asia-northeast1
ARTIFACT_REPOSITORY ?= go-quant-tick
ARTIFACT_PACKAGE ?= quanttick
DEFAULT_ARTIFACT_VERSION := $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo dev)-$(shell date -u +%Y%m%d%H%M%S)
ARTIFACT_VERSION ?= $(DEFAULT_ARTIFACT_VERSION)
TARGET_OS ?= linux
TARGET_ARCH ?= amd64
TARGET_CGO ?= 0
BUILD_DIR ?= bin
ARTIFACT_NAME ?= quanttick-$(TARGET_OS)-$(TARGET_ARCH)
ARTIFACT_SOURCE := $(BUILD_DIR)/$(ARTIFACT_NAME)
TOPICS ?= raw-trades aggregated-trades significant-trades
CREATE_SUBSCRIPTION ?= 1
MESSAGE_RETENTION_DURATION ?=
RETAIN_ACKED_MESSAGES ?= false
SERVICE_ENV ?=
SERVICE_ENV_VARS := PUBLISH_STREAMS SIGNIFICANT_TRADE_FILTER BINANCE_SYMBOLS BINANCE_FUTURES_SYMBOLS BITFINEX_SYMBOLS BITMEX_SYMBOLS COINBASE_SYMBOLS HYPERLIQUID_SYMBOLS RAW_TRADES_TOPIC AGGREGATED_TRADES_TOPIC SIGNIFICANT_TRADES_TOPIC PUBLISH_TIMEOUT FLUSH_TIMEOUT SHUTDOWN_FLUSH_TIMEOUT SENTRY_DSN

STARTUP_SCRIPT := deploy/startup.sh

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

define require_project_id
project_id="$(PROJECT_ID)"; \
if [ -z "$$project_id" ]; then project_id="$$PROJECT_ID"; fi; \
test -n "$$project_id" || { echo "PROJECT_ID is required"; exit 1; }
endef

define require_service_account
service_account="$(SERVICE_ACCOUNT)"; \
if [ -z "$$service_account" ]; then service_account="$$SERVICE_ACCOUNT"; fi; \
test -n "$$service_account" || { echo "SERVICE_ACCOUNT is required"; exit 1; }
endef

define write_service_env
service_env_file=$$(mktemp); \
{ \
	printf 'PROJECT_ID=%s\n' "$$project_id"; \
	for name in $(SERVICE_ENV_VARS); do \
		eval value=\$$$${name}; \
		if [ -n "$$value" ]; then printf '%s=%s\n' "$$name" "$$value"; fi; \
	done; \
	service_env="$(SERVICE_ENV)"; \
	if [ -z "$$service_env" ]; then service_env="$${SERVICE_ENV:-}"; fi; \
	if [ -n "$$service_env" ]; then \
		old_ifs="$$IFS"; IFS=','; \
		for item in $$service_env; do [ -n "$$item" ] && printf '%s\n' "$$item"; done; \
		IFS="$$old_ifs"; \
	fi; \
} > "$$service_env_file"
endef

.PHONY: test race build build-linux artifact-name upload-artifact deploy-binary update-binary create-pubsub

test:
	go test ./...

race:
	go test -race ./...

build:
	go build -o bin/quanttick ./cmd/quanttick

build-linux:
	@mkdir -p "$(BUILD_DIR)"
	CGO_ENABLED=$(TARGET_CGO) GOOS=$(TARGET_OS) GOARCH=$(TARGET_ARCH) go build -trimpath -ldflags="-s -w" -o "$(ARTIFACT_SOURCE)" ./cmd/quanttick

artifact-name:
	@$(load_env); \
	$(require_project_id); \
	printf '%s/%s/%s/%s/%s\n' "$$project_id" "$(ARTIFACT_LOCATION)" "$(ARTIFACT_REPOSITORY)" "$(ARTIFACT_PACKAGE)" "$(ARTIFACT_VERSION)"

upload-artifact: build-linux
	@$(load_env); \
	$(require_project_id); \
	gcloud artifacts generic upload \
		--project "$$project_id" \
		--location "$(ARTIFACT_LOCATION)" \
		--repository "$(ARTIFACT_REPOSITORY)" \
		--package "$(ARTIFACT_PACKAGE)" \
		--version "$(ARTIFACT_VERSION)" \
		--source "$(ARTIFACT_SOURCE)"

deploy-binary: upload-artifact
	@$(load_env); \
	$(require_project_id); \
	$(require_service_account); \
	$(write_service_env); \
	gcloud compute instances create "$(INSTANCE)" \
		--project "$$project_id" \
		--zone "$(ZONE)" \
		--machine-type "$(MACHINE_TYPE)" \
		--image-family "$(IMAGE_FAMILY)" \
		--image-project "$(IMAGE_PROJECT)" \
		--boot-disk-size "$(BOOT_DISK_SIZE)" \
		--boot-disk-type "$(BOOT_DISK_TYPE)" \
		--no-address \
		--service-account "$$service_account" \
		--scopes "$(SCOPES)" \
		--metadata-from-file startup-script="$(STARTUP_SCRIPT)",SERVICE_ENV="$$service_env_file" \
		--metadata ARTIFACT_PROJECT_ID="$$project_id",ARTIFACT_LOCATION="$(ARTIFACT_LOCATION)",ARTIFACT_REPOSITORY="$(ARTIFACT_REPOSITORY)",ARTIFACT_PACKAGE="$(ARTIFACT_PACKAGE)",ARTIFACT_VERSION="$(ARTIFACT_VERSION)",ARTIFACT_NAME="$(ARTIFACT_NAME)"; \
	rm -f "$$service_env_file"

update-binary: upload-artifact
	@$(load_env); \
	$(require_project_id); \
	$(write_service_env); \
	gcloud compute instances add-metadata "$(INSTANCE)" \
		--project "$$project_id" \
		--zone "$(ZONE)" \
		--metadata-from-file startup-script="$(STARTUP_SCRIPT)",SERVICE_ENV="$$service_env_file" \
		--metadata ARTIFACT_PROJECT_ID="$$project_id",ARTIFACT_LOCATION="$(ARTIFACT_LOCATION)",ARTIFACT_REPOSITORY="$(ARTIFACT_REPOSITORY)",ARTIFACT_PACKAGE="$(ARTIFACT_PACKAGE)",ARTIFACT_VERSION="$(ARTIFACT_VERSION)",ARTIFACT_NAME="$(ARTIFACT_NAME)"; \
	rm -f "$$service_env_file"; \
	gcloud compute instances reset "$(INSTANCE)" \
		--project "$$project_id" \
		--zone "$(ZONE)"

create-pubsub:
	@$(load_env); \
	$(require_project_id); \
	set -e; \
	for topic in $(TOPICS); do \
		if ! gcloud pubsub topics describe "$$topic" --project "$$project_id" >/dev/null 2>&1; then \
			gcloud pubsub topics create "$$topic" --project "$$project_id"; \
		fi; \
		if [ "$(CREATE_SUBSCRIPTION)" = "1" ]; then \
			if ! gcloud pubsub subscriptions describe "$$topic" --project "$$project_id" >/dev/null 2>&1; then \
				cmd="gcloud pubsub subscriptions create $$topic --topic $$topic --project $$project_id"; \
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
