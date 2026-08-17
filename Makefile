.PHONY: help install build test-proxy test-client clean docker-build docker-test docker-clean build-for-docker

help:
	@echo "Market Data Testing Framework (Capture Proxy Architecture)"
	@echo ""
	@echo "Available commands:"
	@echo "  make install              - Install Python dependencies with Poetry"
	@echo "  make build                - Build Docker image"
	@echo "  make test-proxy           - Run proxy server"
	@echo "  make test-client          - Run Python client (requires proxy running)"
	@echo "  make docker-build         - Build Docker image"
	@echo "  make docker-test          - Run test in Docker"
	@echo "  make build-for-docker     - Build gnome-orchestrator for Linux, copy .m2 (run before docker-build)"
	@echo "  make check-maven-updates  - Check for gnome-orchestrator updates"
	@echo "  make update-maven-deps    - Update gnome-orchestrator to latest version"
	@echo "  make clean                - Clean output files"
	@echo "  make docker-clean         - Clean Docker images"
	@echo ""
	@echo "Examples:"
	@echo "  make test-proxy LISTING_ID=1 DURATION=60"
	@echo "  make test-client LISTING_ID=1 DURATION=30"
	@echo "  make docker-test LISTING_ID=1 DURATION=60"
	@echo "  make check-maven-updates"
	@echo "  make update-maven-deps"

install:
	poetry install

build: docker-build

# Docker build workflow (run these in order when gnome-orchestrator source has changed):
#   1. mvn install          (in gnome-orchestrator — builds on macOS as usual)
#   2. make build-for-docker (here — rebuilds inside Linux so the fat JAR contains libNativeSockets.so)
#   3. make docker-build    (here — builds the test image using the Linux .m2)
#
# Why: mvn install on macOS produces a fat JAR with libNativeSockets.dylib. The Docker
# container runs Linux and needs libNativeSockets.so. build-for-docker re-runs the Maven
# build inside a Linux container, overwriting the SNAPSHOT in .m2 with the Linux version.
build-for-docker:
	@echo "Building gnome-gateways and gnome-orchestrator for Linux and copying .m2..."
	@cp -r ~/.m2 .m2
	@cp ../gnome-orchestrator/settings.xml .m2/settings.xml
	docker run --rm \
		--env-file .env \
		-v $(abspath .m2):/root/.m2 \
		-v $(abspath ../gnome-gateways):/gateways \
		-v $(abspath ../gnome-orchestrator):/workspace \
		maven:3.9-eclipse-temurin-21 \
		sh -c "mvn -f /gateways/pom.xml clean install -DskipTests && mvn -f /workspace/pom.xml clean install -DskipTests"
	@echo "Done. Run 'make docker-build'."

docker-build:
	docker-compose build

check-maven-updates:
	@echo "Checking for gnome-orchestrator updates..."
	@cd docker && mvn versions:display-dependency-updates -Dincludes=group.gnometrading:gnome-orchestrator

update-maven-deps:
	@echo "Updating gnome-orchestrator to latest version..."
	@cd docker && mvn versions:use-latest-releases -DallowSnapshots=false -Dincludes=group.gnometrading:gnome-orchestrator
	@echo "Updated! Check docker/pom.xml for the new version."

test-proxy:
	@echo "Running Capture Proxy server..."
	poetry run python -m gnomepy_testing.capture_proxy \
		$(or $(LISTING_ID),1) \
		--duration $(or $(DURATION),600) \
		--port $(or $(PORT),8765) \
		--log-file output/proxy.log \
		--expected-clients 1

test-client:
	@echo "Running Python client..."
	poetry run python -m gnomepy_testing.client.proxy_client \
		--host $(or $(HOST),localhost) \
		--port $(or $(PORT),8765) \
		--output output/python_listing_$(or $(LISTING_ID),1).bin \
		--duration $(or $(DURATION),30)

test-comparison:
	@echo "Running comparison..."
	poetry run python -m gnomepy_testing.compare_outputs \
		--python output/python_listing_$(or $(LISTING_ID),1).bin \
		--java output/java_listing_$(or $(LISTING_ID),1).bin \
		--ignore-fields timestamp_recv

test-print-output:
	@echo "Printing output..."
	poetry run python -m gnomepy_testing.compare_outputs \
		--python output/python_listing_$(or $(LISTING_ID),1).bin \
		--java output/java_listing_$(or $(LISTING_ID),1).bin \
		--ignore-fields timestamp_recv \
		--print \
		--max-messages $(or $(MAX_MESSAGES),10)

docker-test:
	docker-compose run --rm \
		-e LISTING_ID=$(or $(LISTING_ID),1) \
		-e DURATION=$(or $(DURATION),10) \
		market-data-test

clean:
	rm -rf output/*.bin
	rm -rf output/*.log
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

docker-clean:
	docker-compose down --rmi local
	docker system prune -f

# Specific listing tests
test-hyperliquid:
	@$(MAKE) test-proxy LISTING_ID=6 DURATION=60

test-binance:
	@$(MAKE) test-proxy LISTING_ID=2 DURATION=30

test-coinbase:
	@$(MAKE) test-proxy LISTING_ID=3 DURATION=30

test-polymarket:
	@$(MAKE) test-proxy LISTING_ID=16307 DURATION=30

