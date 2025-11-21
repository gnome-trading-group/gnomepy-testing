.PHONY: help install build test-proxy test-client clean docker-build docker-test docker-clean

help:
	@echo "Market Data Testing Framework (Capture Proxy Architecture)"
	@echo ""
	@echo "Available commands:"
	@echo "  make install        - Install Python dependencies with Poetry"
	@echo "  make build          - Build Docker image"
	@echo "  make test-proxy     - Run proxy server"
	@echo "  make test-client    - Run Python client (requires proxy running)"
	@echo "  make docker-build   - Build Docker image"
	@echo "  make docker-test    - Run test in Docker"
	@echo "  make clean          - Clean output files"
	@echo "  make docker-clean   - Clean Docker images"
	@echo ""
	@echo "Examples:"
	@echo "  make test-proxy LISTING_ID=1 DURATION=60"
	@echo "  make test-client LISTING_ID=1 DURATION=30"
	@echo "  make docker-test LISTING_ID=1 DURATION=60"

install:
	poetry install

build: docker-build

docker-build:
	docker-compose build

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
	@$(MAKE) test-proxy LISTING_ID=1 DURATION=60

test-binance:
	@$(MAKE) test-proxy LISTING_ID=2 DURATION=30

test-coinbase:
	@$(MAKE) test-proxy LISTING_ID=3 DURATION=30

