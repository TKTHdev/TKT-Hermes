BINARY_NAME  := hermes_server
CONFIG_FILE  := cluster.conf
LOG_DIR      := ./logs
RESULT_DIR   := ./results

ALL_IDS := $(shell jq -r '.[] | select(.role == "server") | .id' $(CONFIG_FILE))
TARGET_ID ?= all
ifeq ($(TARGET_ID),all)
    IDS := $(ALL_IDS)
else
    IDS := $(TARGET_ID)
endif

DEBUG ?= false
DEBUG_FLAG :=
ifeq ($(DEBUG),true)
    DEBUG_FLAG := --debug
endif

# Benchmark parameters
WORKLOAD ?= ycsb-a
WORKERS  ?= 1
TYPE ?= ycsb-a
KEYS ?= 6
TIMESTAMP := $(shell date +%Y%m%d_%H%M%S)

# Experiment parameters
EXPERIMENT_SAMPLES ?= 3
EXPERIMENT_TYPES   ?= ycsb-a ycsb-b ycsb-c

.PHONY: help build start kill clean benchmark experiment

help:
	@echo "Usage: make [target] [OPTIONS]"
	@echo ""
	@echo "Targets:"
	@echo "  build      - Build the binary"
	@echo "  start      - Start all nodes (or TARGET_ID=n for specific node)"
	@echo "  kill       - Kill all nodes (or TARGET_ID=n for specific node)"
	@echo "  clean      - Remove binaries and logs"
	@echo "  benchmark  - Run YCSB benchmark for a single workload type (CSV output)"
	@echo "  experiment - Run all workload types, multiple samples, and plot results"
	@echo ""
	@echo "Options:"
	@echo "  DEBUG=true                        - Enable debug logging"
	@echo "  TYPE=ycsb-a                       - Workload type for benchmark (ycsb-a, ycsb-b, ycsb-c)"
	@echo "  WORKERS='1 2 4'                   - Space-separated worker counts"
	@echo "  KEYS='6 100 1000'                 - Space-separated key counts"
	@echo "  EXPERIMENT_SAMPLES=3              - Number of samples per condition (experiment only)"
	@echo "  EXPERIMENT_TYPES='ycsb-a ycsb-b'  - Workload types to sweep (experiment only)"

build:
	go build -o $(BINARY_NAME) .

start: build
	@mkdir -p $(LOG_DIR)
	@for id in $(IDS); do \
		echo "Starting node $$id..."; \
		./$(BINARY_NAME) start --id $$id --conf $(CONFIG_FILE) $(DEBUG_FLAG) > $(LOG_DIR)/node_$$id.log 2>&1 & \
		echo $$! > $(LOG_DIR)/node_$$id.pid; \
	done
	@echo "All nodes started."

kill:
	@for id in $(IDS); do \
		if [ -f $(LOG_DIR)/node_$$id.pid ]; then \
			pid=$$(cat $(LOG_DIR)/node_$$id.pid); \
			echo "Killing node $$id (PID: $$pid)..."; \
			kill $$pid 2>/dev/null || echo "Node $$id not running."; \
			rm -f $(LOG_DIR)/node_$$id.pid; \
		else \
			echo "Node $$id: no PID file found."; \
		fi; \
	done

clean:
	rm -f $(BINARY_NAME)
	rm -rf $(LOG_DIR)
	rm -rf $(RESULT_DIR)

benchmark: build
	@mkdir -p $(RESULT_DIR) $(LOG_DIR)
	@CSV="$(RESULT_DIR)/benchmark_$(TIMESTAMP)_$(TYPE).csv"; \
	echo "Workload,Workers,Keys,Throughput(ops/sec),Latency(ms)" > "$$CSV"; \
	for w in $(WORKERS); do \
		for k in $(KEYS); do \
			echo "=== Workers=$$w Keys=$$k ==="; \
			$(MAKE) kill 2>/dev/null || true; \
			sleep 1; \
			for id in $(ALL_IDS); do \
				./$(BINARY_NAME) start --id $$id --conf $(CONFIG_FILE) $(DEBUG_FLAG) \
					> $(LOG_DIR)/node_$$id.log 2>&1 & \
				echo $$! > $(LOG_DIR)/node_$$id.pid; \
			done; \
			sleep 2; \
			./$(BINARY_NAME) client \
				--conf $(CONFIG_FILE) --workload $(TYPE) \
				--workers $$w --keys $$k $(DEBUG_FLAG) \
				| tee -a $(LOG_DIR)/bench.log \
				| grep '^RESULT:' \
				| sed 's/^RESULT://' >> "$$CSV"; \
			$(MAKE) kill 2>/dev/null || true; \
			sleep 1; \
		done; \
	done; \
	echo "Results written to $$CSV"; \
	cat "$$CSV"

experiment: build
	@mkdir -p $(RESULT_DIR) $(LOG_DIR)
	@CSV="$(RESULT_DIR)/experiment_$(TIMESTAMP).csv"; \
	PLOT="$(RESULT_DIR)/experiment_$(TIMESTAMP).png"; \
	echo "Workload,Workers,Keys,Throughput(ops/sec),Latency(ms)" > "$$CSV"; \
	for type in $(EXPERIMENT_TYPES); do \
		for w in $(WORKERS); do \
			for k in $(KEYS); do \
				for s in $$(seq 1 $(EXPERIMENT_SAMPLES)); do \
					echo "=== Type=$$type Workers=$$w Keys=$$k Sample=$$s/$(EXPERIMENT_SAMPLES) ==="; \
					$(MAKE) kill 2>/dev/null || true; \
					sleep 1; \
					for id in $(ALL_IDS); do \
						./$(BINARY_NAME) start --id $$id --conf $(CONFIG_FILE) $(DEBUG_FLAG) \
							> $(LOG_DIR)/node_$$id.log 2>&1 & \
						echo $$! > $(LOG_DIR)/node_$$id.pid; \
					done; \
					sleep 2; \
					./$(BINARY_NAME) client \
						--conf $(CONFIG_FILE) --workload $$type \
						--workers $$w --keys $$k $(DEBUG_FLAG) \
						| tee -a $(LOG_DIR)/experiment.log \
						| grep '^RESULT:' \
						| sed 's/^RESULT://' >> "$$CSV"; \
					$(MAKE) kill 2>/dev/null || true; \
					sleep 1; \
				done; \
			done; \
		done; \
	done; \
	echo ""; \
	echo "Results written to $$CSV"; \
	uv run --script scripts/plot_experiment.py "$$CSV" "$$PLOT"
