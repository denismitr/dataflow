default: help

.PHONY: help
help:
	@echo 'Usage: make [target] ...'
	@echo ''
	@echo 'targets:'
	@LC_ALL=C $(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'

.PHONY: deps
deps:
	$(info Updating and vendoring dependencies...)
	@go mod tidy
	@go mod vendor
	@echo All done.

.PHONY: test
test:
	$(info Running unit tests...)
	@go test -v -race ./...
	@sleep 1
	@echo All done.	

.PHONY: test-keyvalue
test-keyvalue:
	$(info Running unit tests for keyvalue...)
	@go test -v -race ./keyvalue/... -count=1
	@sleep 1
	@echo All tests done.	

.PHONY: lint
lint:
	$(info Running linters...)
	@golangci-lint run  --config=.golangci.yml --timeout=180s ./...