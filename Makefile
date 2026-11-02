ITCH ?= $(HOME)/quantfactory/_data/itch/01302019.NASDAQ_ITCH50.gz
DB ?= $(HOME)/quantfactory/_data/tickstore/db-01302019
.PHONY: build test race bench data demo fuzz lint clean
build:
	go build -o bin/ ./cmd/...
test:
	go test ./...
race:
	go test -race ./...
data: build
	bin/tickstore ingest -itch $(ITCH) -dir $(DB)
bench: build
	bash scripts/bench.sh
demo: build
	bash scripts/demo.sh
fuzz:
	go test -run '^$$' -fuzz FuzzTimestamps -fuzztime $${FUZZTIME:-30s} ./internal/enc/
	go test -run '^$$' -fuzz FuzzReader -fuzztime $${FUZZTIME:-30s} ./internal/store/
lint:
	go vet ./...
clean:
	rm -rf bin
