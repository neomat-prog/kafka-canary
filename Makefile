build:
	@go build -o bin/canary ./cmd/consume

run: build
	@./bin/canary

consume:
	@go run ./cmd/consume

test:
	@go test ./... && go vet ./...
