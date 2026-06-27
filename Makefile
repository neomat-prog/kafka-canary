build:
	@go build -o bin/canary ./cmd/canary/

run: build
	@./bin/canary

consume:
	@go run ./cmd/consume

test:
	@go test ./... && go vet ./...
