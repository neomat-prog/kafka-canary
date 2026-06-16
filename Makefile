build:
# change from /fs to maybe dfs
	@go build -o bin/fs

run: build
	@./bin/fs

test:
	@go test ./... -v 