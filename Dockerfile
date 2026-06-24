FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /canary ./cmd/canary

FROM gcr.io/distroless/static:nonroot
COPY --from=build /canary /canary
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/canary"]
