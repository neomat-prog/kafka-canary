<img
  src="assets/0d09e8b7-7907-4f4c-b7ad-45672fc1e78c.png"
  alt="Gopher"
  width="400"
  align="right"
/>

<h1>kafka-canary</h1>

<p>A small Go service that checks whether a Kafka cluster is actually
delivering messages — by sending a probe, reading it back, and reporting the
result over HTTP.</p>

## What it is

A health check, not a metrics system. It runs a loop:

```text
produce ──> send a message to a topic every few seconds
consume ──> read it back, measure how long the round trip took
expose  ──> /ready returns 200 while messages flow, 503 when they stop
```

That's the whole thing. Point it at any Kafka, run it on Kubernetes, and let
the kubelet's readiness probe hit `/ready` — when Kafka stops delivering, the
pod goes NotReady and you know. Kafka errors are logged, never fatal: the
service stays up during an outage and reconnects on its own.

It's modeled on [strimzi-canary](https://github.com/strimzi/strimzi-canary),
built from scratch as a learning project — so the code favors being readable
over being clever.

## Endpoints

| Path | Returns |
|---|---|
| `/healthy` | `200` while the process is alive (liveness) |
| `/ready` | `200` if messages are flowing, `503` if stalled (readiness) |
| `/status` | JSON: `{"messagesFlowing":true,"lastLatencyMs":7,"lastConsumedAgo":"3.4s"}` |

## Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `CANARY_BROKERS` | `localhost:9092` | bootstrap broker list (CSV) |
| `CANARY_TOPIC` | `__strimzi_canary` | probe topic |
| `CANARY_CONSUMER_GROUP` | `canary-group` | consumer group id |
| `CANARY_PRODUCE_INTERVAL` | `5s` | how often to send a probe |
| `CANARY_METRICS_ADDR` | `:8080` | HTTP listen address |

## Run

```bash
go run ./cmd/canary          # local, against $CANARY_BROKERS
docker compose up --build    # Kafka + canary together
kubectl apply -f k8s/canary.yaml   # on Kubernetes (set CANARY_BROKERS first)
```

## License

[MIT](LICENSE)
