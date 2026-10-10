# Log Sentinel

Real-time anomaly detection for server access logs.

```
log generator ──► Kafka: server-logs ──► PySpark Structured Streaming ──► Kafka: anomalies
                                                   │
                                                   ├──► Elasticsearch: server-logs-*  ──┐
                                                   └──► Elasticsearch: anomalies-*    ──┤
                                                                                        ▼
                                                                         Kibana  +  web dashboard
```

The generator emits JSON (or Apache Combined) log lines with injectable attacks. Spark parses them,
a stateful window analyzer applies four rules, and findings go to Elasticsearch and back onto Kafka.

## Detection rules

| Anomaly | Rule (per 1-minute event-time window) | Severity |
|---|---|---|
| `brute_force` | more than 100 HTTP 401/403 responses from one IP | critical |
| `traffic_surge` | request count above 5x the average of the previous 5 windows | critical |
| `error_burst` | more than 1% of requests are HTTP 5xx (at least 20 requests) | high |
| `latency_spike` | a request slower than mean + 3 sigma of its window (after 30 samples) | high |

Each finding is emitted once per window, even when the window is spread over several Spark micro-batches.
Thresholds can be changed in `.env`.

## Requirements

- Docker Desktop with about 6 GB of memory
- Python 3.10+ and `pip install -r requirements.txt` (for the producer and tests)
- Node 18+ (for the web dashboard only)
- `make` is optional; every Makefile target is a one-line command you can run by hand

## Quick start

```bash
cp .env.example .env            # optional, defaults work
docker compose up -d            # Kafka, Elasticsearch, Kibana
docker compose ps               # wait until all three are healthy

make setup                      # create topics + index templates
#   Windows without make:  ./scripts/create-topics.ps1 ; ./scripts/apply-index-templates.ps1

make detector                   # terminal 1: Spark detector (first run downloads the Kafka connector)
make attack                     # terminal 2: 140 brute-force logins from one IP
```

You should see a `brute_force` finding printed by the detector within a few seconds.
Then look at the results:

```bash
curl "localhost:9200/anomalies-*/_search?pretty"
make dashboard                  # http://localhost:3000
make kibana                     # creates data views; then open http://localhost:5601
```

### Seeing a traffic surge

Surge detection compares a window with the windows before it, so it needs a baseline first.
Run steady traffic for two minutes, then start the flood:

```bash
python producer/kafka_producer.py --rate 20 --anomaly-rate 0 --count 2400   # ~2 minutes of baseline
python producer/kafka_producer.py --rate 20 --scenario ddos --count 8000    # flood (sent 20x faster)
```

## Generator

```bash
python producer/log_generator.py --scenario latency --count 10
python producer/log_generator.py --scenario error-burst --count 40
python producer/log_generator.py --format apache --count 5
python producer/kafka_producer.py --rate 50              # continuous, random anomalies (Ctrl+C to stop)
python producer/kafka_producer.py --stdin < my-logs.txt  # replay existing log lines
```

Options: `--rate`, `--anomaly-rate`, `--scenario`, `--format json|apache`, `--count`, `--seed`.
Apache lines carry no response time, so they get a latency of 0 and are never flagged as latency spikes.

## Web dashboard

`node server.js` serves `public/index.html` and `/api/dashboard`, which queries Elasticsearch.
It has no npm dependencies. Set `DEMO_MODE=true` to preview it with sample data and no Elasticsearch.
`vercel.json` is included for deploying the dashboard to Vercel against a hosted Elasticsearch
(set `ELASTICSEARCH_URL` and `ELASTICSEARCH_API_KEY`).

## Tests

```bash
python -m pytest -q
npm test          # syntax check of the Node files
```

The rules, parser, window analyzer, generator and producer helpers are plain Python, so the tests run
without Docker, Kafka or Spark.

## Layout

```
producer/          log_generator.py, kafka_producer.py
spark_jobs/        detector.py (Spark job), analyzer.py (window state), rules.py, log_parser.py, sinks.py, Dockerfile
elastic_config/    index templates (strict mappings)
scripts/           topic + template helpers (.sh and .ps1), Kibana data view setup
api/, server.js    dashboard backend        public/   dashboard frontend
tests/
```

## Notes and gotchas

- **Ports:** the host producer uses `localhost:9094`; containers use `kafka:9092`.
- **Topics are not auto-created.** Run `make setup` before producing.
- **Late data:** the Spark watermark is 2 minutes, and the analyzer keeps 15 windows of state. Older records are ignored.
- **Scale:** Spark parses in parallel, but each micro-batch is collected to the driver for the window analysis,
  so `SPARK_MAX_OFFSETS_PER_TRIGGER` (default 50,000) bounds the memory used. That is plenty for a demo; a
  production design would move the aggregation into Spark itself.
- **Security:** Elasticsearch security is disabled in `docker-compose.yml` and disk-threshold checks are off.
  That is for local development only.
- **Spark GPU:** `docker-compose.gpu.yml` is an optional starting point; it also needs the RAPIDS jar added to
  the `spark-submit` packages.
