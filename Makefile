COMPOSE := docker compose

.PHONY: up down ps logs setup topics templates detector detector-gpu produce attack test dashboard kibana

up:            ## start Kafka, Elasticsearch and Kibana
	$(COMPOSE) up -d

down:          ## stop the stack (volumes are kept)
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

topics:        ## create Kafka topics (auto-creation is disabled)
	sh scripts/create-topics.sh

templates:     ## install Elasticsearch index templates
	sh scripts/apply-index-templates.sh

setup: topics templates

detector:      ## run the Spark streaming detector in Docker
	$(COMPOSE) --profile streaming run --rm --build detector

detector-gpu:
	$(COMPOSE) -f docker-compose.yml -f docker-compose.gpu.yml --profile streaming run --rm --build detector

produce:       ## continuous traffic with random anomalies
	python producer/kafka_producer.py --rate 20

attack:        ## quick brute-force demo
	python producer/kafka_producer.py --scenario brute-force --rate 100 --count 140

test:
	python -m pytest -q

dashboard:     ## web dashboard on http://localhost:3000
	node server.js

kibana:        ## create Kibana data views
	python scripts/setup_kibana.py
