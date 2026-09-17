.PHONY: test smoke run docker-build docker-run

test:
	python3 -m unittest discover -s tests -v

smoke:
	./scripts/smoke.sh

run:
	EA_DATA_DIR=./data EA_ADDR=0.0.0.0:8080 python3 -m eventarch.server

docker-build:
	docker build -t eventarch:0.1.0 .

docker-run:
	docker run --rm -p 8080:8080 -v eventarch-data:/data eventarch:0.1.0
