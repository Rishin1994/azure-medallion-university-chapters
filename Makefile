.PHONY: setup run-fixture run-live test clean

PY := .venv/bin/python

setup:
	python -m venv .venv
	.venv/bin/pip install -r requirements.txt

run-fixture:
	$(PY) -m pipeline.run_pipeline --source fixture

run-live:
	$(PY) -m pipeline.run_pipeline --source live

test:
	$(PY) -m pytest tests/ -v

clean:
	rm -rf lake .pytest_cache
