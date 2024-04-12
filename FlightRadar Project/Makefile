build:
	python setup.py bdist_wheel

test:
	python -m pytest tests/unit

flake8:
	flake8 etl
	flake8 tests/unit

test-reports:
	coverage run --source=etl -m pytest tests/unit
	coverage html
	coverage xml

install-dependencies:
	pip install -r requirements.txt

install:
	pip install -e .

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf etl.egg-info/

deploy:
	dbx deploy --jobs airlines_analyzer_extract, airlines_analyzer_transform_and_load --deployment-file conf/deployment.json

launch:
	dbx launch --job airlines_analyzer_extract, airlines_analyzer_transform_and_load --deployment-file conf/deployment.json
