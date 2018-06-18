init:
	# create venv 
	python3 -m venv ../etl_venv; \
	# source venv
	. ../etl_venv/bin/activate; \
	# install all necessary packages
	pip3 install -r requirements.txt; \

test:
	. ../etl_venv/bin/activate; \
	# run unit test
	python3 -m unittest discover tests; \

docs:
	# create html documentation using sphinx
	cd docs && make html; \

install:
	# source venv
	. ../etl_venv/bin/activate; \
	python3 setup.py clean install

.PHONY: init test docs install
