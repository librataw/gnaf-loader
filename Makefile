init:
	# create venv 
	python3 -m venv ../etl_venv \
	# do it in virtual environment mode
	. venv/bin/activate; \
	# install all necessary packages
	pip3 install -r requirements.txt; \

test:
	# run unit test
	python3 -m unittest discover tests

docs:
	# create html documentation using sphinx
	cd docs && make html

.PHONY: init test docs
