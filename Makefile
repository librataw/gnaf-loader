init:
	# create venv 
	python3 -m venv env; \
	# source venv
	. env/bin/activate; \
	# install all necessary packages
	pip3 install -r requirements.txt; \

test:
	. env/bin/activate; \
	# run unit test
	python3 -m unittest discover tests; \

docs:
	# create html documentation using sphinx
	cd docs && make html; \

install:
	# source venv
	. env/bin/activate; \
	python3 setup.py clean install

.PHONY: init test docs install
