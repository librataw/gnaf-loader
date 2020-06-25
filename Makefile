init:
	# install all necessary packages
	pip3 install -r requirements.txt; \

test:
	# run unit test
	python3 -m unittest discover gnaf_loader.tests; \

docs:
	# create html documentation using sphinx
	cd docs && make html; \

install:
	python3 setup.py clean install

.PHONY: init test docs install
