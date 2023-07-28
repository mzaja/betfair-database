@ECHO OFF
black --check betfairdatabase tests
coverage run --source=betfairdatabase -m unittest discover -s tests -b
coverage html
coverage report