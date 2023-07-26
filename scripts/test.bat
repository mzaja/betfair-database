@ECHO OFF
@REM black --check tests betfairdatabase
coverage run --source=betfairdatabase -m unittest discover -s tests -b
coverage html
coverage report