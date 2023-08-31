@ECHO OFF
black --check betfairdatabase tests
if %ERRORLEVEL% NEQ 0 (EXIT /B 1)
isort --check betfairdatabase tests
if %ERRORLEVEL% NEQ 0 (EXIT /B 1)
coverage run --source=betfairdatabase -m unittest discover -s tests -b
coverage html
coverage report