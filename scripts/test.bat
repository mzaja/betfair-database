@ECHO OFF
black --check betfairdatabase tests
if %ERRORLEVEL% NEQ 0 (EXIT /B 1)
isort --check betfairdatabase tests
if %ERRORLEVEL% NEQ 0 (EXIT /B 1)
coverage run
coverage html
coverage report
