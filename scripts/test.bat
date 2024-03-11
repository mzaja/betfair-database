@ECHO OFF
black --check betfairdatabase tests
IF %ERRORLEVEL% NEQ 0 (EXIT /B 1)
isort --check betfairdatabase tests
IF %ERRORLEVEL% NEQ 0 (EXIT /B 1)
@REM Check .coveragerc for implicit parameters
coverage run
coverage html
coverage report
