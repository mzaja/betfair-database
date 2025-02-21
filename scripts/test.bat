@ECHO OFF
black --check betfairdatabase tests
IF %ERRORLEVEL% NEQ 0 (EXIT /B 1)
isort --check betfairdatabase tests
IF %ERRORLEVEL% NEQ 0 (EXIT /B 1)
@REM Check .coveragerc for implicit parameters
coverage run
IF %ERRORLEVEL% NEQ 0 (EXIT /B 1)
coverage report
@REM Only generate HTML report if coverage is below 100 %
IF %ERRORLEVEL% NEQ 0 (
    coverage html
    firefox htmlcov\index.html
)
