@ECHO OFF
@REM Publishes the package to PyPI
@REM Install/upgrade twine using: python -m pip install twine --upgrade
@REM API access token must be present in %USERPROFILE%\.pypirc

@REM Upload to Test PyPI to verify everything is ok
@REM python -m twine upload --repository testpypi dist/*

@REM Manually execute the command below to upload to PyPI
@REM python -m twine upload dist/*

@REM =========================================================
@REM Check history, commit, tag the release and push to remote
@REM =========================================================
@REM Set release version here:
SET RELEASE_VERSION=1.2.0

@REM Check that the distribution has been built already
IF NOT EXIST "dist\betfairdatabase-%RELEASE_VERSION%-py3-none-any.whl" (
    ECHO Wheel missing for the current release!
    EXIT 1
)
IF NOT EXIST "dist\betfairdatabase-%RELEASE_VERSION%.tar.gz" (
    ECHO tar.gz missing for the current release!
    EXIT 1
)

@REM Check that release version is present in changelog
FINDSTR /C:"version = \"%RELEASE_VERSION%\"" "pyproject.toml" > NUL
IF %ERRORLEVEL% NEQ 0 (
    ECHO pyproject.toml does not contain the release version!
    EXIT 1
)

@REM Check that release version is present in changelog
FINDSTR /C:"%RELEASE_VERSION%" "HISTORY.md" > NUL
IF %ERRORLEVEL% NEQ 0 (
    ECHO HISTORY.md does not contain the release version!
    EXIT 1
)

@REM Run pre-commit on all files
pre-commit run
IF %ERRORLEVEL% NEQ 0 (
    ECHO pre-commit found errors and modified files!
    EXIT 1
)

@REM Stage and commit files, add tags and push
git add HISTORY.md pyproject.toml scripts\deploy.bat
git commit -m "Prepare release %RELEASE_VERSION%"
git tag v%RELEASE_VERSION%
git push
git push --tags

@REM Go to package repository and add a release manually
START firefox https://github.com/mzaja/betfair-database/releases/new

@REM Ask for confirmation before publishing the package to PyPI
SET /P choice="Upload package to PyPI? (y/n) "
IF %choice%==y ( python -m twine upload dist/* )
