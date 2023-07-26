@REM Publishes the package to PyPI
@REM Install/upgrade twine using: python -m pip install twine --upgrade
@REM API access token must be present in %USERPROFILE%\.pypirc

@REM Upload to Test PyPI to verify everything is ok
python -m twine upload --repository testpypi dist/*

@REM Manually execute the command below to upload to PyPI
@REM python -m twine upload dist/*