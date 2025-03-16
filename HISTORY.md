# Release History

## 1.3.1 (2025-03-16)
### Bug fixes
- Field `marketTime` is aliased to `marketStartTime` in processed market definitions, rather than renamed.

## 1.3.0 (2025-03-16)
### Improvements
- Supports indexing official historical Betfair data, both free and purchased.
- Market metadata file is created from the market data file's market catalogue if a market catalogue file is missing. `marketCatalogueFilePath` field is renamed to `marketMetadataFilePath` to reflect this change.
- New fields added: `numberOfWinners`, `marketSettledTime` and `localMarketSettledTime`. Reindex the database to upgrade it to the new format.

## 1.2.0 (2025-02-23)
### Deprecations
- Short command line option for retrieving the tool version has been changed from `-v` to `-V` (lowercase to uppecase). The lowercase version continues to be supported, but will be removed in the future.
### Improvements
- Logging statements added. Default logging level is `INFO` in command line mode and `WARNING` otherwise.
- Added `-v` and `--verbose` command line switches for setting the logging level to `DEBUG`.
- Added `-q` and `--quiet` command line switches for disabling the logging altogether.
- Added `size` method which returns the number of indexed markets in the database.
- Long running methods (`index`, `insert`, `clean` and `export`) display optional progress bars.
- Python 3.13 support added.
### Bug fixes
- In command line mode,`export` command defaults `dest` argument to the current working directory if not specified. This is in line with how the other APIs behave.

## 1.1.0 (2024-03-11)
### Improvements
- `DatabaseDirectoryError` is raised when a database directory is not a directory or it does not exist.
- Command line application catches errors and prints meaningful error messages.

## 1.0.1 (2024-03-10)
### Bug fixes
- Unparsable market catalogue files do not break indexing and inserting.

## 1.0.0 (2023-10-10)
### Improvements
- `insert` method handles duplicate files.
- Python 3.12 support added.

## 0.3.0 (2023-09-11)
### Improvements
- Added `clean` method for removing entries with nonexistent files.

## 0.2.0 (2023-08-27)
### Improvements
- New `insert` method for updating the database with new files.
- Object-oriented interface through `BetfairDatabase` class.
- New generic fields: `localDayOfWeek`, `localDayOfWeek`, `localMarketStartTime`.
- New racing-specific fields: `raceId`, `raceTypeFromName`, `raceDistanceMeters`, `raceDistanceFurlongs`.
- `export` method accepts both directories and files as a destination.

## 0.1.1 (2023-07-31)
### Bug fixes
- Decode market catalogue files as UTF-8.

## 0.1.0 (2023-07-28)
First release.
