# Release History

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
