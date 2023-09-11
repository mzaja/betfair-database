# betfair-database
[![test](https://github.com/mzaja/betfair-database/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/mzaja/betfair-database/actions/workflows/test.yml) [![Coverage Status](https://coveralls.io/repos/github/mzaja/betfair-database/badge.svg?branch=main)](https://coveralls.io/github/mzaja/betfair-database?branch=main) ![PyPI version](https://img.shields.io/pypi/v/betfairdatabase) ![Python version](https://img.shields.io/pypi/pyversions/betfairdatabase) ![License](https://img.shields.io/github/license/mzaja/betfair-database)

Turns a collection of historical Betfair data into a queryable SQL database.

## Installation
Install the package from **PyPI**:
```bash
pip install betfairdatabase
```

## Usage
### Getting started
1. Index the folder holding historical Betfair data to turn it into a database.
2. Use SQL queries to select data.

```py
import betfairdatabase as bfdb

path_to_data = "./my_betfair_data"
bfdb.index(path_to_data)  # Create an index to convert the folder into a database

# Select all greyhound races in Sheffield
dataset = bfdb.select(
    path_to_data, where="eventTypeId='4339' AND eventVenue='Sheffield'"
)
for market in dataset:
    print(
        market["marketDataFilePath"],  # Path to stream data file
        market["marketCatalogueFilePath"],  # Path to market catalogue file
    )
```

The historical data can be grouped and divided using any subfolder hierarchy, but it must follow this convention:

1. Market catalogue is stored in a JSON file named `<market id>.json`.
2. Market data file (containing stream data) is stored in the same folder as the market catalogue file. It shares the same basename `<market id>` and ends with `.zip`, `.gz` or `.bz2`, or it has no extension (uncompressed data).

A sample database structure is shown below:
```
my_betfair_data/
├── arbitrary_folder/
    ├── 1.22334455.json  # Market catalogue file
    ├── 1.22334455  # Uncompressed market data file
    ├── 1.55667788.json  # Market catalogue file
    └── 1.55667788.zip  # Compressed market data file
```

### Retrieving data
`select()` method accepts the following arguments:
- `database_dir`: Main directory of the database initialised with `index()`.
- `columns`: A list of columns (field names) to retrieve. If omitted, all columns are returned. View a list of available columns by calling `betfairdatabase.columns()`.
- `where`: SQL query to execute.
- `limit`: Maximum number of results to return. If omitted, all results are returned.
- `return_dict`: If `True` (default), results are returned as a dictionary where keys are column names and values are data. If `False`, results are returned as tuples containing only data. The second option is faster but makes data harder to work with.

Below are several examples of selecting and filtering data:

```py
import betfairdatabase as bfdb

path_to_data = "./my_betfair_data"

# Return all market ids and paths to data files in the database
bfdb.select(path_to_data, columns=["marketId", "marketDataFilePath"])

# Return full market metadata for horse racing win markets
bfdb.select(path_to_data, where="eventTypeId='7' AND marketType='WIN'")

# Return full market metadata for a maximum of 100 BSP markets
bfdb.select(path_to_data, where="bspMarket=true", limit=100)

# Return a maximum of 250 data file paths for horse and greyhound racing
bfdb.select(
    path_to_data,
    columns=["marketDataFilePath"],
    where="eventTypeId IN ('7', '4339') AND marketType='WIN'",
    limit=250,
)
```

### Inserting data
Database can be updated with new files using `insert` method. This is much faster and more efficient than reindexing the whole database on each update. Files are moved by default, but they can also be copied if `copy=True` argument is provided.

```py
import betfairdatabase as bfdb

bfdb.insert("./my_betfair_data", "./my_capture_dir")
```

### Exporting data
Database index can be exported to a CSV file with the `export()` method. This is useful for debugging, visualising data and post-processing it with external tools.

```py
import betfairdatabase as bfdb

csv_file = bfdb.export("./my_betfair_data", "./my_data_dump")
print(csv_file)  # Prints: ./my_data_dump/my_betfair_data.csv
```

### Removing missing data
Throughout the course of database's lifetime, indexed files may get removed. `clean()` method checks for the presence of indexed market data files and removes the missing entries from the index, avoiding the need to reindex the whole database on every single file removal. However, reindexing the database may be the faster option when a large number of files has been removed.

```py
import betfairdatabase as bfdb

bfdb.clean("./my_betfair_data")
```

### Object-oriented interface
All of the above methods can also be accessed through OOP interface via `BetfairDatabase` class. This is useful when performing multiple operations on the same database as the database directory needs to be provided only once.
```py
from betfairdatabase import BetfairDatabase

db = BetfairDatabase("./my_betfair_data")
db.index()
db.select()
db.insert("./my_capture_dir")
db.export()
db.clean()
```

### Command line interface
The package also installs a `bfdb` command line app, which provides access to the following methods:
```bash
bfdb index "./my_database_dir"  # Index a database
bfdb export "./my_database_dir" "./my_db_dump.csv" # Export a database
bfdb insert "./my_database_dir" "./my_captured_data"  # Update the database
bfdb clean "./my_database_dir"  # Clean the database
```

For more information about the command line interface, run:
```bash
bfdb --help
```
