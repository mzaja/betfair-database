# Test data info

## corrupt
Contains an empty ZIP and an empty JSON file. Use to trigger file processing errors.

## datasets
### official
Contains a mixture of BASIC and PRO Betfair historical market files.

Only `1.214555872.bz2` (greyhounds) file is accompanied by a JSON market definition file. The other archives expect to have their metadata files generated.

| Name            | Sport      | Tier  |
|-----------------|------------|-------|
| 1.145405534.bz2 | Soccer     | PRO   |
| 1.211006011.bz2 | Horses     | BASIC |
| 1.214555872.bz2 | Greyhounds | BASIC |
| 1.223716976.bz2 | Tennis     | BASIC |
| 1.230478683.bz2 | Golf       | BASIC |

### uncompressed
Contains a selection of self-recorded market stream files accompanied by market catalogues. `1.199967351.json` is a lone market catalogue file without an accompanying data file.

### zip-lzma
Contains a selection of self-recorded market stream files, compressed using ZIP-LZMA algorithm. Each ZIP file is acoompanied by a market catalogue file in JSON format.

## duplicates
Contains a mixture of duplicate market data and market metadata files already contained in [uncompressed](#uncompressed) and [zip-lzma](#zip-lzma) datasets.

## missing_data
A lone market catalogue without a matching market data file.

## missing_metadata
Contains a mixture of self-recorded and official Betfair historical market stream files without metadata. All supported file types are included. There are two special files in the dataset:
 - `1.209492553` is a self-recorded market stream file without a market definition inside.
 - `1.223716890` is a corrupt file with unparsable JSON, but does contain `"marketDefinition"` inside.

| Name            | Source   | Sport      | Lines |
|-----------------|----------|------------|-------|
| 1.197931750.gz  | Recorded | Greyhounds | 10    |
| 1.209492553     | Recorded | ???        | 18    |
| 1.214555872.bz2 | Official | Greyhounds | 53    |
| 1.219107753.zip | Recorded | Greyhounds | 29    |
| 1.223716890     | Official | Tennis     |  1    |
| 1.223716981     | Official | Tennis     | 19    |
