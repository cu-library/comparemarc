# comparemarc

Let's say you've just updated a lot of MARC records, and you want to see the changes that have been made.

```bash
$ ./comparemarc.py load --help
```
```
Usage: ./comparemarc.py load [OPTIONS] INPUTFILE

  Load a MARC file into the database.

Options:
  --records INTEGER             Number of records in the input, use to skip
                                counting records.
  --delete / --no-delete        Delete the marcdata table before loading.
  --bibidselector TEXT          Where is the unique bibid stored. ex: 001 or
                                907a
  --trimbibid / --no-trimbibid  Delete the last character in the bibid.
  --help                        Show this message and exit.
```

```bash
$ ./comparemarc.py check --help
```
```
Usage: ./comparemarc.py check [OPTIONS] INPUTFILE OUTPUTFILE

  Check a MARC file against the database to ensure no unexpected changes
  occurred.

Options:
  --records INTEGER             Number of records in the input, use to skip
                                counting records.
  --bibidselector TEXT          Where is the unique bibid stored. ex: 001 or
                                907a
  --trimbibid / --no-trimbibid  Delete the last character in the bibid.
  --ignore TEXT                 Changes in these tags will be ignored.
  --help                        Show this message and exit.
```

```bash
$ ./comparemarc.py gremlin --help
```
```
Usage: ./comparemarc.py gremlin [OPTIONS]

  Unleash a gremlin into the database to make changes.

Options:
  --delete INTEGER  Percent of rows in the table to delete.
  --change INTEGER  Percent of rows in the database to change the value of.
  --add INTEGER     Percent of rows in the database to copy, modify, and add.
  --help            Show this message and exit.
```

## Setup

To speed up execution on large MARC files, comparemarc uses a PostgreSQL database to store the 'before' data. You'll need to create database user, database, and table. Example create table /create index statements are in `create_table.sql`.

```bash
$ git clone https://github.com/cu-library/comparemarc.git
$ cd comparemarc/
$ python36 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ cp config.py-example config.py
$ nano config.py # Edit the config file with your DB table, username, password.
```

## Example Usage

For testing, a truncated version of the MARC file at https://www.lib.umich.edu/open-access-bibliographic-records was created, with 9008 records. Passing `--records` to the load tool avoids having to count the records, making loading faster. Especially for large MARC files, it's a good idea to use a known or approximate number of records.

The `gremlin` command is useful for testing purposes, to see if random changes to the DB are caught.

```
$ ./comparemarc.py load --no-trimbibid --records 9008 umich_trunc.marc
Deleting 'marcdata' table... done.
Processing MARC records  [####################################]  100%
Done, processed 9008 records.
$ ./comparemarc.py gremlin
Deleting rows from the 'marcdata' table... done. [23404]
Changing rows in the 'marcdata' table... done. [21179]
Adding new rows to the 'marcdata' table... done. [20657]
$ ./comparemarc.py check --no-trimbibid --records 9008 umich_trunc.marc report.txt
Processing MARC records  [####################################]  100%
Done, processed 9008 records.
```

The report has changes per record, and a summary of the changes that occurred.
