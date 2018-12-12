# comparemarc

Let's say you've just updated a lot of MARC records, and you want to see the changes that have been made.

```bash
$ comparemarc load --help
```
```
Usage: comparemarc load [OPTIONS] INPUTFILE

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
$ comparemarc check --help
```
```
Usage: comparemarc check [OPTIONS] INPUTFILE OUTPUTFILE

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
$ comparemarc gremlin --help
```
```
Usage: main.py gremlin [OPTIONS]

  Unleash a gremlin into the database to make changes.

Options:
  --delete INTEGER  Percent of rows in the table to delete.
  --change INTEGER  Percent of rows in the database to change the value of.
  --add INTEGER     Percent of rows in the database to copy, modify, and add.
  --help            Show this message and exit.
```

## Example usage

