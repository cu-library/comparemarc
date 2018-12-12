CREATE TABLE marcdata (
bibid text NOT NULL,
tag TEXT NOT NULL,
indicator1 TEXT NOT NULL,
indicator2 TEXT NOT NULL,
subfield TEXT NOT NULL,
value TEXT);
CREATE INDEX marcdata_index_bibid ON marcdata(bibid);
