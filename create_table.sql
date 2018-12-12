CREATE TABLE marcdata (
bibid text NOT NULL,
tag TEXT NOT NULL,
indicator1 TEXT NOT NULL,
indicator2 TEXT NOT NULL,
subfield TEXT NOT NULL,
value TEXT);
CREATE INDEX marcdata_index1 ON marcdata(bibid, tag, indicator1, indicator2, subfield);
CREATE INDEX marcdata_index2 ON marcdata(bibid);
CREATE INDEX marcdata_index3 ON marcdata(bibid, tag);
