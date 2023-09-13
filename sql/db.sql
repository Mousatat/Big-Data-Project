-- switch to the database
\c project;

-- Optional
START TRANSACTION;

-- Add tables
-- emps table
CREATE TABLE customers (
    custid integer NOT NULL PRIMARY KEY,
    gender VARCHAR ( 50 ) NOT NULL,
    age integer,
    kids integer,
);

-- dept table
CREATE TABLE pings(
    custid integer NOT NULL REFERENCES customers(custid),
    timestp integer,
);

CREATE TABLE test(
    custid integer NOT NULL REFERENCES customers(custid),
    date VARCHAR ( 50 ) NOT NULL,
    onlinehrs integer,
);

-- No constraints to add

SET datestyle TO iso, ymd;

\COPY customers FROM 'data/customers.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

\COPY pings FROM 'data/train_new.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

\COPY test FROM 'data/test.csv' DELIMITER ',' CSV HEADER NULL AS 'null';

-- optional
--COMMIT;

UPDATE test SET date = TO_DATE(date, 'dd/mm/yy');

-- For checking the content of tables
SELECT * from customers;
SELECT * from pings;
SELECT * from test;
