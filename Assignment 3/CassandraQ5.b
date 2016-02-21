Question5:

CREATE KEYSPACE bxr140530 WITH replication={'class':'SimpleStrategy','replication_factor':1};
USE bxr140530;

CREATE TABLE businessnew(businessid varchar Primary Key,fulladdress varchar,categories varchar);
DESCRIBE table businessnew;
COPY businessnew(businessid,fulladdress,categories) FROM '/home/004/b/bx/bxr140530/BigData/dataset/business.csv' WITH DELIMITER = '^';
SELECT * from businessnew where businessid='HPWmjuivv3xJ279qSVf';
TRUNCATE businessnew;
DROP table businessnew;

