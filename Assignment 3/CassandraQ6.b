
Question6:

CREATE TABLE reviewnew1(  reviewid varchar, userid varchar,   businessid varchar,   stars float,   PRIMARY KEY (userid, businessid));
COPY reviewnew1(reviewid,userid,businessid,stars) FROM '/home/004/b/bx/bxr140530/BigData/dataset/review.csv' WITH DELIMITER = '^';
CREATE INDEX newindex ON reviewnew1(stars);
SELECT * FROM reviewnew1 WHERE stars =4  LIMIT 10;
TRUNCATE reviewnew1;

check: SELECT * FROM reviewnew1;
0 rows
DROP table reviewnew1;

