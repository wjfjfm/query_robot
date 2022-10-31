# Query Robot

a rebot to execute query and send result to spesific databse

## Quick Start

```python
 robo = QueryRobo()
 robo.robo_db =   DBInfo("robo_host",   "user", "password", "database", 3306, "table")
 robo.query_db =  DBInfo("query_host",  "user", "password", "database", 3306)
 robo.output_db = DBInfo("output_host", "user", "password", "database", 3306, "table")

 robo.run()
```

## In Detail

Query rebot will:
- find command in robo_db
- execuete command in query_db
- output result to output_db


## DDL

```SQL
CREATE TABLE robo_command (
  id int(8) PRIMARY KEY AUTO_INCREMENT,
  precheck int(8),
  query TEXT,
  if_exist BOOLEAN DEFAULT 1 NOT NULL,
  if_change BOOLEAN DEFAULT 1 NOT NULL,
  if_report BOOLEAN DEFAULT 1 NOT NULL,
  report_interval int DEFAULT 60 NOT NULL,
  output varchar(255)
);

CREATE TABLE output (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  text TEXT
);
```

## Sample Command

```SQL
INSERT INTO robo_command (query, output)
VALUES (
  "SELECT *  FROM monitor WHERE status='DIED' ",
  "Istance DIED!! "
);
```
