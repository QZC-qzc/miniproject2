Part2:
# create database
CREATE KEYSPACE test4 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

# specify the database to use
use test4;

# create table
CREATE TABLE test4.access (id int primary key, ip text, date text , path text, status text);

# import data
COPY test4.access (id,date,ip,path,status) FROM '/home/ubuntu/access_log_new.csv' WITH HEADER = TRUE;

# show the imported data
select * from access;

Part3:
Q1:
SELECT COUNT (*) AS path_count FROM access where path = 'GET /assets/img/release-schedule-logo.png HTTP/1.1' ALLOW FILTERING;

Q2:
select count(*) as ip_count from access where ip = '10.207.188.188' ALLOW FILTERING;

For Q3 and Q4, we need three user defined functions:
CREATE OR REPLACE FUNCTION state_group_and_count( state map<text, int>, type text )
CALLED ON NULL INPUT
RETURNS map<text, int>
LANGUAGE java AS '
Integer count = (Integer) state.get(type);  if (count == null) count = 1; else count++; state.put(type, count); return state; ' ;

CREATE OR REPLACE FUNCTION maxII(state map<text, int>)
RETURNS NULL ON NULL INPUT
RETURNS map<text, int> LANGUAGE java AS
'
Map<String,Integer> map1 = new HashMap<String,Integer>();
int maxValueInMap=(Collections.max(state.values()));
for (String key : state.keySet()) {
	if (state.get(key)==maxValueInMap) {
		map1.put(key,maxValueInMap);
	}}
return map1;
';

CREATE OR REPLACE AGGREGATE groupcountmaxii(text)
SFUNC state_group_and_count
STYPE map<text, int>
FINALFUNC maxII
INITCOND {};

Q3:
select groupcountmaxii(path) from access;

Q4:
select groupcountmaxii(ip) from access;