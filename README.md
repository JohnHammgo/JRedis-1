# JRedis
A humble attempt to duplicate some of the basic functionalities of the popular in-memory database Redis in Java. The complexity of command executions are comparable to that of the actual Redis.

The supported commands are :

GET
SET
GETBIT
SETBIT
ZADD - Multiple [score member] inserts functionality is not supported
ZCARD
ZCOUNT - Min and Max are assumed to be always inclusive
ZRANGE
SAVE

--Running JRedis

i) Build the project using Maven.
ii) 'cd' to the target folder.
iii) Type "java -jar < jar file name > < full path of database>".

--Connecting to JRedis
i) Open a terminal and connect to port 4324 using telnet as "telnet localhost 4324".
ii) Now you are ready to execute commands.

Hope you enjoy!
Thanks.
