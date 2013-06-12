# Kyoto Tycoon Foreign Data Wrapper for PostgreSQL

Based on Blackhole Foreign Data Wrapper for PostgreSQL

to build:
make
(sudo) make install

to test:
make installcheck

Usage:


CREATE SERVER <server name> FOREIGN DATA WRAPPER kt_fdw OPTIONS
(host '127.0.0.1', port '1978', timeout '-1');
(the above options are the defaults)

CREATE USER MAPPING FOR PUBLIC SERVER kt_server;

CREATE FOREIGN TABLE <table name> (key TEXT, value TEXT) SERVER <server name>;

Now you can Select, Update, Delete and Insert!
