# Kyoto Tycoon Foreign Data Wrapper for PostgreSQL

Thanks to the Blackhole Foreign Data Wrapper for PostgreSQL for providing
a good template of how to build FDWs for PostgreSQL.

to build:
make
(sudo) make install

to test:
make installcheck

A note about transactional support:
This FDW supports transactions of Kyoto Tycoon is compiled with LUA support.
To compile with LUA use ./configure --enable-lua when compiling Kyoto Tycoon.
Furthermore the ktserver has to be started with tranactions.lua loaded:

./ktserver -scr <fdw directory>/transactions.lua

If you want to disable transactions remove -DUSE_TRANSACTIONS from the makefile.


Usage:


CREATE SERVER <server name> FOREIGN DATA WRAPPER kt_fdw OPTIONS
(host '127.0.0.1', port '1978', timeout '-1');
(the above options are the defaults)

CREATE USER MAPPING FOR PUBLIC SERVER kt_server;

CREATE FOREIGN TABLE <table name> (key TEXT, value TEXT) SERVER <server name>;

Now you can Select, Update, Delete and Insert!
