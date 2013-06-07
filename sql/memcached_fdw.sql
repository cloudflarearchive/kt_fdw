/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper  memcached
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Andrew Dunstan <andrew@dunslane.net>
 *
 * IDENTIFICATION
 *                memcached_fdw/=sql/memcached_fdw.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION memcached_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION memcached_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER memcached_fdw
  HANDLER memcached_fdw_handler
  VALIDATOR memcached_fdw_validator;
