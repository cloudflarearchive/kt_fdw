/*-------------------------------------------------------------------------
 *
 *                foreign-data wrapper  kt
 *
 * Copyright (c) 2013, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author:  Andrew Dunstan <andrew@dunslane.net>
 *
 * IDENTIFICATION
 *                kt_fdw/=sql/kt_fdw.sql
 *
 *-------------------------------------------------------------------------
 */

CREATE FUNCTION kt_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION kt_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER kt_fdw
  HANDLER kt_fdw_handler
  VALIDATOR kt_fdw_validator;
