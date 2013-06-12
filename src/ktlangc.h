/*************************************************************************************************
 * C language binding
 *                                                               Copyright (C) 2009-2012 FAL Labs
 * This file is part of Kyoto Cabinet.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/


#ifndef _KTLANGC_H                       /* duplication check */
#define _KTLANGC_H

#if defined(__cplusplus)
extern "C" {
#endif

#if !defined(__STDC_LIMIT_MACROS)
#define __STDC_LIMIT_MACROS  1           /**< enable limit macros for C++ */
#endif

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <locale.h>
#include <math.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <stdint.h>

/**
 * C wrapper of polymorphic database.
 */
typedef struct {
  void* db;                              /**< dummy member */
} KTDB;

typedef struct {
  void* cur;                             /**< dummy member */
} KTCUR;


KTDB* ktdbnew(void);
void ktdbdel(KTDB* db);
bool ktdbopen(KTDB* db, const char* host, int32_t port, double timeout);
bool ktdbclose(KTDB* db);


int64_t ktdbcount(KTDB* db);

KTCUR* get_cursor(KTDB* DB);
void ktcurdel(KTCUR* cur);

bool next(KTDB* db, KTCUR* cur, char **key, char **value);

bool ktadd(KTDB*db, const char * key, const char * value);
bool ktreplace(KTDB*db, const char * key, const char * value);
bool ktremove(KTDB*db, const char * key);
const char *ktgeterror(KTDB* db);





#if defined(__cplusplus)
}
#endif

#endif                                   /* duplication check */

/* END OF FILE */
