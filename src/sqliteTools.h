/*
 * hipr.h
 *
 *  Created on: 04.12.2014
 *      Author: kaisers
 */

#ifndef SQLITETOOLS_H_
#define SQLITETOOLS_H_

#include <sqlite3.h>
#include "sqlite_con.h"
#include "sqlite_stmt.h"
using namespace sqlite;

#include "rostream.h"

#include <iostream>
#include <sstream>
#include <list>
using namespace std;

#include <cstdlib>

#include <R.h>
#include <Rinternals.h>
#include <Rdefines.h>
#include <R_ext/PrtUtil.h>
#include <R_ext/Rdynload.h> // DllInfo


extern "C" {
SEXP expand_table(SEXP pParams, SEXP pCopyCol, SEXP pCopyColTypes,  SEXP pExpCol, SEXP pVerbose);
}



#endif /* SQLITETOOLS_H_ */
