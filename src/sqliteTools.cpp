/*
 * sqliteTools.cpp
 *
 *  Created on: 04.12.2014
 *      Author: kaisers
 */

#include "sqliteTools.h"

extern "C"{

struct callback_data
{
	sqlite_stmt * stmt;
	unsigned int expand_start;	// = 3 + n_copy_columns
	unsigned int expand_end;	// = expand_start + n_expand - 1
};


int expand_callback(void *ptr, int nrows, char **column_value, char **column_name)
{
	callback_data *cd = (callback_data*) ptr;
	sqlite_stmt * stmt = cd->stmt;
	unsigned int i, n_expand;
	int index, lo_bound, hi_bound;

	ostream & os = stmt->get_con().getos();

	// SELECT id, min_woche, max_woche, cpy1, cpy2, exp1, exp2 FROM tbl;
	lo_bound = atoi(column_value[1]);
	hi_bound = atoi(column_value[2]);
	n_expand = hi_bound - lo_bound + 1;

	// INSERT INTO rtbl (id, rid, woche, cpy1, cpy2, exp1, exp2) VALUES (?, ?, ?, ?, ?, ?, ?)
	// 0: auto_id
	stmt->bind_int(1, stmt->getAutoId());		// id
	stmt->bind_int(2, atoi(column_value[0]));	// rid

	// Bind values for copied columns
	for(i = 3; i < cd->expand_start; ++i)
		stmt->bind_text(i + 1, column_value[i]);

	// Bind values for expanded columns
	for(i = cd->expand_start; i <= cd->expand_end; ++i)
		stmt->bind_double(i + 1, strtod(column_value[i], NULL) / n_expand);

	for(index = lo_bound; index <= hi_bound; ++index)
	{
		stmt->bind_int(1, stmt->getAutoId());
		stmt->bind_int(3, index);
		if(!stmt->step())
			os << "[expand_table.expand_callback] Step error!";
	}
	return 0;
}


void create_output_table(sqlite_con &con,
					string &write_table,
					string &index_column,
					list<string> &copyCols,
					list<string> &copyColTypes,
					list<string> &expandCols,
					bool verbose)
{

	stringstream sql;
	list<string>::const_iterator iter, iter1, iter2;
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Drop target table (if exists)
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	if(con.drop_table(write_table))
	{
		if(verbose)
			Rprintf("[expand_table] Drop table success.\n");
	}
	else
	{
		con.close();
		error("Drop table error!");
	}


	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Create target table
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	const char * delim = ", ";

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// SQL string for creation of output table
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	sql << "CREATE TABLE IF NOT EXISTS "	<< write_table << " (";
	sql << "id INTEGER "					<< delim;
	sql << "rid INTEGER"					<< delim;

	// Index column
	sql << index_column << " TEXT"			<< delim;

	// Names and types for columns which are copied
	iter1 = copyCols.begin();
	iter2 = copyColTypes.begin();

	for(; iter1!=copyCols.end(); ++iter1, ++iter2)
		sql << *iter1 << " " << *iter2 		<< delim;

	// Names for columns which are expanded
	for(iter1 = expandCols.begin(); iter1 != expandCols.end(); ++iter1)
		sql << *iter1 << " REAL"			<< delim;

	sql << "PRIMARY KEY(id));";

	// Print debug message
	if(verbose)
		Rprintf("[expand_table] SQL: '%s'\n", sql.str().c_str());

	if(con.create_table(sql.str()))
	{
		if(verbose)
			Rprintf("[expand_table] Create table success.\n");
	}
	else
	{
		con.close();
		error("[expand_table] Create table error!");
	}
	return;
}


void prepare_insert_statement(sqlite_stmt &stmt,
			string &read_table,
			string &write_table,
			string &index_column,
			list<string> &copyCols,
			list<string> expandCols,
			string &lo_bound_col,
			string &up_bound_col,
			bool verbose)
{
	stringstream sql;
	unsigned int nCopyCols = copyCols.size();
	unsigned int nExpandCols = expandCols.size();
	unsigned int i;
	list<string>::const_iterator iter;

	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Prepare INSERT statement
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

	sql << "INSERT INTO " << write_table << " (id, rid, ";

	sql << index_column;

	// Copied columns
	for(iter = copyCols.begin(); iter != copyCols.end(); ++iter)
		sql << ", " << *iter ;

	// Expanded columns
	for(iter = expandCols.begin(); iter != expandCols.end(); ++iter)
		sql << ", " << *iter;

	sql << ") VALUES (?, ?, ?"; // id, rid, index_column

	for(i = 0; i < nCopyCols; ++i)
		sql << ", ?";

	for(i = 0; i < nExpandCols; ++i)
		sql << ", ?";

	sql << ");";

	// Print debug message
	if(verbose)
		Rprintf("[expand_table] SQL: '%s'\n", sql.str().c_str());


	if(!stmt.prepare(sql.str()))
	{
		stmt.get_con().close();
		error("[expand_table] Prepare statement error!");
	}

	return;

}



SEXP expand_table(SEXP pParams, SEXP pCopyCol, SEXP pCopyColTypes,  SEXP pExpCol, SEXP pVerbose)
{
	if(TYPEOF(pParams) != STRSXP)
		error("pParams must be character!");

	if(length(pParams) != 6)
		error("pParams must have length 6!");

	if(TYPEOF(pCopyCol) != STRSXP)
		error("pCopyCol must be character");

	if(TYPEOF(pCopyColTypes) != STRSXP)
		error("pCopyColTypes must be character");

	if(TYPEOF(pExpCol) != STRSXP)
		error("pExpCol must be character!");

	if(TYPEOF(pVerbose) != INTSXP)
		error("pVerbose must be integer!");


	if(length(pCopyCol) != length(pCopyColTypes))
		error("pCopyCol and pCopyColTypes must have equal length!");


	int i, nCopyCols, nExpandCols;
	stringstream sql;
	list<string>::const_iterator iter, iter1, iter2;

	nCopyCols = length(pCopyCol);
	nExpandCols = length(pExpCol);

	if(!nCopyCols)
		error("pCopyCols must not be empty!");

	if(!nExpandCols)
		error("pExpandCols must not be empty!");


	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Provided parameters:
	// [0] database name
	// [1] read table name
	// [2] write table name
	// [3] lower bound column
	// [4] upper bound column
	// [5] index column			:	Name of column in which values will vary
	//								from lower bound to upper bound
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

	string db_file		= string(CHAR(STRING_ELT(pParams, 0)));
	string read_table	= string(CHAR(STRING_ELT(pParams, 1)));
	string write_table	= string(CHAR(STRING_ELT(pParams, 2)));
	string lo_bound_col	= string(CHAR(STRING_ELT(pParams, 3)));
	string up_bound_col	= string(CHAR(STRING_ELT(pParams, 4)));
	string index_column	= string(CHAR(STRING_ELT(pParams, 5)));

	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Table columns which will be copied
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	list<string> copyCols;
	for(i=0; i < nCopyCols; ++i)
		copyCols.push_back(string(CHAR(STRING_ELT(pCopyCol, i))));

	list<string> copyColTypes;
	for(i=0; i < nCopyCols; ++i)
		copyColTypes.push_back(string(CHAR(STRING_ELT(pCopyColTypes, i))));

	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Table columns which will be expanded
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	list<string> expandCols;
	for(i=0; i < nExpandCols; ++i)
		expandCols.push_back(string(CHAR(STRING_ELT(pExpCol, i))));

	// Controls verbosity of printed messages
	bool verbose = (bool) INTEGER(pVerbose)[0];


	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Open connection to database
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

	if(verbose)
		Rprintf("[expand_table] Opening Database\n");

	rostream ros;
	sqlite_con con(db_file, ros, verbose);

	if(!con.open())
		error("[expand_table] Could not open SQLite database '", db_file.c_str() , "'.\n");

	if(!con.set_sync(sqlite_con::SYNC_OFF))
	{
		con.close();
		error("[expand_table] Cannot set database to asynchronous state!");
	}

	sqlite_stmt stmt(con);

	create_output_table(con, write_table, index_column,
						copyCols, copyColTypes, expandCols,
						verbose);


	prepare_insert_statement(stmt, read_table, write_table,
						index_column, copyCols, expandCols,
						lo_bound_col, up_bound_col,
						verbose);


	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Create SELECT query
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	sql.str("");
	sql.clear();

	sql << "SELECT id, " << lo_bound_col << ", " << up_bound_col;

	// Copied columns
	for(iter = copyCols.begin(); iter != copyCols.end(); ++iter)
		sql << ", " << *iter;

	// Expanded columns
	for(iter = expandCols.begin(); iter != expandCols.end(); ++iter)
		sql << ", " << *iter;

	sql << " FROM " << read_table << ";";

	if(verbose)
		Rprintf("[expand_table] SQL: '%s'\n", sql.str().c_str());

	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Execute query and expand algorithm
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// See also:
	// http://stackoverflow.com/questions/1711631/improve-insert-per-second-performance-of-sqlite

	// Prepare struct which will be passed to callback function
	callback_data cd;
	cd.stmt = &stmt;
	cd.expand_start = 3 + nCopyCols;
	cd.expand_end = cd.expand_start + nExpandCols - 1;


	// ToDo: Check whether sync_off critically slows down execution
	con.set_sync(sqlite_con::SYNC_OFF);
	con.begin();
	con.exec_callback(sql.str(), expand_callback, &cd);
	con.commit();
	con.set_sync(sqlite_con::SYNC_FULL); // Default

	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Close database connection.
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
	if(verbose)
		Rprintf("[expand_table] Closing database.\n");

	// Resets sync and journalling mode
	if(con.close())
	{
		if(verbose)
			Rprintf("[expand_table] Finished.\n");
	}
	else
		error("Database closing error!");

	return R_NilValue;
}




} // extern "C"
