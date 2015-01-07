/*
 * sqlite_con.h
 *
 *  Created on: 28.02.2011
 *      Author: kaisers
 *
 *  uses: libsqlite3-dev
 */

#ifndef SQLITE_CON_H_
#define SQLITE_CON_H_

#include <string>
#include <sqlite3.h>
#include <ostream>
#include <iomanip>
#include <sstream>
#include <vector>
#include <time.h>
#include <stdlib.h>

using namespace std;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
// Namespace declaration
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
namespace sqlite {


// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
// Class declaration
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
class sqlite_con {
public:
	sqlite_con(const string &name, ostream &file_out, int verb=0);
	~sqlite_con();

	ostream & getos() const { return os_; }

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// C++ operators
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	sqlite_con& operator=(const sqlite_con& rhs);
	operator bool() { return con_status==CON_OPEN; }

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Database connection
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	bool open();
	bool close();

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Database tables
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	bool create_table(const string& sql);
	bool drop_table(const string& tablename);
	bool create_index(const string &indexname, const string &tablename, const char *colnames);

	unsigned long int insert_sql(const string& sql);
	unsigned long int get_max_id_val(const string &tablename);
	long get_count_value(const string &sql);

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Transactions
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	void begin();
	void commit();

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Callback function
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	bool exec_callback(const string & sql,  int (*callback)(void*, int, char**, char**), void *v);

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// friend class implements parameterized queries
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	friend class sqlite_stmt;

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Synchronous-status
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	bool set_sync(const int &SYNC_STATUS);
	int get_sync();
	static const int SYNC_OFF;
	static const int SYNC_NORMAL;
	static const int SYNC_FULL;

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Journaling mode
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	bool set_con_journal(const string & mode);
	static const string JRNL_DELETE;
	static const string JRNL_TRUNCATE;
	static const string JRNL_MEMORY;
	static const string JRNL_PERSIST;
	static const string JRNL_WAL;
	static const string JRNL_OFF;

private:

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Copy construct is not allowed because database connection
	// must be handled uniquely by one object
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	sqlite_con(const sqlite_con& rhs);

	sqlite3* db;
	sqlite3_stmt *stmt;

	string db_name;
	int con_status;		// db-connection status
	int com_status;		// commit status
	int result;
	stringstream sql;

	// connection status
	static const int CON_OPEN;
	static const int CON_CLOSED;
	static const int CON_ERROR;

	// commit status
	static const int COM_BEGIN;
	static const int COM_COMMITTED;

	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	// Printing messages
	// - - - - - - - - - - - - - - - - - - - - - - - - - //
	ostream &os_;
	int verbose;
	string sqlite_result(unsigned res);
};

// Static data
const int sqlite_con::CON_OPEN		=1;
const int sqlite_con::CON_CLOSED	=0;
const int sqlite_con::CON_ERROR		=-1;

const int sqlite_con::COM_BEGIN		=1;
const int sqlite_con::COM_COMMITTED	=0;

const int sqlite_con::SYNC_OFF=0;
const int sqlite_con::SYNC_NORMAL=1;
const int sqlite_con::SYNC_FULL=2;

const string sqlite_con::JRNL_DELETE=string("DELETE");
const string sqlite_con::JRNL_TRUNCATE=string("TRUNCATE");
const string sqlite_con::JRNL_MEMORY=string("MEMORY");
const string sqlite_con::JRNL_PERSIST=string("PERSIST");
const string sqlite_con::JRNL_WAL=string("WAL");
const string sqlite_con::JRNL_OFF=string("OFF");



sqlite_con::sqlite_con(const string &name, ostream &file_out, int verb):
		db(0), stmt(0), db_name(name),
		con_status(CON_CLOSED), com_status(COM_COMMITTED),
		os_(file_out), verbose(verb)
{
	os_.imbue(locale(""));
}

sqlite_con::~sqlite_con() {

	if(con_status==CON_OPEN)
	{
		if(com_status==COM_BEGIN)
			sqlite3_exec(db,"COMMIT",0,0,0);

		// Reset
		set_sync(sqlite_con::SYNC_FULL);
		set_con_journal(sqlite_con::JRNL_DELETE);
		sqlite3_close(db);
	}
	if(verbose)
		os_ << "[sqlite_con] Destructed.\n";
}


sqlite_con& sqlite_con::operator=(const sqlite_con& rhs)
{
	if(this != &rhs)
	{
		result=rhs.result;
		sql.str()=rhs.sql.str();
	}
	return *this;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Database connection

bool sqlite_con::open()
{
	result = sqlite3_open(db_name.c_str(), &db);
	if(result == SQLITE_OK)
	{
		con_status = CON_OPEN;
		if(verbose)
			os_ << "[sqlite_con] Connection opened.\n";

		return true;
	}else
	{
		if(verbose)
			os_ << "[sqlite_con]: Database connection could not be opened!\n";
	}
	return false;
}

bool sqlite_con::close()
{
	if(con_status==CON_OPEN)
	{
		// Reset
		set_sync(sqlite_con::SYNC_FULL);
		set_con_journal(sqlite_con::JRNL_DELETE);
		sqlite3_close(db);
		con_status=CON_CLOSED;

		if(verbose)
			os_ << "[sqlite_con] Database connection closed.\n";
		return true;
	}
	return false;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Administration

// Synchronous-status
bool sqlite_con::set_sync(const int &SYNC_STATUS)
{
	stringstream sql;
	sql << "PRAGMA synchronous=" << SYNC_STATUS << ";";
	result=sqlite3_exec(db,sql.str().c_str(),0,0,0);
	if(result!=SQLITE_OK)
	{
		os_ << "[sqlite_con] set_sync ERROR:" << sqlite_result(result) << endl;
		return false;
	}

	if(verbose)
		os_ << "[sqlite_con] Synchronous status: " << SYNC_STATUS << ".\n";

	return true;
}

bool sqlite_con::set_con_journal(const string & mode)
{
	stringstream sql;
	sql << "PRAGMA journal_mode= " << mode << ";";
	result=sqlite3_exec(db,sql.str().c_str(),0,0,0);
	if(result!=SQLITE_OK)
	{
		os_ << "[sqlite_con] set_con_journal ERROR: " << sqlite_result(result) << endl;
		return false;
	}
	if(verbose)
		os_ << "[sqlite_con] Journal mode: " << mode << "\n";

	return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Creation and drop of tables, creation of indexes, get_max_id_val

bool sqlite_con::create_table(const string& sql)
{
	if(con_status!=CON_OPEN)
	{
		os_ << "[sqlite_con] create_table ERROR: Database connection is closed!" << endl;
		return false;
	}

	result=sqlite3_prepare_v2(db,sql.c_str(),sql.length(),&stmt,0);
	if(result!=SQLITE_OK)
	{
		os_ << "[sqlite_con] Create_table (prepare) ERROR: " << sqlite_result(result) << "!\n";
		os_ << "[sqlite_con] SQL = '" << sql.c_str() << "'.\n";
		return false;
	}
	result=sqlite3_step(stmt);

	if(result!=SQLITE_DONE)
	{
		os_ << "[sqlite_con] Create_table (step) ERROR: " << sqlite_result(result) << "!\n";
		return false;
	}
	sqlite3_finalize(stmt);

	if(verbose)
		os_ << "[sqlite_con] Table creation success.\n";

	return true;
}

bool sqlite_con::drop_table(const string& tablename)
{
	sql.str("");
	sql << "DROP TABLE IF EXISTS " << tablename << ";";
	result=sqlite3_exec(db,sql.str().c_str(),0,0,0);

	if(result != SQLITE_OK)
	{
		if(verbose)
			os_ << "[sqlite_con] drop_table '" << tablename << "' ERROR: " << sqlite_result(result) << "!\n";

		return false;
	}

	if(verbose)
		os_ << "[sqlite_con] drop_table '" << tablename << "' success.\n";

	return true;
}

bool sqlite_con::create_index(const string &indexname, const string &tablename, const char *colnames)
{
	if(con_status != CON_OPEN)
	{
		os_ << "[sqlite_con] create_index ERROR: Database connection is closed!\n";
		os_ << "[sqlite_con] Table: '" << tablename << "\tIndex: '" << indexname << "'\n";
		return false;
	}

	stringstream sql;
	sql << "CREATE INDEX IF NOT EXISTS ";
	sql << indexname;
	sql << " on " << tablename;
	sql << " (" << colnames << ");";

	sqlite3_prepare_v2(db, sql.str().c_str(), -1, &stmt, 0);
	result = sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	if(result == SQLITE_DONE)
	{
		if(verbose)
			os_ << "[sqlite_con] Created index '" << indexname << "' on table '" << tablename << "'\n";
		return true;
	}

	os_ << "[sqlite_con] create_index ERROR: " << sqlite_result(result) << "!\n";
	os_ << "[sqlite_con] Indexname '" << indexname << "' on table '" << tablename << "'.\n";
	return false;
}

unsigned long int sqlite_con::get_max_id_val(const string &tablename)
{
	char **result_table = NULL;
	char *errmsg = NULL;
	int nrows = 0, ncols = 0;

	sql.str("");
	sql << "SELECT COALESCE(max(id),0) FROM " << tablename << ";";
	result=sqlite3_get_table(db, sql.str().c_str(), &result_table, &nrows, &ncols, &errmsg);

	if(result != SQLITE_OK)
	{
		os_ << "[sqlite_con] get_max_id_val ERROR on table '" << tablename << "': " << sqlite_result(result) << "\n";
		return 0;
	}

	// Convert string representation into integer
	std::stringstream sst;
	unsigned long max;
	sst << result_table[1];
	sst >> max;

	sqlite3_free_table(result_table);
	return max;
}

long sqlite_con::get_count_value(const string &sql)
{
	char **sql_result = NULL;
	char *errmsg = NULL;
	int nrows = 0, ncols = 0;

	result = sqlite3_get_table(db, sql.c_str(), &sql_result, &nrows, &ncols, &errmsg);
	if(result != SQLITE_OK)
	{
		os_ << "[sqlite_con] get_count_value ERROR: " << sqlite_result(result) << endl;
		os_ << errmsg << "\n";
		os_ << "sql: '" << sql << "'\n";
		return -1;
	}

	if( (nrows != 1) || (ncols != 1) )
	{
		os_ << "[sqlite_con] get_count_value ERROR: Wrong result dimension: nrows=" << nrows << ", ncols=" << ncols << endl;
		return -1;
	}

	long r=atoi(sql_result[1]);
	sqlite3_free_table(sql_result);
	return r;
}

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //
// Beginning, Committing and sql-based insert, callback-based extraction
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - //

void sqlite_con::begin()
{
	if(com_status==COM_COMMITTED)
	{
		os_ << "[sqlite_con] Begin Database transaction.\n";
		sqlite3_exec(db,"BEGIN",0,0,0);
		com_status=COM_BEGIN;
	}
}

void sqlite_con::commit()
{
	if(com_status==COM_BEGIN)
	{
		sqlite3_exec(db,"COMMIT",0,0,0);
		com_status=COM_COMMITTED;

		if(verbose)
			os_ << "[sqlite_con] Database transaction committed.\n";
	}
}

unsigned long int sqlite_con::insert_sql(const string& sql)
{
	if(con_status != CON_OPEN)
			return 0;

	sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, 0);
	result=sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	if(result==SQLITE_OK || result==SQLITE_DONE)
	{
		return sqlite3_last_insert_rowid(db);
	} else
	{
		os_ << "[sqlite_con] Database Insert ERROR: " << sqlite_result(result) << "\n";
		os_ << sql << "\n";
	}
	return 0;
}

bool sqlite_con::exec_callback(const string & sql,  int (*callback)(void*,int,char**,char**),void *v=0)
{
	char *exec_error=NULL;
	result=sqlite3_exec(db, sql.c_str(), callback, v, &exec_error);

	if(result != SQLITE_OK)
	{
		os_ << "[sqlite_con] exec_callback ERROR: " << exec_error << "\n";
		os_ << sql << "\n";
		return false;
	}
	sqlite3_free(exec_error);
	return true;
}

string sqlite_con::sqlite_result(unsigned res)
{
	switch(res)
	{
		case SQLITE_OK:
			return std::string("SQLITE_OK\t0\tSuccessful result");
		/* beginning-of-error-codes */
		case  SQLITE_ERROR:
			return std::string("SQLITE_ERROR\t1\tSQL error or missing database");
		case  SQLITE_INTERNAL:
			return std::string("SQLITE_INTERNAL\2\tInternal logic error in SQLite");
		case  SQLITE_PERM:
			return std::string("SQLITE_PERM\3\tAccess permission denied");
		case  SQLITE_ABORT:
			return std::string("SQLITE_ABORT\t4\tCallback routine requested an abort");
		case  SQLITE_BUSY:
			return std::string("SQLITE_BUSY\t5\tThe database file is locked");
		case  SQLITE_LOCKED:
			return std::string("SQLITE_LOCKED\t6\tA table in the database is locked");
		case  SQLITE_NOMEM:
			return std::string("SQLITE_NOMEM\t7\tA malloc() failed");
		case  SQLITE_READONLY:
			return std::string("SQLITE_NOMEM\t8\tAttempt to write a readonly database");
		case  SQLITE_INTERRUPT:
			return std::string("SQLITE_INTERRUPT\t9\tOperation terminated by sqlite3_interrupt()");
		case  SQLITE_IOERR:
			return std::string("SQLITE_IOERR\t10\tSome kind of disk I/O error occurred");
		case  SQLITE_CORRUPT:
			return std::string("SQLITE_CORRUPT\t11\tThe database disk image is malformed");
		case  SQLITE_NOTFOUND:
			return std::string("SQLITE_NOTFOUND\t12\tNOT USED. Table or record not found");
		case  SQLITE_FULL:
			return std::string("SQLITE_FULL\t13\tInsertion failed because database is full");
		case  SQLITE_CANTOPEN:
			return std::string("SQLITE_CANTOPEN\t14\tUnable to open the database file");
		case  SQLITE_PROTOCOL:
			return std::string("SQLITE_PROTOCOL\t15\tDatabase lock protocol error");
		case  SQLITE_EMPTY:
			return std::string("SQLITE_EMPTY\t16\tDatabase is empty");
		case  SQLITE_SCHEMA:
			return std::string("SQLITE_SCHEMA\t17\tThe database schema changed");
		case  SQLITE_TOOBIG:
			return std::string("SQLITE_TOOBIG\t18\tString or BLOB exceeds size limit");
		case  SQLITE_CONSTRAINT:
			return std::string("SQLITE_CONSTRAINT\t19\tAbort due to constraint violation");
		case  SQLITE_MISMATCH:
			return std::string("SQLITE_MISMATCH\t20\tData type mismatch");
		case  SQLITE_MISUSE:
			return std::string("SQLITE_MISUSE\t21\tLibrary used incorrectly");
		case  SQLITE_NOLFS:
			return std::string("SQLITE_NOLFS\t22\tUses OS features not supported on host");
		case  SQLITE_AUTH:
			return std::string("SQLITE_AUTH\t23\tAuthorization denied");
		case  SQLITE_FORMAT:
			return std::string("SQLITE_FORMAT\t24\tAuxiliary database format error");
		case  SQLITE_RANGE:
			return std::string("SQLITE_RANGE\t25\t2nd parameter to sqlite3_bind out of range");
		case  SQLITE_NOTADB:
			return std::string("SQLITE_NOTADB\t26\tFile opened that is not a database file");
		case  SQLITE_ROW:
			return std::string("SQLITE_ROW\t100\tsqlite3_step() has another row ready");
		case  SQLITE_DONE:
			return std::string("SQLITE_DONE\t101\tsqlite3_step() has finished executing");
		default:
			return std::string("Undefined sqlite3 message");
	}
}



} // namespace sqlite
#endif /* SQLITE_CON_H_ */
