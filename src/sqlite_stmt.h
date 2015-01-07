/*
 * sqlite_stmt.h
 *
 *  Created on: 31.03.2011
 *      Author: kaisers
 */

#ifndef SQLITE_STMT_H_
#define SQLITE_STMT_H_

#include "sqlite_con.h"
#include <cstring>
#include <string>
#include <ostream>
#include <vector>

using namespace std;

namespace sqlite {

class sqlite_stmt {
public:
	sqlite_stmt(sqlite_con &c) : con(c), stmt(0), stmt_status(STMT_UNPREP), result(0), auto_id(0) {}
	sqlite_stmt(const sqlite_stmt &s);
	~sqlite_stmt();

	sqlite_stmt& operator=(const sqlite_stmt &s);
	sqlite_con & get_con() const { return con; }

	// can be as auto incrementing id.
	// Does *NOT* garantee no-reuse!
	unsigned long getAutoId() { return ++auto_id; }
	void setAutoId(unsigned long id) { auto_id = id; }
	bool prepare(const string &sql);

	///////////////////////////////////////////////////////////////////////////////////////////////
	// Inline definition of bind functions
	bool bind_int(unsigned pos, const unsigned long int &value)
	{
		if(!con)
			return false;

		if(stmt_status == STMT_FINALIZED)
		{
			con.os_ << "[sqlite_stmt] bind_int ERROR: Statement is FINALIZED!\n";
			return false;
		}

		result = sqlite3_bind_int64(stmt,pos,value);
		if(result != SQLITE_OK)
		{
			con.os_ << "[sqlite_stmt] bind_int ERROR: " << con.sqlite_result(result) << "\n";
			return false;
		}
		return true;
	}

	bool bind_double(unsigned pos, const double &value)
	{
		if(!con)
			return false;
		if(stmt_status==STMT_FINALIZED)
		{
			con.os_ << "[sqlite_stmt] bind_double ERROR: Statement is FINALIZED!\n";
			return false;
		}

		result=sqlite3_bind_double(stmt,pos,value);
		if(result!=SQLITE_OK)
		{
			con.os_ << "[sqlite_stmt] bind_double ERROR: " << con.sqlite_result(result) << "\n";
			return false;
		}
		return true;
	}


	bool bind_text(const unsigned &pos, const string &text)
	{
		if(!con)
			return false;

		if(stmt_status == STMT_FINALIZED)
		{
			con.os_ << "[sqlite_stmt] bind_text ERROR: Statement is FINALIZED!\n";
			return false;
		}

		result = sqlite3_bind_text(stmt, pos, text.c_str(), text.size(), SQLITE_TRANSIENT);
		if(result!=SQLITE_OK)
		{
			con.os_ << "[sqlite_stmt] bind_text ERROR: " << con.sqlite_result(result) << "\n";
			return false;
		}
		return true;
	}
	bool bind_text(const unsigned &pos, const char *text)
	{
		if(!con)
			return false;

		if(stmt_status == STMT_FINALIZED)
		{
			con.os_ << "[sqlite_stmt] bind_text ERROR: stmt_status=STMT_FINALIZED!\n";
			return false;
		}

		result=sqlite3_bind_text(stmt,pos,text,strlen(text),SQLITE_TRANSIENT);
		if(result!=SQLITE_OK)
		{
			con.os_ << "[sqlite_stmt] bind_text ERROR: " << con.sqlite_result(result) << "\n";
			return false;
		}
		return true;
	}



	bool step();
	bool step(const unsigned &pos, const vector<unsigned long int> &v);
	bool finalize();
	friend class align_con;

private:
	sqlite_con &con;
	sqlite3_stmt *stmt;
	string sql;
	int stmt_status;
	int result;
	unsigned long int auto_id;

	static const int STMT_UNPREP;
	static const int STMT_PREPARED;
	static const int STMT_FINALIZED;
};

const int sqlite_stmt::STMT_UNPREP = 1;
const int sqlite_stmt::STMT_PREPARED = 2;
const int sqlite_stmt::STMT_FINALIZED = 3;

sqlite_stmt::sqlite_stmt(const sqlite_stmt &rhs) : con(rhs.get_con()), result(0), auto_id(0)
{
	if(!con)
	{
		stmt_status = sqlite_stmt::STMT_FINALIZED;
		stmt=0;
		con.os_ << "[sqlite_stmt] copy_constructor error: connection is not open!\n";
	}
	else
	{
		stmt_status = rhs.stmt_status;
		if(rhs.stmt_status == STMT_UNPREP)
		{
			stmt=0;
		}
		else
		{
			sql = rhs.sql;
			result = sqlite3_prepare_v2(con.db, sql.c_str(), sql.length(), &stmt, 0);
			if(result == SQLITE_OK)
			{
				stmt_status = STMT_PREPARED;
			}
			else
			{
				con.os_ << "[sqlite_stmt] copy_constructor prepare error: " << con.sqlite_result(result) << "\n";
				con.os_ << "sql: " << sql << "\n";
				result = sqlite3_finalize(stmt);
				stmt = 0;
				stmt_status = STMT_FINALIZED;
			}
		}
	}
}

sqlite_stmt& sqlite_stmt::operator=(const sqlite_stmt &rhs)
{
	if(this == &rhs)
		return *this;

	sql=rhs.sql;
	stmt_status=rhs.stmt_status;
	con=rhs.get_con();

	if(stmt!=0)
	{
		result=sqlite3_finalize(stmt);
		stmt=0;
	}

	if(!con)
	{
		stmt_status = STMT_FINALIZED;
		con.os_ << "[sqlite_stmt] operator=: sqlite connection is not open!\n";
		return *this;
	}

	if(stmt_status == STMT_PREPARED)
	{
		sql=rhs.sql;
		result=sqlite3_prepare_v2(con.db,sql.c_str(),sql.length(),&stmt,0);
		if(result == SQLITE_OK)
		{
	       stmt_status=STMT_PREPARED;

		}else
		{
			con.os_ << "[sqlite_stmt] operator= prepare error: " << con.sqlite_result(result) << "\n";
			con.os_ << "sql: " << sql << "\n";
	        result = sqlite3_finalize(stmt);
	        stmt = 0;
	        stmt_status = STMT_FINALIZED;
		}
	}
	return *this;
}



sqlite_stmt::~sqlite_stmt()
{
	if(stmt_status!=STMT_FINALIZED)
        sqlite3_finalize(stmt);
}

bool sqlite_stmt::prepare(const string &sql_txt)
{
	if(!con)
		return false;

	if(stmt_status == STMT_FINALIZED)
	{
		con.os_ << "[sqlite_stmt] prepare error: stmt_status=STMT_FINALIZED!\n";
		return false;
	}

	result = sqlite3_prepare_v2(con.db,sql_txt.c_str(),sql_txt.length(),&stmt,0);
	if(result == SQLITE_OK)
	{
       stmt_status=STMT_PREPARED;
       return true;
	}else
	{
		con.os_ << "[sqlite_stmt] prepare error: " << con.sqlite_result(result) << "\n";
		con.os_ << "sql: " << sql_txt << "\n";
        result = sqlite3_finalize(stmt);
        stmt = 0;
        stmt_status = STMT_FINALIZED;
	}
    return false;
}

bool sqlite_stmt::step()
{
	if(!con)
		return false;

	if(stmt_status != STMT_PREPARED)
    {
		con.os_ << "[sqlite_stmt] step NOT EXECUTED because stmt_status!=STMT_PREPARED!\n";
		return false;
    }
	result = sqlite3_step(stmt);
	if(result != SQLITE_DONE)
	{
		con.os_ << "[sqlite_stmt] step error: " << con.sqlite_result(result) << "\n";
		return false;
	}
	result = sqlite3_reset(stmt);
	if(result != SQLITE_OK)
	{
		con.os_ << "[sqlite_stmt] step reset error: " << con.sqlite_result(result) << "\n";
		return false;
	}
	return true;
}

bool sqlite_stmt::step(const unsigned &pos, const vector<unsigned long int> &v)
{
	if( (stmt==0) || (stmt_status != STMT_PREPARED) )
	{
		con.os_ << "[sqlite_stmt] step(vector) ERROR: stmt==0 or stmt_status!=STMT_PREPARED!\n";
		return false;
	}

	unsigned long int i;
	for(i=0; i<v.size(); ++i)
	{
		result = sqlite3_bind_int64(stmt, pos, v[i]);
		if(result != SQLITE_OK)
		{
			con.os_ << "[sqlite_stmt] step(vector) bind ERROR i=" << i << " and v[i]=" << v[i] << ": " << con.sqlite_result(result) << "\n";
			return false;
		}

		result = sqlite3_step(stmt);
		if(result != SQLITE_DONE)
		{
			con.os_ << "[sqlite_stmt] step(vector) step ERROR i=" << i << " and v[i]=" << v[i] << ": " << con.sqlite_result(result) << "\n";
			return false;
		}

		result = sqlite3_reset(stmt);
		if(result != SQLITE_OK)
		{
			con.os_ << "[sqlite_stmt] step(vector) reset ERROR i=" << i << " and v[i]=" << v[i] << ": " << con.sqlite_result(result) << "\n";
			return false;
		}
	}
	return true;
}


bool sqlite_stmt::finalize()
{
	if(!stmt)
		return true;

	if(stmt_status != STMT_FINALIZED)
    {
        result = sqlite3_finalize(stmt);
        if(result != SQLITE_OK)
        {
			con.os_ << "[sqlite_stmt] Finalize ERROR: " << con.sqlite_result(result) << "\n";
			return false;
        }

        stmt = 0;
        stmt_status = STMT_FINALIZED;
    }
    return true;
}


} // namespace sqlite
#endif /* SQLITE_STMT_H_ */
