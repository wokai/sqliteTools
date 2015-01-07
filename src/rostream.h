#ifndef ROSTREAM_H_
#define ROSTREAM_H_

/*
 * rostream.h
 *
 *  Created on: 25.05.2011
 *      Author: wolfgang
 */

#include <Rinternals.h>
#include <string>
#include <sstream>
using namespace std;

template<typename charT,typename traits=char_traits<charT> >
class rbuf: public basic_stringbuf<charT,traits>
{
public:
	rbuf() {}
	virtual ~rbuf() { sync(); }
protected:
	int sync()
	{
		Rprintf("%s",this->str().c_str());
		this->str(basic_string<charT>());	// clear string buffer
		return 0;
	}
};

template<typename charT,typename traits=char_traits<charT> >
class basic_rostream: public basic_ostream<charT,traits>
{
public:
	basic_rostream():basic_ostream<charT,traits>(new rbuf<charT,traits>()) {}
	virtual ~basic_rostream(){ delete this->rdbuf(); }
};

typedef basic_rostream<char> rostream;

#endif /* ROSTREAM_H_ */
