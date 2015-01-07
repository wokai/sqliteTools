\name{convertToNum}
% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - %
% Alias
% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - %
\alias{convertToNum}
\title{convertToNum converts SQLite database columns from charater to numeric}
\description{The function first replaces ',' by '.' and then converts using
'as.numeri'}
\usage{
convertToNum(dbcon, tbl, column)
}
\arguments{
  \item{dbcon}{SQlit connection}
  \item{tbl}{table name}
  \item{column}{Name of table columns}
}
\value{None.}
\author{W. Kaisers}
\examples{
#
}
\keyword{convertToNum}
