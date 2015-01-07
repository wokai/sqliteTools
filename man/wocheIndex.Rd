\name{wocheIndex}
% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - %
% Alias
% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - %
\alias{wocheIndex}
\title{wocheIndex: Adds woche_index to SQLite database}
\description{Creates woche_index column and copies values from 
base tables (fbas or kbas).}
\usage{
wocheIndex(tbl, bas = "fbas", con)
}
\arguments{
  \item{tbl}{Name of table.}
  \item{bas}{character: 'fbas' or 'kbas'. Name of base table from which
    woche_index values are read.}
  \item{con}{SQLite database connection.}
}
\value{None.}
\author{W. Kaisers}
\examples{
#
}
\keyword{wocheIndex}
