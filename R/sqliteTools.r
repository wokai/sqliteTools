

.onUnload <- function(libpath) { library.dynam.unload("sqliteTools", libpath) }


expandTable <- function(dbfile, 
    tables, boundCols, indexCol, copyCols, expandCols, verbose=FALSE)
{
    if(!is.character(dbfile))
        stop("dbfile must be character")
    
    if(length(dbfile) != 1)
        stop("dbfile must have length 1")
    
    if(!file.exists(dbfile))
        stop("Database file does not exist!")
    
    if(!is.character(tables))
        stop("tables must be character")
    
    if(length(tables) != 2)
        stop("tables must have length 2 (inTable and outTable)")
    
    if(!is.character(boundCols))
        stop("boundCols must be character")
    
    if(length(boundCols) != 2)
        stop("boundCols must have length 2 (lowerBound and upperBound)!")
    
    if(!is.character(indexCol))
        stop("indexCol must be character")
    
    if(length(indexCol) == 0)
        stop("indexCol must not be empty")
    
    if(!is.character(copyCols))
        stop("copyCols must be character!")
    
    if(length(copyCols) == 0)
        stop("copyCols must not be empty!")
    
    if(!is.character(expandCols))
        stop("expandCols must be character!")
    
    if(length(expandCols) == 0)
        stop("expandCols must not be empty!")
    
    if(!is.integer(verbose))
        verbose <- as.integer(verbose)
    
    con <- dbConnect(RSQLite::SQLite(), dbfile)
    inputTable <- tables[1]
    outputTable <- tables[2]
    
    
    if(any(table(copyCols)) > 1)
        stop("copyCols must be unique!")
    
    if(any(table(expandCols)) > 1)
        stop("expandCols must be unique!")
    
    tbl <- dbListTables(con)
    mtc <- match(inputTable, tbl)
    if(any(is.na(mtc)))
        stop("inputTable '", inputTable, "' does not exist!", sep="")
    
    sql <- paste("SELECT * FROM", inputTable, "LIMIT 1;")
    res <- dbGetQuery(con, sql)
    
    colnames <- c("id", copyCols, expandCols)
    mtc <- match(colnames, names(res))
    if(any(is.na(mtc)))
    {
        message("[repTable] Missing columns in inputTable:")
        cat(paste(colnames[is.na(mtc)], collapse="\n"))
        cat("\n")
        stop("ERROR!")
    }
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # Translate r_types (character, ...) for copied columns
    # into SQLite types for creation of output table
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    mtc <- match(copyCols, names(res))
    copyColTypes <- unlist(lapply(res[, mtc], dbDataType, dbObj=con))
    
    
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    # Provided parameters:
    # [0] database name
    # [1] read table name
    # [2] write table name
    # [3] lower bound column
    # [4] upper bound column
    # [5] index column          :   Name of column in which values will vary
    #                               lower bound to upper bound
    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
    params <- c(
        path.expand(dbfile),
        inputTable,     # read table
        outputTable,    # write table
        boundCols[1],   # loBoundCol
        boundCols[2],   # hiBoundCol
        indexCol
    )
    
    # (pParams, pCopyCol, pCopyColTypes,  pExpCol, pVerbose)
    .Call("expand_table", params, copyCols, copyColTypes,
            expandCols, verbose, PACKAGE="sqliteTools")
    return(invisible())
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# 3) It turned out, that some 'kosten' actually are stored as character
# inside SQLite (for format reasons: 13,21 instead of 13.21)
# They have to be converted.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

convertToNum <- function(dbcon, tbl, column)
{
    dfr <- dbReadTable(dbcon, tbl)
    
    for(cl in column)
        dfr[, cl] <- as.numeric(sub(",",".", dfr[, cl]))
    
    dbWriteTable(dbcon, tbl, dfr, overwrite=TRUE, row.names=FALSE)
}

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
# Add woche_index column
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #

wocheIndex <- function(tbl, bas="fbas", con)
{
    btb <- dbReadTable(con, bas)
    n <- length(tbl)
    for(i in 1:n)
    {
        dt <- dbReadTable(con, tbl[i])
        mtc <- match(dt$vers_id, btb$vers_id)
        dt$woche_index <- btb$woche_index[mtc]
        dbWriteTable(con, tbl[i], dt, overwrite=TRUE, row.names=FALSE)
    }
}
