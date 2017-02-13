///
module ddb.pg.resultset;

import ddb.db : DBRow;
import ddb.pg.exceptions;
import ddb.pg.messages : Message;
import ddb.pg.connection : PGConnection;
import ddb.pg.types : PGFields;

@safe:

version(Have_vibe_core) {
    import vibe.internal.freelistref : FreeListRef;
    alias PGResultSet(Specs...) = FreeListRef!(PGResultSetImpl!Specs);
} else {
    class PGResultSet(Specs...)
    {
	    static FreeListRef opCall(ARGS...)(ARGS args)
	    {
            return new PGResultSetImpl(args);
	    }
    }
}

/// Input range of DBRow!Specs
class PGResultSetImpl(Specs...)
{
    alias _Specs = Specs;
    alias Row = DBRow!Specs;
    alias FetchRowDelegate = Row delegate(ref Message msg, ref PGFields fields) @safe;

    private FetchRowDelegate fetchRow;
    private PGConnection conn;
    private PGFields fields;
    package(ddb) {
        Row row;
        bool validRow;
        Message nextMsg;
    }
    private size_t[][string] columnMap;

    this(PGConnection conn, ref PGFields fields, FetchRowDelegate dg)
    {
        this.conn = conn;
        this.fields = fields;
        this.fetchRow = dg;
        validRow = false;

        foreach (i, field; fields)
        {
            size_t[]* indices = field.name in columnMap;

            if (indices)
                *indices ~= i;
            else
                columnMap[field.name] = [i];
        }
    }

    ~this()
    {
        if (conn && conn.activeResultSet)
            close();

        import vibe.core.log;
        logDebug("ResultSet: close");
    }

    package(ddb.pg) size_t columnToIndex(string column, size_t index)
    {
        size_t[]* indices = column in columnMap;
        enforce(indices, "Unknown column name");
        return (*indices)[index];
    }

    pure nothrow bool empty()
    {
        return !validRow;
    }

    void popFront()
    {
        if (nextMsg.type == 'D')
        {
            row = fetchRow(nextMsg, fields);
            static if (!Row.hasStaticLength)
                row.columnToIndex = &columnToIndex;
            validRow = true;
            nextMsg = conn.getMessage();
        }
        else
            validRow = false;
    }

    pure nothrow Row front()
    {
        return row;
    }

    /// Closes current result set. It must be closed before issuing another query on the same connection.
    void close()
    {
        if (nextMsg.type != 'Z')
            conn.finalizeQuery();
        conn.activeResultSet = false;
    }

    int opApply(int delegate(ref Row row) @safe dg)
    {
        int result = 0;

        while (!empty)
        {
            result = dg(row);
            popFront;

            if (result)
                break;
        }

        return result;
    }

    int opApply(int delegate(ref size_t i, ref Row row) @safe dg)
    {
        int result = 0;
        size_t i;

        while (!empty)
        {
            result = dg(i, row);
            popFront;
            i++;

            if (result)
                break;
        }

        return result;
    }
}



