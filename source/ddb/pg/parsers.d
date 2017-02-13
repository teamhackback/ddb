module ddb.pg.parsers;

import std.conv : to;

import ddb.db : DBRow;
import ddb.pg.connection : PGConnection;
import ddb.pg.exceptions;
import ddb.pg.messages : Message;
import ddb.pg.types;

/**
Parses incoming responses from the Postgres Server
*/

@safe:

PGFields parseRowDescription(scope ref Message msg)
{
    PGField[] fields;
    short fieldCount;
    short formatCode;
    PGField fi;

    msg.read(fieldCount);

    fields.length = fieldCount;

    foreach (i; 0..fieldCount)
    {
        msg.readCString(fi.name);
        msg.read(fi.tableOid);
        msg.read(fi.index);
        msg.read(fi.oid);
        msg.read(fi.typlen);
        msg.read(fi.modifier);
        msg.read(formatCode);

        enforce(formatCode == 0 || formatCode == 1,
            new Exception("Field's format code returned in RowDescription is not 0 (text) or 1 (binary)"));

        fi.binaryMode = cast(bool) formatCode;

        fields[i] = fi;
    }
    return () @trusted { return cast(PGFields)fields; }();
}

auto parseDataRow(Result)(scope ref Message msg, Result result,
                                   scope ref PGFields fields, PGConnection conn)
{
    alias Row = Result.Row;
    result.row = conn.fetchRow!(Result._Specs)(msg, fields);
    static if (!Row.hasStaticLength)
        result.row.columnToIndex = &result.columnToIndex;
    result.validRow = true;
    result.nextMsg = conn.getMessage();

    conn.activeResultSet = true;
    return result;
}

void parseReadyForQuery(scope ref Message msg, PGConnection conn)
@trusted
{
    enforce(msg.data.length == 1);
    msg.read(cast(char)conn.trStatus);

    // check for validity
    with (TransactionStatus)
    switch (conn.trStatus)
    {
        case OutsideTransaction, InsideTransaction, InsideFailedTransaction: break;
        default: throw new Exception("Invalid transaction status: " ~ conn.trStatus);
    }
}

void parseCommandCompletion(scope ref Message msg, PGConnection conn, out uint oid, ref ulong rowsAffected)
{
    import std.string : indexOf, lastIndexOf;

    string tag;
    msg.readCString(tag);

    auto s1 = indexOf(tag, ' ');
    if (s1 >= 0) {
        switch (tag[0 .. s1]) {
            case "INSERT":
                // INSERT oid rows
                auto s2 = lastIndexOf(tag, ' ');
                assert(s2 > s1);
                oid = to!uint(tag[s1 + 1 .. s2]);
                rowsAffected = to!ulong(tag[s2 + 1 .. $]);
                break;
            case "DELETE", "UPDATE", "MOVE", "FETCH":
                // DELETE rows
                rowsAffected = to!ulong(tag[s1 + 1 .. $]);
                break;
            default:
                // CREATE TABLE
                break;
         }
    }
}
