module ddb.messages;

import std.ascii : LetterCase;
import std.bitmanip : bigEndianToNative;
import std.conv : ConvException, to, text;
import std.datetime;
import std.traits;
import std.variant : Variant;
import std.uuid : UUID;
static import std.uuid;

import ddb.db : DBRow, isNullable, isVariantN, nullableTarget;
import ddb.exceptions;
import ddb.pgconnection : PGConnection;
import ddb.types;
import ddb.utils;

//import ddb.db : Variant = SafeVariant;

@safe:

enum ResponseMessageTypes : char {
    RowDescription = 'T',
    DataRow = 'D',
    CommandComplete = 'C',
    EmptyQueryResponse = 'I',
    PortalSuspended = 's',
    ReadyForQuery = 'Z',
    ErrorResponse = 'E',
    BackendKeyData = 'K',
    ParameterStatus = 'S',
    AuthenticationXXXX = 'R',
    NoticeResponse = 'N',
    ParseComplete = '1',
    BindComplete = '2',
    CloseComplete = '3',
    NoData = 'n',
    NotificationResponse = 'A',

    // TODO:
    FunctionCallResponse = 'V',
    ParameterDescription = 't',
    CopyDone = 'd',
    CopyFail = 'f',
    CopyInResponse = 'G',
    CopyOutResponse = 'H',
    CopyBothResponse = 'W',
}

enum RequestMessageTypes : char {
    Parse = 'P',
    Password = 'p',
    Close = 'C',
    Terminate = 'X',
    Bind = 'B',
    Describe = 'D',
    Execute = 'E',
    Flush = 'H',
    Sync = 'S',
    Query = 'Q',
    // TODO:
    FunctionCall = 'F',
    CopyData = 'd',
}

struct Message
{
    PGConnection conn;
    char type; // ResponseMessageTypes
    ubyte[] data;

    //private size_t position = 0;
    size_t position = 0;

    T read(T, Params...)(Params p)
    {
        T value;
        read(value, p);
        return value;
    }

    void read()(out char x)
    {
        x = data[position++];
    }


    void read(Int)(out Int x) if((isIntegral!Int || isFloatingPoint!Int) && Int.sizeof > 1)
    {
        ubyte[Int.sizeof] buf;
        buf[] = data[position..position+Int.sizeof];
        x = bigEndianToNative!Int(buf);
        position += Int.sizeof;
    }

    // deserializes text representation
    T parse(T)(int len)
    {
        static if (is(T == bool))
        {
            return read!bool;
        }
        else
        {
            import ddb.formats : parseImpl;
            T x;
            parseImpl(x, readString(len));
            return x;
        }
    }

    string readCString()
    {
        string x;
        readCString(x);
        return x;
    }

    void readCString(out string x) @trusted
    {
        ubyte* p = data.ptr + position;

        while (*p > 0)
            p++;
        x = cast(string)data[position .. cast(size_t)(p - data.ptr)];
        position = cast(size_t)(p - data.ptr + 1);
    }

    string readString(int len)
    {
        string x;
        readString(x, len);
        return x;
    }

    void readString(out string x, int len) @trusted
    {
        x = cast(string)(data[position .. position + len]);
        position += len;
    }

    void read()(out bool x)
    {
        x = cast(bool)data[position++];
    }

    void read()(out ubyte[] x, int len)
    {
        enforce(position + len <= data.length);
        x = data[position .. position + len];
        position += len;
    }

    void read()(out UUID u) // uuid
    {
        ubyte[16] uuidData = data[position .. position + 16];
        position += 16;
        u = UUID(uuidData);
    }

    void read()(out Date x) // date
    {
        int days = read!int; // number of days since 1 Jan 2000
        x = PGEpochDate + dur!"days"(days);
    }

    void read()(out TimeOfDay x) // time
    {
        long usecs = read!long;
        x = PGEpochTime + dur!"usecs"(usecs);
    }

    void read()(out DateTime x) // timestamp
    {
        long usecs = read!long;
        x = PGEpochDateTime + dur!"usecs"(usecs);
    }

    void read()(out SysTime x) // timestamptz
    {
        long usecs = read!long;
        x = SysTime(PGEpochDateTime + dur!"usecs"(usecs), UTC());
        x.timezone = LocalTime();
    }

    // BUG: Does not support months
    void read()(out core.time.Duration x) // interval
    {
        long usecs = read!long;
        int days = read!int;
        int months = read!int;

        x = dur!"days"(days) + dur!"usecs"(usecs);
    }

    void read()(out Point p)
    {
        p = Point(read!double, read!double);
    }

    void read()(out LSeg lseg)
    {
        lseg = LSeg(read!double, read!double, read!double, read!double);
    }

    void read()(out Line line)
    {
        line = Line(read!double, read!double, read!double);
    }

    void read()(out Circle circle)
    {
        circle = Circle(read!double, read!double, read!double);
    }

    SysTime readTimeTz() // timetz
    {
        TimeOfDay time = read!TimeOfDay;
        int zone = read!int / 60; // originally in seconds, convert it to minutes
        Duration duration = dur!"minutes"(zone);
        auto stz = new immutable SimpleTimeZone(duration);
        return SysTime(DateTime(Date(0, 1, 1), time), stz);
    }

    T readComposite(T)(bool binaryMode)
    {
        alias Record = DBRow!T;

        static if (Record.hasStaticLength)
        {
            alias Record.fieldTypes fieldTypes;

            static string genFieldAssigns() // CTFE
            {
                string s = "";

                foreach (i; 0 .. fieldTypes.length)
                {
                    s ~= "read(fieldOid);\n";
                    s ~= "read(fieldLen);\n";
                    s ~= "if (fieldLen == -1)\n";
                    s ~= text("record.setNull!(", i, ");\n");
                    s ~= "else\n";
                    s ~= text("record.set!(fieldTypes[", i, "], ", i, ")(",
                              "readBaseType!(fieldTypes[", i, "])(fieldOid, binaryMode, fieldLen)",
                              ");\n");
                    // text() doesn't work with -inline option, CTFE bug
                }

                return s;
            }
        }

        Record record;

        int fieldCount, fieldLen;
        uint fieldOid;

        read(fieldCount);

        static if (Record.hasStaticLength)
            mixin(genFieldAssigns);
        else
        {
            record.setLength(fieldCount);

            foreach (i; 0 .. fieldCount)
            {
                read(fieldOid);
                read(fieldLen);

                if (fieldLen == -1)
                    record.setNull(i);
                else
                    () @trusted { record[i] = readBaseType!(Record.ElemType)(fieldOid, binaryMode, fieldLen); }();
            }
        }

        return record.base;
    }
    mixin template elmnt(U : U[])
    {
        alias U ElemType;
    }
    private AT readDimension(AT)(int[] lengths, uint elementOid, int dim, bool binaryMode)
    {

        mixin elmnt!AT;

        int length = lengths[dim];

        AT array;
        static if (isDynamicArray!AT)
            array.length = length;

        int fieldLen;

        foreach(i; 0 .. length)
        {
            static if (isArray!ElemType && !isSomeString!ElemType)
                array[i] = readDimension!ElemType(lengths, elementOid, dim + 1, binaryMode);
            else
            {
                static if (isNullable!ElemType)
                    alias nullableTarget!ElemType E;
                else
                    alias ElemType E;

                read(fieldLen);
                if (fieldLen == -1)
                {
                    static if (isNullable!ElemType || isSomeString!ElemType)
                        () @trusted { array[i] = null; }();
                    else
                        throw new Exception("Can't set NULL value to non nullable type");
                }
                else
                    () @trusted { array[i] = readBaseType!E(elementOid, binaryMode, fieldLen); }();
            }
        }

        return array;
    }

    T readArray(T)(bool binaryMode)
        if (isArray!T)
    {
        alias multiArrayElemType!T U;

        // todo: more validation, better lowerBounds support
        int dims, hasNulls;
        uint elementOid;
        int[] lengths, lowerBounds;

        read(dims);
        read(hasNulls); // 0 or 1
        read(elementOid);

        if (dims == 0)
            return T.init;

        enforce(arrayDimensions!T == dims, "Dimensions of arrays do not match");
        static if (!isNullable!U && !isSomeString!U)
            enforce(!hasNulls, "PostgreSQL returned NULLs but array elements are not Nullable");

        lengths.length = lowerBounds.length = dims;

        int elementCount = 1;

        foreach(i; 0 .. dims)
        {
            int len;

            read(len);
            read(lowerBounds[i]);
            lengths[i] = len;

            elementCount *= len;
        }

        T array = readDimension!T(lengths, elementOid, 0, binaryMode);

        return array;
    }

    T readEnum(T)(int len)
    {
        string genCases() // CTFE
        {
            string s;

            foreach (name; __traits(allMembers, T))
            {
                s ~= text(`case "`, name, `": return T.`, name, `;`);
            }

            return s;
        }

        string enumMember = readString(len);

        switch (enumMember)
        {
            mixin(genCases);
            default: throw new ConvException("Can't set enum value '" ~ enumMember ~ "' to enum type " ~ T.stringof);
        }
    }

    private static struct Pair { string pgtype, dtype; }

    // for a PGType generate label and its corresponding array label parsing
    private static string genLabels(Pair[] pairs) {
        // read: binary data
        // parse: plaintext data
        string s;
            foreach (pair; pairs) {
                with(pair)
                s ~= "case " ~ pgtype ~ ":
                    static if (isConvertible!(T, " ~ dtype ~"))
                        if (binaryMode)
                            return _to!T(read!(" ~ dtype ~ "));
                        else
                            return _to!T(parse!(" ~ dtype ~ ")(len));
                    else
                        throw convError!T();
                case _" ~ pgtype ~ ":
                    static if (isConvertible!(T, " ~ dtype ~ "[]))
                        return _to!T(readArray!(" ~ dtype ~ "[])(binaryMode));
                    else
                        throw convError!T(); ";
            }
        return s;
    }

    T readBaseType(T)(uint oid, bool binaryMode, int len = 0)
    {
        auto convError(T)()
        {
            string* type = oid in baseTypes;
            return new ConvException("Can't convert PostgreSQL's type " ~ (type ? *type : to!string(oid)) ~ " to " ~ T.stringof);
        }

        with (PGType)
        switch (oid)
        {
            // TODO: increases compilation time from 4s to 6s
            mixin(genLabels([
                Pair("BOOLEAN", "bool"),
                Pair("INT2", "short"),
                Pair("INT4", "int"),
                Pair("INT8", "long"),
                Pair("FLOAT4", "float"),
                Pair("FLOAT8", "double"),
                Pair("CHAR", "char"),
                Pair("DATE", "Date"),
                Pair("TIME", "TimeOfDay"),
                Pair("TIMESTAMP", "DateTime"),
                Pair("TIMESTAMPTZ", "SysTime"),
                Pair("INTERVAL", "core.time.Duration"),
                Pair("TIMETZ", "SysTime"),
                Pair("UUID", "std.uuid.UUID"),
                Pair("POINT", "Point"),
                Pair("LSEG", "LSeg"),
                Pair("BOX", "Box"),
                Pair("LINE", "Line"),
                Pair("CIRCLE", "Circle"),
                //mixin(genLabels!("POLYGON", "Polygon"),
                //mixin(genLabels!("PATH", "Path"));
            ]));

            // oid and reg*** aliases
            case OID, REGPROC, REGPROCEDURE, REGOPER, REGOPERATOR,
                 REGCLASS, REGTYPE, REGCONFIG, REGDICTIONARY:
                static if (isConvertible!(T, uint))
                    return _to!T(read!uint);
                else
                    throw convError!T();
            case BPCHAR, VARCHAR, TEXT, NAME, UNKNOWN:
                static if (isConvertible!(T, string))
                    return _to!T(readString(len));
                else
                    throw convError!T();
            case BYTEA:
                static if (isConvertible!(T, ubyte[]))
                    return _to!T(read!(ubyte[])(len));
                else
                    throw convError!T();
            case RECORD: // record and other composite types
                static if (isVariantN!T && T.allowed!(Variant[]))
                    return () @trusted { return T(readComposite!(Variant[])(binaryMode)); }();
                else
                    return readComposite!T(binaryMode);
            case _RECORD: // _record and other arrays
                static if (isArray!T && !isSomeString!T)
                    return readArray!T(binaryMode);
                else static if (isVariantN!T && T.allowed!(Variant[]))
                    return () @trusted { return T(readArray!(Variant[])(binaryMode)); }();
                else
                    throw convError!T();
            case JSON:
                static if (isConvertible!(T, string))
                    return _to!T(readString(len));
                else
                    throw convError!T();
            default:
                if (oid in conn.arrayTypes)
                    goto case _RECORD;
                else if (oid in conn.compositeTypes)
                    goto case RECORD;
                else if (oid in conn.enumTypes)
                {
                    static if (is(T == enum))
                        return readEnum!T(len);
                    else static if (isConvertible!(T, string))
                        return _to!T(readString(len));
                    else
                        throw convError!T();
                }
        }

        throw convError!T();
    }
}

@safe:

/**
Class encapsulating errors and notices.

This class provides access to fields of ErrorResponse and NoticeResponse
sent by the server. More information about these fields can be found
$(LINK2 http://www.postgresql.org/docs/9.6/static/protocol-error-fields.html,here).
*/
class ResponseMessage
{
    package(ddb) string[char] fields;

    private string getOptional(char type)
    {
        string* p = type in fields;
        return p ? *p : "";
    }

    /// Message fields
    @property string severity()
    {
        return fields['S'];
    }

    /// ditto
    @property string code()
    {
        return fields['C'];
    }

    /// ditto
    @property string message()
    {
        return fields['M'];
    }

    /// ditto
    @property string detail()
    {
        return getOptional('D');
    }

    /// ditto
    @property string hint()
    {
        return getOptional('H');
    }

    /// ditto
    @property string position()
    {
        return getOptional('P');
    }

    /// ditto
    @property string internalPosition()
    {
        return getOptional('p');
    }

    /// ditto
    @property string internalQuery()
    {
        return getOptional('q');
    }

    /// ditto
    @property string where()
    {
        return getOptional('W');
    }

    /// ditto
    @property string schemaName()
    {
        return getOptional('s');
    }

    /// ditto
    @property string tableName()
    {
        return getOptional('t');
    }

    /// ditto
    @property string columnName()
    {
        return getOptional('c');
    }

    /// ditto
    @property string dataTypeName()
    {
        return getOptional('d');
    }

    /// ditto
    @property string constraintName()
    {
        return getOptional('n');
    }

    /// ditto
    @property string file()
    {
        return getOptional('F');
    }

    /// ditto
    @property string line()
    {
        return getOptional('L');
    }

    /// ditto
    @property string routine()
    {
        return getOptional('R');
    }

    /**
    Returns summary of this message using the most common fields (severity,
    code, message, detail, hint)
    */
    override string toString()
    {
        string s = severity ~ ' ' ~ code ~ ": " ~ message;

        string* detail = 'D' in fields;
        if (detail)
            s ~= "\nDETAIL: " ~ *detail;

        string* hint = 'H' in fields;
        if (hint)
            s ~= "\nHINT: " ~ *hint;

        return s;
    }
}

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

import ddb.pgresultset : PGResultSet;

PGResultSet!Specs parseDataRow(Specs...)(scope ref Message msg, PGResultSet!Specs result,
                                   scope ref PGFields fields, PGConnection conn)
{
    alias Row = DBRow!Specs;
    result.row = conn.fetchRow!Specs(msg, fields);
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
