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

struct Message
{
    PGConnection conn;
    char type;
    ubyte[] data;

    private size_t position = 0;

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

    T readComposite(T)()
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
                              "readBaseType!(fieldTypes[", i, "])(fieldOid, fieldLen)",
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
                    () @trusted { record[i] = readBaseType!(Record.ElemType)(fieldOid, fieldLen); }();
            }
        }

        return record.base;
    }
    mixin template elmnt(U : U[])
    {
        alias U ElemType;
    }
    private AT readDimension(AT)(int[] lengths, uint elementOid, int dim)
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
                array[i] = readDimension!ElemType(lengths, elementOid, dim + 1);
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
                    () @trusted { array[i] = readBaseType!E(elementOid, fieldLen); }();
            }
        }

        return array;
    }

    T readArray(T)()
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

        T array = readDimension!T(lengths, elementOid, 0);

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

    T readBaseType(T)(uint oid, int len = 0)
    {
        auto convError(T)()
        {
            string* type = oid in baseTypes;
            return new ConvException("Can't convert PostgreSQL's type " ~ (type ? *type : to!string(oid)) ~ " to " ~ T.stringof);
        }

        // for a PGType generate label and its corresponding array label parsing
        template genLabels(string pgtype, string dtype) {
            enum genLabels = "
               case " ~ pgtype ~ ":
                   static if (isConvertible!(T, " ~ dtype ~"))
                       return _to!T(read!(" ~ dtype ~ "));
                   else
                       throw convError!T();
               case _" ~ pgtype ~ ":
                   static if (isConvertible!(T, " ~ dtype ~ "[]))
                       return _to!T(readArray!(" ~ dtype ~ "[]));
                   else
                       throw convError!T();
            ";
        }

        import std.stdio;
        writefln("readBaseType oid: %s", oid);

        with (PGType)
        switch (oid)
        {
            // TODO: increases compilation time from 4s to 6s
            mixin(genLabels!("BOOLEAN", "bool"));
            mixin(genLabels!("INT2", "short"));
            mixin(genLabels!("INT4", "int"));
            mixin(genLabels!("INT8", "long"));
            mixin(genLabels!("FLOAT4", "float"));
            mixin(genLabels!("FLOAT8", "double"));
            mixin(genLabels!("UUID", "std.uuid.UUID"));
            mixin(genLabels!("CHAR", "char"));
            mixin(genLabels!("DATE", "Date"));
            mixin(genLabels!("TIME", "TimeOfDay"));
            mixin(genLabels!("TIMESTAMP", "DateTime"));
            mixin(genLabels!("TIMESTAMPTZ", "SysTime"));
            mixin(genLabels!("INTERVAL", "core.time.Duration"));
            mixin(genLabels!("TIMETZ", "SysTime"));
            mixin(genLabels!("POINT", "Point"));
            mixin(genLabels!("LSEG", "LSeg"));
            //mixin(genLabels!("PATH", "Path"));
            mixin(genLabels!("BOX", "Box"));
            //mixin(genLabels!("POLYGON", "Polygon"));
            mixin(genLabels!("LINE", "Line"));
            mixin(genLabels!("CIRCLE", "Circle"));

            // oid and reg*** aliases
            case OID, REGPROC, REGPROCEDURE, REGOPER, REGOPERATOR,
                 REGCLASS, REGTYPE, REGCONFIG, REGDICTIONARY:
                static if (isConvertible!(T, uint))
                    return _to!T(read!uint);
                else
                    throw convError!T();
            case BPCHAR, VARCHAR, TEXT, NAME, UNKNOWN: // bpchar, varchar, text, name, unknown
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
                    return () @trusted { return T(readComposite!(Variant[])); }();
                else
                    return readComposite!T;
            case _RECORD: // _record and other arrays
                static if (isArray!T && !isSomeString!T)
                    return readArray!T;
                else static if (isVariantN!T && T.allowed!(Variant[]))
                    return () @trusted { return T(readArray!(Variant[])); }();
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
$(LINK2 http://www.postgresql.org/docs/9.0/static/protocol-error-fields.html,here).
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
