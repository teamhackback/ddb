///
module ddb.pg.parameters;

import std.algorithm.sorting : sort;
import std.variant : Variant;

import ddb.pg.stream : PGStream;
import ddb.pg.command : PGCommand;
import ddb.pg.types;
import ddb.pg.exceptions;

@safe:

/// Class representing single query parameter
class PGParameter
{
    private PGParameters params;
    immutable short index;
    immutable PGType type;
    private Variant _value;

    /// Value bound to this parameter
    @property Variant value() @trusted
    {
        return _value;
    }
    /// ditto
    @property Variant value(T)(T v)
    {
        params.changed = true;
        return _value = Variant(v);
    }

    package(ddb) this(PGParameters params, short index, PGType type)
    {
        enforce(index > 0, new ParamException("Parameter's index must be > 0"));
        this.params = params;
        this.index = index;
        this.type = type;
    }
}

/// Collection of query parameters
class PGParameters
{
    private PGParameter[short] params;
    private PGCommand cmd;
    package(ddb) bool changed;

    package(ddb) int[] getOids()
    {
        short[] keys = () @trusted { return params.keys; }();
        sort(keys);

        int[] oids = new int[params.length];

        foreach (int i, key; keys)
        {
            oids[i] = params[key].type;
        }

        return oids;
    }

    ///
    @property short length()
    {
        return cast(short)params.length;
    }

    package(ddb) this(PGCommand cmd)
    {
        this.cmd = cmd;
    }

    /**
    Creates and returns new parameter.
    Examples:
    ---
    // without spaces between $ and number
    auto cmd = new PGCommand(conn, "INSERT INTO users (name, surname) VALUES ($ 1, $ 2)");
    cmd.parameters.add(1, PGType.TEXT).value = "John";
    cmd.parameters.add(2, PGType.TEXT).value = "Doe";

    assert(cmd.executeNonQuery == 1);
    ---
    */
    PGParameter add(short index, PGType type)
    {
        enforce(!cmd.prepared, "Can't add parameter to prepared statement.");
        changed = true;
        return params[index] = new PGParameter(this, index, type);
    }

    PGParameters bind(T)(short index, PGType type, T value)
    {
        enforce(!cmd.prepared, "Can't add parameter to prepared statement.");
        changed = true;
        params[index] = new PGParameter(this, index, type);
        params[index].value = value;
        return this;
    }


    // todo: remove()

    PGParameter opIndex(short index)
    {
        return params[index];
    }

    int opApply(int delegate(ref PGParameter param) @safe dg)
    {
        int result = 0;

        foreach (number; sort(() @trusted { return params.keys; }()))
        {
            result = dg(params[number]);

            if (result)
                break;
        }

        return result;
    }

    // length of all params on the binary stream (in bytes)
    package(ddb) int calcLen(out bool hasText) @trusted
    {
        import ddb.pg.exceptions;
        import std.conv : to;

        int paramsLen;
        foreach (param; this)
        {
            enforce(param.value.hasValue, new ParamException("Parameter $" ~ to!string(param.index) ~ " value is not initialized"));

            void checkParam(T)(int len)
            {
                if (param.value != null)
                {
                    enforce(param.value.convertsTo!T, new ParamException("Parameter's value is not convertible to " ~ T.stringof));
                    paramsLen += len;
                }
            }

            with (PGType)
            /*final*/ switch (param.type)
            {
                case BOOLEAN:
                    checkParam!bool(1);
                    break;
                case INT2: checkParam!short(2); break;
                case INT4: checkParam!int(4); break;
                case INT8: checkParam!long(8); break;
                case FLOAT8: checkParam!double(8); break;

                //case BOOLEAN:
                //case TIMESTAMP:
                case INET:
                case NUMERIC:
                case JSONB:
                case INTERVAL:
                case VARCHAR:
                case TEXT:
                    paramsLen += param.value.coerce!string.length;
                    hasText = true;
                    break;
                case BYTEA:
                    paramsLen += param.value.length;
                    break;
                case JSON:
                    paramsLen += param.value.coerce!string.length; // TODO: object serialisation
                    break;
                case DATE:
                    paramsLen += 4; break;
                case TIMESTAMP:
                    paramsLen += 16; break;
                default:
                    assert(0, param.type.to!string ~ " Not implemented");
            }
        }
        return paramsLen;
    }

    package(ddb) void writeParams(scope PGStream stream) @trusted
    {
        import std.conv : to;
        import std.datetime;

        with (stream)
        foreach (param; params)
        {
            if (param.value == null)
            {
                write(-1);
                continue;
            }

            with (PGType)
            switch (param.type)
            {
                case BOOLEAN:
                    write(cast(bool) 1);
                    write(param.value.get!bool);
                    break;
                case INT2:
                    write(cast(int)2);
                    write(param.value.get!short);
                    break;
                case INT4:
                    write(cast(int)4);
                    write(param.value.get!int);
                    break;
                case INT8:
                    write(cast(int)8);
                    write(param.value.get!long);
                    break;
                case FLOAT8:
                    write(cast(int)8);
                    write(param.value.get!double);
                    break;

                case POINT:
                    write(cast(int)16);
                    auto p = param.value.get!Point;
                    write(p.x);
                    write(p.y);
                    break;

                case LINE, CIRCLE:
                    write(cast(int)24);
                    auto p = param.value.get!Circle;
                    write(p.x);
                    write(p.y);
                    write(p.r);
                    break;

                case LSEG, BOX:
                    write(cast(int)32);
                    auto p = param.value.get!Box;
                    write(p.x1);
                    write(p.y1);
                    write(p.x2);
                    write(p.y2);
                    break;

                //case BOOLEAN:
                //case TIMESTAMP:
                case INET:
                case NUMERIC:
                case JSONB:
                case INTERVAL:
                case VARCHAR:
                case TEXT:
                    auto s = param.value.coerce!string;
                    write(cast(int) s.length);
                    write(cast(ubyte[]) s);
                    break;
                case BYTEA:
                    auto s = param.value;
                    write(cast(int) s.length);

                    ubyte[] x;
                    x.length = s.length;
                    for (int i = 0; i < x.length; i++) {
                        x[i] = s[i].get!(ubyte);
                    }
                    write(x);
                    break;
                case JSON:
                    auto s = param.value.coerce!string;
                    write(cast(int) s.length);
                    write(cast(ubyte[]) s);
                    break;
                case DATE:
                    write(cast(int) 4);
                    write(Date.fromISOString(param.value.coerce!string));
                    break;
               case TIMESTAMP:
                    write(cast(int) 8);
                    auto t = cast(DateTime) Clock.currTime(UTC());
                    write(t);
                    break;
                default:
                    assert(0, param.type.to!string ~ " Not implemented");
            }
        }
    }

}
