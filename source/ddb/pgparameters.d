module ddb.pgparameters;

import std.algorithm.sorting : sort;
import std.variant : Variant;

import ddb.pgcommand : PGCommand;
import ddb.types;
import ddb.exceptions;

/// Class representing single query parameter
class PGParameter
{
    private PGParameters params;
    immutable short index;
    immutable PGType type;
    private Variant _value;

    /// Value bound to this parameter
    @property Variant value()
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
        short[] keys = params.keys;
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

    // todo: remove()

    PGParameter opIndex(short index)
    {
        return params[index];
    }

    int opApply(int delegate(ref PGParameter param) dg)
    {
        int result = 0;

        foreach (number; sort(params.keys))
        {
            result = dg(params[number]);

            if (result)
                break;
        }

        return result;
    }
}


