module ddb.utils;

static import std.conv;
import std.traits;

import ddb.db : isVariantN;

char[32] MD5toHex(T...)(in T data)
{
    import std.ascii : LetterCase;
    import std.digest.md : md5Of, toHexString;
    return md5Of(data).toHexString!(LetterCase.lower);
}

// workaround, because std.conv currently doesn't support VariantN
template _to(T)
{
    static if (isVariantN!T)
        T _to(S)(S value) @trusted { T t = value; return t; }
    else
        T _to(A...)(A args) @trusted { return std.conv.to!T(args); }
}

template isConvertible(T, S)
{
    static if (__traits(compiles, { S s; _to!T(s); }) || (isVariantN!T && T.allowed!S))
        enum isConvertible = true;
    else
        enum isConvertible = false;
}

template arrayDimensions(T : T[])
{
    static if (isArray!T && !isSomeString!T)
        enum arrayDimensions = arrayDimensions!T + 1;
    else
        enum arrayDimensions = 1;
}

template arrayDimensions(T)
{
        enum arrayDimensions = 0;
}

template multiArrayElemType(T : T[])
{
    static if (isArray!T && !isSomeString!T)
        alias multiArrayElemType!T multiArrayElemType;
    else
        alias T multiArrayElemType;
}

template multiArrayElemType(T)
{
    alias T multiArrayElemType;
}

static assert(arrayDimensions!(int) == 0);
static assert(arrayDimensions!(int[]) == 1);
static assert(arrayDimensions!(int[][]) == 2);
static assert(arrayDimensions!(int[][][]) == 3);
