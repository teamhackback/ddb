module ddb.pg.formats;

import std.conv;
import std.datetime;
import std.traits;
import std.uuid : UUID;

import ddb.pg.types;

/**
This modules handles plain text responses from Postgres
*/

@safe:

void parseImpl(Int)(out Int x, string s)
    if((isIntegral!Int || isFloatingPoint!Int) && Int.sizeof > 1
         || isSomeChar!Int)
{
    x = s.to!Int;
}

void parseImpl(out Date x, string s)
{
    x = Date.fromISOString(s);
}

void parseImpl(out SysTime x, string s)
{
    import std.string : translate;
    enum dchar[dchar] trTable = [' ': 'T'];
    x = SysTime.fromISOExtString(s.translate(trTable));
}

void parseImpl(out TimeOfDay x, string s)
{
    assert(0, "Not implemented");
}

void parseImpl(out core.time.Duration x, string s)
{
    assert(0, "Not implemented");
}

void parseImpl(out DateTime x, string s)
{
    x = DateTime.fromISOString(s);
}

void parseImpl(out UUID x, string s)
{
    x = s.to!UUID;
}

void parseImpl(out Circle x, string s)
{
    assert(0, "Not implemented");
}

void parseImpl(out Line x, string s)
{
    assert(0, "Not implemented");
}

void parseImpl(out Box x, string s)
{
    assert(0, "Not implemented");
}

void parseImpl(out Point x, string s)
{
    assert(0, "Not implemented");
}
