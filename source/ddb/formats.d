module ddb.formats;

import std.conv;
import std.datetime;
import std.traits;

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
