module ddb.exceptions;

import ddb.messages : ResponseMessage;
import std.exception;
public import std.exception : enforce;

class ParamException : Exception
{
    this(string msg, string fn = __FILE__, size_t ln = __LINE__) @safe pure nothrow
    {
        super(msg, fn, ln);
    }
}

/// Exception thrown on server error
class ServerErrorException: Exception
{
    /// Contains information about this _error. Aliased to this.
    ResponseMessage error;
    alias error this;

    this(string msg, string fn = __FILE__, size_t ln = __LINE__) @safe pure nothrow
    {
        super(msg, fn, ln);
    }

    this(ResponseMessage error, string fn = __FILE__, size_t ln = __LINE__)
    {
        super(error.toString(), fn, ln);
        this.error = error;
    }
}
