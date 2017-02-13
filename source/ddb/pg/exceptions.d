///
module ddb.pg.exceptions;

import ddb.pg.messages : ResponseMessage;
import std.exception;
public import std.exception : enforce;

@safe:

class ParamException : Exception
{
    this(string msg, string fn = __FILE__, size_t ln = __LINE__)  pure nothrow
    {
        super(msg, fn, ln);
    }
}

/// Exception thrown on server error
class PGServerErrorException: Exception
{
    /// Contains information about this _error. Aliased to this.
    ResponseMessage error;
    alias error this;

    this(string msg, string fn = __FILE__, size_t ln = __LINE__)  pure nothrow
    {
        super(msg, fn, ln);
    }

    this(ResponseMessage error, string fn = __FILE__, size_t ln = __LINE__)
    {
        super(error.toString(), fn, ln);
        this.error = error;
    }
}

///
class TransactionException : Exception
{
	this(string msg, Throwable thr)
    {
        super(msg, thr);
    }
}

///
class CommitTransactionException : TransactionException
{
	this(string msg, Throwable thr)
    {
        super(msg, thr);
    }
}

///
class RollbackTransactionException : TransactionException
{
	this(string msg, Throwable thr)
    {
        super(msg, thr);
    }
}
