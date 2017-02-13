///
module ddb.pg.client;

import ddb.pg.connection : PGConnection;

@safe:

/**
A Postgres client with connection pooling.
*/
class PGClient
{
version(Have_vibe_core)
{
    import vibe.core.connectionpool : ConnectionPool, LockedConnection;
    import ddb.pg.connection : PGConnection;
    import ddb.pg.subscriber : PGSubscriber;

    private {
        const string[string] m_params;
        ConnectionPool!PGConnection m_pool;
    }

    this(string[string] conn_params)
    {
        m_params = conn_params.dup;
        m_pool = new ConnectionPool!PGConnection(&createConnection);

        // force a connection to cause an exception for wrong URLs
        lockConnection();
    }

    LockedConnection!PGConnection lockConnection() { return m_pool.lockConnection(); }

    private PGConnection createConnection()
    {
        return new PGConnection(m_params);
    }

     /**
     Creates a PGSubscriber for launching a listener
     */
     PGSubscriber createSubscriber()
     {
         return PGSubscriber(this);
     }

    @property void maxConcurrency(uint val) @trusted { m_pool.maxConcurrency = val; }
    @property uint maxConcurrency() @trusted { return m_pool.maxConcurrency; }
}
else
{
    this(string[string] conn_params)
    {
        static assert(false,
                        "The 'PostgresDB' connection pool requires Vibe.d and therefore "~
                        "must be used with -version=Have_vibe_core"
                     );
    }

    PGConnection lockConnection() {
        string[string] dummy;
        return new PGConnection(dummy);
    }

    @property void maxConcurrency(uint val) @trusted {  }
    @property uint maxConcurrency() @trusted { return 0; }
}
}

/**
Returns a PGClient that can be used to communicate to the specified
database server.
*/
PGClient connectPG(string[string] conn_params)
{
    return new PGClient(conn_params);
}
