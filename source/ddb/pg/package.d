/**
PostgreSQL client implementation.

Features:
$(UL
    $(LI Standalone (does not depend on libpq))
    $(LI Binary formatting (avoids parsing overhead))
    $(LI Prepared statements)
    $(LI Parametrized queries (partially working))
    $(LI $(LINK2 http://www.postgresql.org/docs/9.0/static/datatype-enum.html, Enums))
    $(LI $(LINK2 http://www.postgresql.org/docs/9.0/static/arrays.html, Arrays))
    $(LI $(LINK2 http://www.postgresql.org/docs/9.0/static/rowtypes.html, Composite types))
)

TODOs:
$(UL
    $(LI Redesign parametrized queries)
    $(LI BigInt/Numeric types support)
    $(LI Geometric types support)
    $(LI Network types support)
    $(LI Bit string types support)
    $(LI UUID type support)
    $(LI XML types support)
    $(LI Transaction support)
    $(LI Asynchronous notifications)
    $(LI Better memory management)
    $(LI More friendly PGFields)
)

Bugs:
$(UL
    $(LI Support only cleartext and MD5 $(LINK2 http://www.postgresql.org/docs/9.0/static/auth-methods.html, authentication))
    $(LI Unfinished parameter handling)
    $(LI interval is converted to Duration, which does not support months)
)

$(B Data type mapping:)

$(TABLE
    $(TR $(TH PostgreSQL type) $(TH Aliases) $(TH Default D type) $(TH D type mapping possibilities))
    $(TR $(TD smallint) $(TD int2) $(TD short) <td rowspan="19">Any type convertible from default D type</td>)
    $(TR $(TD integer) $(TD int4) $(TD int))
    $(TR $(TD bigint) $(TD int8) $(TD long))
    $(TR $(TD oid) $(TD reg***) $(TD uint))
    $(TR $(TD decimal) $(TD numeric) $(TD not yet supported))
    $(TR $(TD real) $(TD float4) $(TD float))
    $(TR $(TD double precision) $(TD float8) $(TD double))
    $(TR $(TD character varying(n)) $(TD varchar(n)) $(TD string))
    $(TR $(TD character(n)) $(TD char(n)) $(TD string))
    $(TR $(TD text) $(TD) $(TD string))
    $(TR $(TD "char") $(TD) $(TD char))
    $(TR $(TD bytea) $(TD) $(TD ubyte[]))
    $(TR $(TD timestamp without time zone) $(TD timestamp) $(TD DateTime))
    $(TR $(TD timestamp with time zone) $(TD timestamptz) $(TD SysTime))
    $(TR $(TD date) $(TD) $(TD Date))
    $(TR $(TD time without time zone) $(TD time) $(TD TimeOfDay))
    $(TR $(TD time with time zone) $(TD timetz) $(TD SysTime))
    $(TR $(TD interval) $(TD) $(TD Duration (without months and years)))
    $(TR $(TD boolean) $(TD bool) $(TD bool))
    $(TR $(TD enums) $(TD) $(TD string) $(TD enum))
    $(TR $(TD arrays) $(TD) $(TD Variant[]) $(TD dynamic/static array with compatible element type))
    $(TR $(TD composites) $(TD record, row) $(TD Variant[]) $(TD dynamic/static array, struct or Tuple))
)

Examples:
with vibe.d use -version=Have_vibe_d_core and use a ConnectionPool (PostgresDB Object & lockConnection)
---

    auto pdb = new PostgresDB([
        "host" : "192.168.2.50",
        "database" : "postgres",
        "user" : "postgres",
        "password" : ""
    ]);
    auto conn = pdb.lockConnection();

    auto cmd = new PGCommand(conn, "SELECT typname, typlen FROM pg_type");
    auto result = cmd.executeQuery;
    scope(exit) result.destroy;

    foreach (row; result)
        writeln(row["typname"], ", ", row[1]);

---
without vibe.d you can use std sockets with PGConnection object

---
import std.stdio;
import ddb.postgres;

int main(string[] argv)
{
    auto conn = new PGConnection([
        "host" : "localhost",
        "database" : "test",
        "user" : "postgres",
        "password" : "postgres"
    ]);

    scope(exit) conn.close;

    auto cmd = new PGCommand(conn, "SELECT typname, typlen FROM pg_type");
    auto result = cmd.executeQuery;
    scope(exit) result.destroy;

    foreach (row; result)
        writeln(row[0], ", ", row[1]);

    return 0;
}
---

Copyright: Copyright Piotr Szturmaj 2011-.
License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
Authors: Piotr Szturmaj
*//*
Documentation contains portions copied from PostgreSQL manual (mainly field information and
connection parameters description). License:

Portions Copyright (c) 1996-2010, The PostgreSQL Global Development Group
Portions Copyright (c) 1994, The Regents of the University of California

Permission to use, copy, modify, and distribute this software and its documentation for any purpose,
without fee, and without a written agreement is hereby granted, provided that the above copyright
notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR DIRECT,
INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE UNIVERSITY
OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF
CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS,
OR MODIFICATIONS.
*/
module ddb.pg;

public import ddb.db;
public import ddb.pg.command : PGCommand;
public import ddb.pg.connection : PGConnection;
public import ddb.pg.exceptions;

version(Have_vibe_core)
{
    @safe:

    class PostgresDB {
        import vibe.core.connectionpool : ConnectionPool;
        import ddb.pg.connection : PGConnection;

        private {
            string[string] m_params;
            ConnectionPool!PGConnection m_pool;
        }

        this(string[string] conn_params)
        {
            m_params = conn_params.dup;
            m_pool = new ConnectionPool!PGConnection(&createConnection);
        }

        auto lockConnection() { return m_pool.lockConnection(); }

        private PGConnection createConnection()
        {
            return new PGConnection(m_params);
        }

        @property void maxConcurrency(uint val) @trusted { m_pool.maxConcurrency = val; }
        @property uint maxConcurrency() @trusted { return m_pool.maxConcurrency; }
    }
}
else
{
    class PostgresDB {
        static assert(false,
                      "The 'PostgresDB' connection pool requires Vibe.d and therefore "~
                      "must be used with -version=Have_vibe_d_core"
                      );
    }
}

