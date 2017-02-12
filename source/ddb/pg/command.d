module ddb.pg.command;

import std.exception : enforce;

import ddb.pg.messages : Message, ResponseMessage;
import ddb.pg.connection : PGConnection;
import ddb.pg.parameters : PGParameter, PGParameters;
import ddb.pg.resultset : PGResultSet;
import ddb.pg.types;

@safe:

/// Class encapsulating prepared or non-prepared statements (commands).
class PGCommand
{
    private PGConnection conn;
    private string _query;
    private PGParameters params;
    private PGFields _fields = null;
    private string preparedName;
    private uint _lastInsertOid;
    package(ddb) bool prepared;

    /// List of parameters bound to this command
    @property PGParameters parameters()
    {
        return params;
    }

    /// List of fields that will be returned from the server. Available after successful call to bind().
    @property PGFields fields()
    {
        return _fields;
    }

    /**
    Checks if this is query or non query command. Available after successful call to bind().
    Returns: true if server returns at least one field (column). Otherwise false.
    */
    @property bool isQuery()
    {
        enforce(_fields !is null, new Exception("bind() must be called first."));
        return _fields.length > 0;
    }

    /// Returns: true if command is currently prepared, otherwise false.
    @property bool isPrepared()
    {
        return prepared;
    }

    /// Query assigned to this command.
    @property string query()
    {
        return _query;
    }
    /// ditto
    @property string query(string query)
    {
        enforce(!prepared, "Can't change query for prepared statement.");
        return _query = query;
    }

    /// If table is with OIDs, it contains last inserted OID.
    @property uint lastInsertOid()
    {
        return _lastInsertOid;
    }

    this(PGConnection conn, string query = "")
    {
        this.conn = conn;
        _query = query;
        params = new PGParameters(this);
        _fields = new immutable(PGField)[0];
        preparedName = "";
        prepared = false;
    }

    /// Prepare this statement, i.e. cache query plan.
    void prepare()
    {
        enforce(!prepared, "This command is already prepared.");
        preparedName = conn.reservePrepared();
        conn.prepare(preparedName, _query, params);
        prepared = true;
        params.changed = true;
    }

    /// Unprepare this statement. Goes back to normal query planning.
    void unprepare()
    {
        enforce(prepared, "This command is not prepared.");
        conn.unprepare(preparedName);
        preparedName = "";
        prepared = false;
        params.changed = true;
    }

    /**
    Binds values to parameters and updates list of returned fields.

    This is normally done automatically, but it may be useful to check what fields
    would be returned from a query, before executing it.
    */
    void bind()
    {
        checkPrepared(false);
        _fields = conn.bind(preparedName, preparedName, params);
        params.changed = false;
    }

    private void checkPrepared(bool bind)
    {
        if (!prepared)
        {
            // use unnamed statement & portal
            conn.prepare("", _query, params);
            if (bind)
            {
                _fields = conn.bind("", "", params);
                params.changed = false;
            }
        }
    }

    private void checkBound()
    {
        if (params.changed)
            bind();
    }

    /**
    Executes a non query command, i.e. query which doesn't return any rows. Commonly used with
    data manipulation commands, such as INSERT, UPDATE and DELETE.
    Examples:
    ---
    auto cmd = new PGCommand(conn, "DELETE * FROM table");
    auto deletedRows = cmd.executeNonQuery;
    cmd.query = "UPDATE table SET quantity = 1 WHERE price > 100";
    auto updatedRows = cmd.executeNonQuery;
    cmd.query = "INSERT INTO table VALUES(1, 50)";
    assert(cmd.executeNonQuery == 1);
    ---
    Returns: Number of affected rows.
    */
    ulong executeNonQuery()
    {
        checkPrepared(true);
        checkBound();
        return conn.executeNonQuery(preparedName, _lastInsertOid);
    }

    alias execute = executeNonQuery;
    alias run = executeNonQuery;

    /**
    Executes query which returns row sets, such as SELECT command.
    Params:
    bufferedRows = Number of rows that may be allocated at the same time.
    Returns: InputRange of DBRow!Specs.
    */
    PGResultSet!Specs executeQuery(Specs...)()
    {
        checkPrepared(true);
        checkBound();
        return conn.executeQuery!Specs(preparedName, _fields);
    }

    alias query = executeQuery;

    /**
    Executes query and returns only first row of the result.
    Params:
    throwIfMoreRows = If true, throws Exception when result contains more than one row.
    Examples:
    ---
    auto cmd = new PGCommand(conn, "SELECT 1, 'abc'");
    auto row1 = cmd.executeRow!(int, string); // returns DBRow!(int, string)
    assert(is(typeof(i[0]) == int) && is(typeof(i[1]) == string));
    auto row2 = cmd.executeRow; // returns DBRow!(Variant[])
    ---
    Throws: Exception if result doesn't contain any rows or field count do not match.
    Throws: Exception if result contains more than one row when throwIfMoreRows is true.
    */
    DBRow!Specs executeRow(Specs...)(bool throwIfMoreRows = true)
    {
        auto result = executeQuery!Specs();
        scope(exit) result.close();
        enforce(!result.empty(), "Result doesn't contain any rows.");
        auto row = result.front();
        if (throwIfMoreRows)
        {
            result.popFront();
            enforce(result.empty(), "Result contains more than one row.");
        }
        return row;
    }

    alias row = executeRow;

    /**
    Executes query returning exactly one row and field. By default, returns Variant type.
    Params:
    throwIfMoreRows = If true, throws Exception when result contains more than one row.
    Examples:
    ---
    auto cmd = new PGCommand(conn, "SELECT 1");
    auto i = cmd.executeScalar!int; // returns int
    assert(is(typeof(i) == int));
    auto v = cmd.executeScalar; // returns Variant
    ---
    Throws: Exception if result doesn't contain any rows or if it contains more than one field.
    Throws: Exception if result contains more than one row when throwIfMoreRows is true.
    */
    T executeScalar(T = Variant)(bool throwIfMoreRows = true)
    {
        auto result = executeQuery!T();
        scope(exit) result.close();
        enforce(!result.empty(), "Result doesn't contain any rows.");
        T row = result.front();
        if (throwIfMoreRows)
        {
            result.popFront();
            enforce(result.empty(), "Result contains more than one row.");
        }
        return row;
    }

    alias scalar = executeScalar;
}


