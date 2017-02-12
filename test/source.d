void runTest() @safe
{
    import ddb.pg : PostgresDB, PGCommand;
    import std.process : environment;
    import std.stdio;

    auto pdb = new PostgresDB([
        "host" : environment.get("DB_HOST", "localhost"),
        "database" : environment["DB_NAME"],
        "user" : environment["DB_USER"],
        "password" : environment["DB_PASSWORD"]
    ]);
    auto conn = pdb.lockConnection();

    // prepared statement
    {
        auto cmd = new PGCommand(conn, `SELECT * from "LoanRequests" Limit 1`);
        auto result = cmd.executeQuery();
        scope(exit) () @trusted { result.destroy; }();
        foreach (row; result)
            writeln(row);
    }

    // query
    with (conn.transaction) {
        auto result2 = conn.query(`SELECT * from "LoanRequests" Limit 1`);
        scope(exit) () @trusted { result2.destroy; }();
        foreach (row; result2)
            writeln(row);
    }

    conn.transaction({
        auto result2 = conn.query(`SELECT * from "LoanRequests" Limit 1`);
        scope(exit) () @trusted { result2.destroy; }();
        foreach (row; result2)
            writeln(row);
    });

    conn.query("LISTEN foo");
    conn.execute("NOTIFY foo, 'bar'");
}

int main()
{
    import vibe.core.core, vibe.core.log;
    int ret = 0;
    runTask({
        try runTest();
        catch (Throwable th) {
            logError("Test failed: %s", th.msg);
            logDiagnostic("Full error: %s", th);
            ret = 1;
        } finally exitEventLoop(true);
    });
    runEventLoop();
    return ret;
}
