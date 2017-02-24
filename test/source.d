//import ddb.pg.subscriber : PGSubscriber;
//PGSubscriber subscriber;

@safe:
void runTest() @safe
{
    import ddb.pg : connectPG , PGCommand;
    import std.process : environment;
    import std.stdio;
    import vibe.core.log;
    import vibe.core.core : runTask;
    setLogLevel(LogLevel.debug_);
    //setLogLevel(LogLevel.trace);

    auto pdb = connectPG([
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
        foreach (row; result)
            writeln(row);
    }

    // scoped struct with destructor
    with (conn.transaction) {
        auto result = query(`SELECT * from "LoanRequests" Limit 1`);
        foreach (row; result)
            writeln(row);
    }

    // scoped lambda
    conn.transaction({
        auto result = conn.query(`SELECT * from "LoanRequests" Limit 1`);
        foreach (row; result)
            writeln(row);
    });

    auto subscriber = pdb.createSubscriber();

    subscriber.subscribe("test1", "test2");
    auto task = subscriber.listen((string channel, string message) {
        writefln("channel: %s, msg: %s", channel, message);
    });

    conn.publish("test1", "Hello World!");
    conn.publish("test2", "Hello from Channel 2");

    runTask({
        subscriber.subscribe("test-fiber");
        subscriber.publish("test-fiber", "Hello from the Fiber!");
        subscriber.unsubscribe();
    });

    import vibe.core.core : sleep;
    import std.datetime : msecs;
    sleep(100.msecs);
}

int main()
{
    import vibe.core.core, vibe.core.log;
    int ret = 0;
    runTask({
        runTest();
        exitEventLoop(true);
    });
    runEventLoop();
    return ret;
}
