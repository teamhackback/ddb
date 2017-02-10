void runTest()
{
    import ddb.postgres : PostgresDB, PGCommand;
    import std.process : environment;
    import std.stdio;

	auto pdb = new PostgresDB([
		"host" : environment.get("DB_HOST", "localhost"),
		"database" : environment["DB_NAME"],
		"user" : environment["DB_USER"],
		"password" : environment["DB_PASSWORD"]
	]);
	auto conn = pdb.lockConnection();

	auto cmd = new PGCommand(conn, "SELECT typname, typlen FROM pg_type");
	auto result = cmd.executeQuery;
	try
	{
		foreach (row; result)
		{
			writeln(row["typname"], ", ", row[1]);
		}
	}
	finally
	{
		result.close;
	}
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
