module ddb.pgconnection;

import std.bitmanip : bigEndianToNative;
import std.conv : text, to;
import std.exception : enforce;
import std.datetime : Clock, Date, DateTime, UTC;
import std.string : indexOf, lastIndexOf;

import ddb.db : DBRow;
import ddb.exceptions;
import ddb.messages : Message, parseCommandCompletion, parseDataRow, parseReadyForQuery, parseRowDescription, RequestMessageTypes, ResponseMessage, ResponseMessageTypes;
import ddb.pgcommand : PGCommand;
import ddb.pgparameters : PGParameter, PGParameters;
import ddb.pgresultset : PGResultSet;
import ddb.pgstream : PGStream;
import ddb.types;
import ddb.utils : MD5toHex;

// Vibe.d provides a @safe RCAllocator
version(Have_vibe_core)
{
    import vibe.internal.freelistref : FreeListRef;
}
else
{
    struct FreeListRef(T)
    {
        static auto opCall(ARGS...)(ARGS args)
        {
            return new T(args);
        }
    }
}

@safe:

/*
Reference:
- https://www.postgresql.org/docs/9.6/static/protocol-message-formats.html
*/

/**
Class representing connection to PostgreSQL server.
*/
class PGConnection
{
    package(ddb):
        PGStream stream;
        string[string] serverParams;
        int serverProcessID;
        int serverSecretKey;
        TransactionStatus trStatus;
        ulong lastPrepared = 0;
        uint[uint] arrayTypes;
        uint[][uint] compositeTypes;
        string[uint][uint] enumTypes;
        bool activeResultSet;

        string reservePrepared()
        {
            synchronized (this)
            {

                return to!string(lastPrepared++);
            }
        }

        Message getMessage()
        {

            char type;
            int len;
            ubyte[1] ub;
            stream.read(ub); // message type

            type = bigEndianToNative!char(ub);
            ubyte[4] ubi;
            stream.read(ubi); // message length, doesn't include type byte

            len = bigEndianToNative!int(ubi) - 4;

            ubyte[] msg;
            if (len > 0)
            {
                msg = new ubyte[len];
                stream.read(msg);
            }

            return Message(this, type, msg);
        }

        void sendStartupMessage(const string[string] params)
        {
            bool localParam(string key)
            {
                switch (key)
                {
                    case "host", "port", "password": return true;
                    default: return false;
                }
            }

            int len = 9; // length (int), version number (int) and parameter-list's delimiter (byte)

            foreach (key, value; params)
            {
                if (localParam(key))
                    continue;

                len += key.length + value.length + 2;
            }

            stream.write(len);
            stream.write(0x0003_0000); // version number 3
            foreach (key, value; params)
            {
                if (localParam(key))
                    continue;
                stream.writeCString(key);
                stream.writeCString(value);
            }
            stream.write(cast(ubyte)-1);
        }

        void sendPasswordMessage(string password)
        {
            int len = cast(int)(4 + password.length + 1);

            stream.write(RequestMessageTypes.Password);
            stream.write(len);
            stream.writeCString(password);
        }

        void sendParseMessage(string statementName, string query, int[] oids)
        {
            int len = cast(int)(4 + statementName.length + 1 + query.length + 1 + 2 + oids.length * 4);

            stream.write(RequestMessageTypes.Parse);
            stream.write(len);
            stream.writeCString(statementName);
            stream.writeCString(query);
            stream.write(cast(short)oids.length);

            foreach (oid; oids)
                stream.write(oid);
        }

        void sendCloseMessage(DescribeType type, string name)
        {
            stream.write(RequestMessageTypes.Close);
            stream.write(cast(int)(4 + 1 + name.length + 1));
            stream.write(cast(char)type);
            stream.writeCString(name);
        }

        void sendTerminateMessage()
        {
            stream.write(RequestMessageTypes.Terminate);
            stream.write(cast(int)4);
        }

        void sendBindMessage(string portalName, string statementName, PGParameters params) @trusted
        {
            int paramsLen = 0;
            bool hasText = false;

            foreach (param; params)
            {
                enforce(param.value.hasValue, new ParamException("Parameter $" ~ to!string(param.index) ~ " value is not initialized"));

                void checkParam(T)(int len)
                {
                    if (param.value != null)
                    {
                        enforce(param.value.convertsTo!T, new ParamException("Parameter's value is not convertible to " ~ T.stringof));
                        paramsLen += len;
                    }
                }

                with (PGType)
                /*final*/ switch (param.type)
                {
                    case BOOLEAN:
                        checkParam!bool(1);
                        break;
                    case INT2: checkParam!short(2); break;
                    case INT4: checkParam!int(4); break;
                    case INT8: checkParam!long(8); break;
                    case FLOAT8: checkParam!double(8); break;

                    //case BOOLEAN:
                    //case TIMESTAMP:
                    case INET:
                    case NUMERIC:
                    case JSONB:
                    case INTERVAL:
                    case VARCHAR:
                    case TEXT:
                        paramsLen += param.value.coerce!string.length;
                        hasText = true;
                        break;
                    case BYTEA:
                        paramsLen += param.value.length;
                        break;
                    case JSON:
                        paramsLen += param.value.coerce!string.length; // TODO: object serialisation
                        break;
                    case DATE:
                        paramsLen += 4; break;
                    case TIMESTAMP:
                        paramsLen += 16; break;
                    default:
                        assert(0, param.type.to!string ~ " Not implemented");
                }
            }

            int len = cast(int)( 4 + portalName.length + 1 + statementName.length + 1 + (hasText ? (params.length*2) : 2) + 2 + 2 +
                params.length * 4 + paramsLen + 2 + 2 );

            stream.write(RequestMessageTypes.Bind);
            stream.write(len);
            stream.writeCString(portalName);
            stream.writeCString(statementName);
            if(hasText)
            {
                stream.write(cast(short) params.length);
                foreach(param; params)
                {
                    with (PGType)
                    switch (param.type)
                    {
                        case BOOLEAN:
                        case TIMESTAMP:
                        case INET:
                        case NUMERIC:
                        case JSONB:
                        case INTERVAL:
                        case VARCHAR:
                        case TEXT:
                            stream.write(cast(short) 0); // text format
                            break;
                        default:
                            stream.write(cast(short) 1); // binary format
                    }
            }
            } else {
                stream.write(cast(short)1); // one parameter format code
                stream.write(cast(short)1); // binary format
            }
            stream.write(cast(short)params.length);

            foreach (param; params)
            {
                if (param.value == null)
                {
                    stream.write(-1);
                    continue;
                }

                with (PGType)
                switch (param.type)
                {
                    case BOOLEAN:
                        stream.write(cast(bool) 1);
                        stream.write(param.value.get!bool);
                        break;
                    case INT2:
                        stream.write(cast(int)2);
                        stream.write(param.value.get!short);
                        break;
                    case INT4:
                        stream.write(cast(int)4);
                        stream.write(param.value.get!int);
                        break;
                    case INT8:
                        stream.write(cast(int)8);
                        stream.write(param.value.get!long);
                        break;
                    case FLOAT8:
                        stream.write(cast(int)8);
                        stream.write(param.value.get!double);
                        break;

                    case POINT:
                        stream.write(cast(int)16);
                        auto p = param.value.get!Point;
                        stream.write(p.x);
                        stream.write(p.y);
                        break;

                    case LINE, CIRCLE:
                        stream.write(cast(int)24);
                        auto p = param.value.get!Circle;
                        stream.write(p.x);
                        stream.write(p.y);
                        stream.write(p.r);
                        break;

                    case LSEG, BOX:
                        stream.write(cast(int)32);
                        auto p = param.value.get!Box;
                        stream.write(p.x1);
                        stream.write(p.y1);
                        stream.write(p.x2);
                        stream.write(p.y2);
                        break;

                    //case BOOLEAN:
                    //case TIMESTAMP:
                    case INET:
                    case NUMERIC:
                    case JSONB:
                    case INTERVAL:
                    case VARCHAR:
                    case TEXT:
                        auto s = param.value.coerce!string;
                        stream.write(cast(int) s.length);
                        stream.write(cast(ubyte[]) s);
                        break;
                    case BYTEA:
                        auto s = param.value;
                        stream.write(cast(int) s.length);

                        ubyte[] x;
                        x.length = s.length;
                        for (int i = 0; i < x.length; i++) {
                            x[i] = s[i].get!(ubyte);
                        }
                        stream.write(x);
                        break;
                    case JSON:
                        auto s = param.value.coerce!string;
                        stream.write(cast(int) s.length);
                        stream.write(cast(ubyte[]) s);
                        break;
                    case DATE:
                        stream.write(cast(int) 4);
                        stream.write(Date.fromISOString(param.value.coerce!string));
                        break;
                   case TIMESTAMP:
                        stream.write(cast(int) 8);
                        auto t = cast(DateTime) Clock.currTime(UTC());
                        stream.write(t);
                        break;
                    default:
                        assert(0, param.type.to!string ~ " Not implemented");
                }
            }

            stream.write(cast(short)1); // one result format code
            stream.write(cast(short)1); // binary format
        }

        enum DescribeType : char { Statement = 'S', Portal = 'P' }

        void sendDescribeMessage(DescribeType type, string name)
        {
            stream.write(RequestMessageTypes.Describe);
            stream.write(cast(int)(4 + 1 + name.length + 1));
            stream.write(cast(char)type);
            stream.writeCString(name);
        }

        void sendExecuteMessage(string portalName, int maxRows)
        {
            stream.write(RequestMessageTypes.Execute);
            stream.write(cast(int)(4 + portalName.length + 1 + 4));
            stream.writeCString(portalName);
            stream.write(cast(int)maxRows);
        }

        void sendFlushMessage()
        {
            stream.write(RequestMessageTypes.Flush);
            stream.write(cast(int)4);
        }

        void sendSyncMessage()
        {
            stream.write(RequestMessageTypes.Sync);
            stream.write(cast(int)4);
        }

        void sendQueryMessage(string query)
        {
            stream.write(RequestMessageTypes.Query);
            stream.write(cast(int)(4 + query.length + 1));
            stream.writeCString(query);
        }

        ResponseMessage handleResponseMessage(Message msg)
        {
            enforce(msg.data.length >= 2);

            char ftype;
            string fvalue;
            ResponseMessage response = new ResponseMessage;

            for (msg.read(ftype); ftype > 0; msg.read(ftype))
            {
                msg.readCString(fvalue);
                response.fields[ftype] = fvalue;
            }

            return response;
        }

        void checkActiveResultSet()
        {
            enforce(!activeResultSet, "There's active result set, which must be closed first.");
        }

        void prepare(string statementName, string query, PGParameters params)
        {
            checkActiveResultSet();
            sendParseMessage(statementName, query, params.getOids());

            sendFlushMessage();

        receive:

            Message msg = getMessage();

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case ErrorResponse:
                    ResponseMessage response = handleResponseMessage(msg);
                    sendSyncMessage();
                    throw new ServerErrorException(response);
                case ParseComplete:
                    return;
                default:
                    // async notice, notification
                    handleAsync(msg);
                    goto receive;
            }
        }

        void unprepare(string statementName)
        {
            checkActiveResultSet();
            sendCloseMessage(DescribeType.Statement, statementName);
            sendFlushMessage();

        receive:

            Message msg = getMessage();

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case ErrorResponse:
                    ResponseMessage response = handleResponseMessage(msg);
                    throw new ServerErrorException(response);
                case CloseComplete:
                    return;
                default:
                    // async notice, notification
                    handleAsync(msg);
                    goto receive;
            }
        }

        PGFields bind(string portalName, string statementName, PGParameters params)
        {
            checkActiveResultSet();
            sendCloseMessage(DescribeType.Portal, portalName);
            sendBindMessage(portalName, statementName, params);
            sendDescribeMessage(DescribeType.Portal, portalName);
            sendFlushMessage();

        receive:

            Message msg = getMessage();

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case ErrorResponse:
                    ResponseMessage response = handleResponseMessage(msg);
                    sendSyncMessage();
                    throw new ServerErrorException(response);
                case BindComplete, CloseComplete:
                    goto receive;
                case RowDescription:
                    return parseRowDescription(msg);
                case NoData:
                    return new immutable(PGField)[0];
                default:
                    // async notice, notification
                    handleAsync(msg);
                    goto receive;
            }
        }

        ulong executeNonQuery(string portalName, out uint oid)
        {
            checkActiveResultSet();
            ulong rowsAffected = 0;

            sendExecuteMessage(portalName, 0);
            sendSyncMessage();
            sendFlushMessage();

        receive:

            Message msg = getMessage();

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case ErrorResponse:
                    ResponseMessage response = handleResponseMessage(msg);
                    throw new ServerErrorException(response);
                case DataRow:
                    finalizeQuery();
                    throw new Exception("This query returned rows.");
                case CommandComplete:
                    parseCommandCompletion(msg, this, oid, rowsAffected);
                    goto receive;
                case EmptyQueryResponse:
                    goto receive;
                case ReadyForQuery:
                    parseReadyForQuery(msg, this);
                    return rowsAffected;
                default:
                    // async notice, notification
                    handleAsync(msg);
                    goto receive;
            }
        }

        DBRow!Specs fetchRow(Specs...)(ref Message msg, ref PGFields fields)
        {
            alias DBRow!Specs Row;

            static if (Row.hasStaticLength)
            {
                alias Row.fieldTypes fieldTypes;

                static string genFieldAssigns() // CTFE
                {
                    string s = "";

                    foreach (i; 0 .. fieldTypes.length)
                    {
                        s ~= "msg.read(fieldLen);\n";
                        s ~= "if (fieldLen == -1)\n";
                        s ~= text("row.setNull!(", i, ")();\n");
                        s ~= "else\n";
                        s ~= text("row.set!(fieldTypes[", i, "], ", i, ")(",
                                  "msg.readBaseType!(fieldTypes[", i, "])(fields[", i, "].oid, fields[", i, "].binaryMode, fieldLen)",
                                  ");\n");
                        // text() doesn't work with -inline option, CTFE bug
                    }

                    return s;
                }
            }

            Row row;
            short fieldCount;
            int fieldLen;

            msg.read(fieldCount);

            static if (Row.hasStaticLength)
            {
                Row.checkReceivedFieldCount(fieldCount);
                mixin(genFieldAssigns);
            }
            else
            {
                row.setLength(fieldCount);
                foreach (i; 0 .. fieldCount)
                {
                    msg.read(fieldLen);
                    if (fieldLen == -1)
                        row.setNull(i);
                    else
                    {
                        () @trusted { row[i] = msg.readBaseType!(Row.ElemType)(fields[i].oid, fields[i].binaryMode, fieldLen); }();
                    }
                }
            }

            return row;
        }

        void finalizeQuery()
        {
            Message msg;

            do
            {
                msg = getMessage();

                // async notice, notification
                handleAsync(msg);
            }
            while (msg.type != ResponseMessageTypes.ReadyForQuery);
            // TODO: triggers InvalidMemoryError
            //parseReadyForQuery(msg, this);
        }

        PGResultSet!Specs executeQuery(Specs...)(string portalName, ref PGFields fields)
        {
            checkActiveResultSet();

            PGResultSet!Specs result = new PGResultSet!Specs(this, fields, &fetchRow!Specs);

            sendExecuteMessage(portalName, 0);
            sendSyncMessage();
            sendFlushMessage();

            ulong rowsAffected = 0;

        receive:

            Message msg = getMessage();

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case DataRow:
                    return parseDataRow(msg, result, fields, this);
                case CommandComplete:
                    string tag;

                    msg.readCString(tag);

                    auto s2 = lastIndexOf(tag, ' ');
                    if (s2 >= 0)
                    {
                        rowsAffected = to!ulong(tag[s2 + 1 .. $]);
                    }

                    goto receive;
                case EmptyQueryResponse:
                    throw new Exception("Query string is empty.");
                case PortalSuspended:
                    throw new Exception("Command suspending is not supported.");
                case ReadyForQuery:
                    parseReadyForQuery(msg, this);
                    result.nextMsg = msg;
                    return result;
                case ErrorResponse:
                    ResponseMessage response = handleResponseMessage(msg);
                    throw new ServerErrorException(response);
                default:
                    // async notice, notification
                    handleAsync(msg);
                    goto receive;
            }
            assert(0);
        }

        void handleAsync(scope ref Message msg)
        {
            import std.stdio;
            writefln("msg %s: %s", msg.type, msg.data);

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case NotificationResponse:
                    int msgLength = msg.read!int;
                    int originPid = msg.read!int;
                    string channelName = msg.readCString;
                    string payload = msg.readCString;
                    writeln("[Async] Notification: ", channelName, ":", payload);
                    break;
                case ReadyForQuery:
                    // ReadyForQuery (readiness to process new command)
                    parseReadyForQuery(msg, this);
                    writeln("[Async] Z");
                    break;
                default:
                    writeln("[Async] Unknonw Notification", );
            }
        }

    public:


        /**
        Opens connection to server.

        Params:
        params = Associative array of string keys and values.

        Currently recognized parameters are:
        $(UL
            $(LI host - Host name or IP address of the server. Required.)
            $(LI port - Port number of the server. Defaults to 5432.)
            $(LI user - The database user. Required.)
            $(LI database - The database to connect to. Defaults to the user name.)
            $(LI options - Command-line arguments for the backend. (This is deprecated in favor of setting individual run-time parameters.))
        )

        In addition to the above, any run-time parameter that can be set at backend start time might be listed.
        Such settings will be applied during backend start (after parsing the command-line options if any).
        The values will act as session defaults.

        Examples:
        ---
        auto conn = new PGConnection([
            "host" : "localhost",
            "database" : "test",
            "user" : "postgres",
            "password" : "postgres"
        ]);
        ---
        */
        this(const string[string] params)
        {
            enforce("host" in params, new ParamException("Required parameter 'host' not found"));
            enforce("user" in params, new ParamException("Required parameter 'user' not found"));

            ushort port = "port" in params? params["port"].to!ushort : 5432;

            version(Have_vibe_core)
            {
                import vibe.core.net : connectTCP;
                stream = new PGStream(connectTCP(params["host"], port));
            }
            else
            {
                import std.socket : InternetAddress, TcpSocket;
                stream = new PGStream(new TcpSocket);
                stream.socket.connect(new InternetAddress(params["host"], port));
            }
            sendStartupMessage(params);

        receive:

            Message msg = getMessage();

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case ErrorResponse, NoticeResponse:
                    ResponseMessage response = handleResponseMessage(msg);

                    if (msg.type == NoticeResponse)
                        goto receive;

                    throw new ServerErrorException(response);
                case AuthenticationXXXX:
                    enforce(msg.data.length >= 4);

                    int atype;

                    msg.read(atype);

                    switch (atype)
                    {
                        case 0:
                            // authentication successful, now wait for another messages
                            goto receive;
                        case 3:
                            // clear-text password is required
                            enforce("password" in params, new ParamException("Required parameter 'password' not found"));
                            enforce(msg.data.length == 4);

                            sendPasswordMessage(params["password"]);

                            goto receive;
                        case 5:
                            // MD5-hashed password is required, formatted as:
                            // "md5" + md5(md5(password + username) + salt)
                            // where md5() returns lowercase hex-string
                            enforce("password" in params, new ParamException("Required parameter 'password' not found"));
                            enforce(msg.data.length == 8);

                            char[3 + 32] password;
                            password[0 .. 3] = "md5";
                            password[3 .. $] = MD5toHex(MD5toHex(
                                params["password"], params["user"]), msg.data[4 .. 8]);

                            sendPasswordMessage(to!string(password));

                            goto receive;
                        default:
                            // non supported authentication type, close connection
                            this.close();
                            throw new Exception("Unsupported authentication type");
                    }

                case ParameterStatus:
                    enforce(msg.data.length >= 2);

                    string pname, pvalue;

                    msg.readCString(pname);
                    msg.readCString(pvalue);

                    serverParams[pname] = pvalue;

                    goto receive;

                case BackendKeyData:
                    enforce(msg.data.length == 8);

                    msg.read(serverProcessID);
                    msg.read(serverSecretKey);

                    goto receive;

                case ReadyForQuery:
                    parseReadyForQuery(msg, this);
                    // connection is opened and now it's possible to send queries
                    reloadAllTypes();
                    return;
                default:
                    // unknown message type, ignore it
                    handleAsync(msg);
                    goto receive;
            }
        }

        /// Closes current connection to the server.
        void close()
        {
            sendTerminateMessage();
            stream.socket.close();
        }

        /// Shorthand methods using temporary PGCommand. Semantics is the same as PGCommand's.
        ulong executeNonQuery(string query)
        {
            checkActiveResultSet();
            sendQueryMessage(query);
            ulong rowsAffected = 0;

        receive:

            Message msg = getMessage();

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case ErrorResponse:
                    ResponseMessage response = handleResponseMessage(msg);
                    throw new ServerErrorException(response);
                case DataRow:
                    finalizeQuery();
                    throw new Exception("This query returned rows.");
                case CommandComplete:
                    uint oid;
                    parseCommandCompletion(msg, this, oid, rowsAffected);
                    goto receive;
                case EmptyQueryResponse:
                    goto receive;
                case ReadyForQuery:
                    parseReadyForQuery(msg, this);
                    return rowsAffected;
                default:
                    // async notice, notification
                    handleAsync(msg);
                    goto receive;
            }
        }

        /// ditto
        PGResultSet!Specs executeQuery(Specs...)(string query)
        {
            checkActiveResultSet();
            PGResultSet!Specs result;
            PGFields fields;

            sendQueryMessage(query);

            ulong rowsAffected = 0;

        receive:

            Message msg = getMessage();

            with (ResponseMessageTypes)
            switch (msg.type)
            {
                case RowDescription:
                    // response to Describe
                    fields = parseRowDescription(msg);
                    result = new PGResultSet!Specs(this, fields, &fetchRow!Specs);
                    goto receive;
                case DataRow:
                    return parseDataRow(msg, result, fields, this);
                case CommandComplete:
                    string tag;

                    msg.readCString(tag);

                    auto s2 = lastIndexOf(tag, ' ');
                    if (s2 >= 0)
                    {
                        rowsAffected = to!ulong(tag[s2 + 1 .. $]);
                    }

                    goto receive;
                case EmptyQueryResponse:
                    throw new Exception("Query string is empty.");
                case PortalSuspended:
                    throw new Exception("Command suspending is not supported.");
                case ReadyForQuery:
                    parseReadyForQuery(msg, this);
                    result.nextMsg = msg;
                    return result;
                case ErrorResponse:
                    ResponseMessage response = handleResponseMessage(msg);
                    throw new ServerErrorException(response);
                default:
                    // async notice, notification
                    handleAsync(msg);
                    goto receive;
            }
            assert(0);

        }

        /// ditto
        DBRow!Specs executeRow(Specs...)(string query, bool throwIfMoreRows = true)
        {
            auto result = executeQuery!Specs(query);
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

        /// ditto
        T executeScalar(T)(string query, bool throwIfMoreRows = true)
        {
            auto result = executeQuery!T(query);
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

        void reloadArrayTypes()
        {
            scope cmd = FreeListRef!PGCommand(this, "SELECT oid, typelem FROM pg_type WHERE typcategory = 'A'");
            auto result = cmd.executeQuery!(uint, "arrayOid", uint, "elemOid");
            scope(exit) () @trusted { result.destroy; }();

            arrayTypes = null;

            foreach (row; result)
            {
                arrayTypes[row.arrayOid] = row.elemOid;
            }

            () @trusted { arrayTypes.rehash; }();
        }

        void reloadCompositeTypes()
        {
            scope cmd = FreeListRef!PGCommand(this, "SELECT a.attrelid, a.atttypid FROM pg_attribute a JOIN pg_type t ON
                                     a.attrelid = t.typrelid WHERE a.attnum > 0 ORDER BY a.attrelid, a.attnum");
            auto result = cmd.executeQuery!(uint, "typeOid", uint, "memberOid");
            scope(exit) () @trusted { result.destroy; }();

            compositeTypes = null;

            uint lastOid = 0;
            uint[]* memberOids;

            foreach (row; result)
            {
                if (row.typeOid != lastOid)
                {
                    compositeTypes[lastOid = row.typeOid] = new uint[0];
                    memberOids = &compositeTypes[lastOid];
                }

                *memberOids ~= row.memberOid;
            }

            () @trusted { compositeTypes.rehash; }();
        }

        void reloadEnumTypes()
        {
            scope cmd = FreeListRef!PGCommand(this, "SELECT enumtypid, oid, enumlabel FROM pg_enum ORDER BY enumtypid, oid");
            auto result = cmd.executeQuery!(uint, "typeOid", uint, "valueOid", string, "valueLabel");
            scope(exit) () @trusted { result.destroy; }();

            enumTypes = null;

            uint lastOid = 0;
            string[uint]* enumValues;

            foreach (row; result)
            {
                if (row.typeOid != lastOid)
                {
                    if (lastOid > 0)
                        () @trusted { (*enumValues).rehash; }();

                    enumTypes[lastOid = row.typeOid] = null;
                    enumValues = &enumTypes[lastOid];
                }

                (*enumValues)[row.valueOid] = row.valueLabel;
            }

            () @trusted {
                if (lastOid > 0)
                    (*enumValues).rehash;

                enumTypes.rehash;
            }();
        }

        void reloadAllTypes()
        {
            // todo: make simpler type lists, since we need only oids of types (without their members)
            reloadArrayTypes();
            reloadCompositeTypes();
            reloadEnumTypes();
        }
}
