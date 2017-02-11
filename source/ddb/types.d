module ddb.types;

import std.datetime;

const PGEpochDate = Date(2000, 1, 1);
const PGEpochDay = PGEpochDate.dayOfGregorianCal;
const PGEpochTime = TimeOfDay(0, 0, 0);
const PGEpochDateTime = DateTime(2000, 1, 1, 0, 0, 0);

enum TransactionStatus : char {
    OutsideTransaction = 'I', // idle
    InsideTransaction = 'T', // transaction
    InsideFailedTransaction = 'E' // error (transaction recovery)
};

enum string[int] baseTypes = [
    // boolean types
    16 : "bool",
    // bytea types
    17 : "bytea",
    // character types
    18 : `"char"`, // "char" - 1 byte internal type
    1042 : "bpchar", // char(n) - blank padded
    1043 : "varchar",
    25 : "text",
    19 : "name",
    // numeric types
    21 : "int2",
    23 : "int4",
    20 : "int8",
    700 : "float4",
    701 : "float8",
    1114: "timestamp",
    1700 : "numeric",
];

public:

/**
Reference:
 - https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h
 - https://github.com/brianc/node-pg-types/blob/master/lib/textParsers.js
 - https://github.com/lpsmith/postgresql-simple/blob/master/src/Database/PostgreSQL/Simple/TypeInfo/Static.hs#L74
 - https://github.com/zadarnowski/postgresql-wire-protocol/blob/master/src/Database/PostgreSQL/Protocol/ObjectIDs.lhs
*/
enum PGType : int
{
    OID = 26, // object id
    _OID = 1028,
    OIDVECTOR = 30, // array of oids, used in system tables
    NAME = 19,
    _NAME = 1003,
    REGPROC = 24,
    _REGPROC = 1008,

    TID = 27, // physical location of tuple
    _TID = 1010,
    XID = 28, // transaction id
    _XID = 1011,
    CID = 29, // command identifier type, sequence in transaction id
    _CID = 1012,

    PG_TYPE = 71,
    PG_ATTRIBUTE = 75,
    PG_PROC = 81,
    PG_CLSS = 83,

    RECORD =  2249,
    _RECORD = 2287,

    // base
    BOOLEAN = 16,
    _BOOLEAN = 1000,
    BYTEA = 17,
    _BYTEA = 1001,
    CHAR = 18, // 1 byte "char", used internally in PostgreSQL
    _CHAR = 1002,
    BPCHAR = 1042, // Blank Padded char(n), fixed size
    _BPCHAR = 1014,
    VARCHAR = 1043,
    _VARCHAR = 1015,
    TEXT = 25,
    _TEXT = 1009,

    // numeric
    INT2 = 21,
    _INT2 = 1005,
    INT4 = 23,
    _INT4 = 1007,
    INT8 = 20,
    _INT8 = 1016,
    INT2VECTOR = 22,
    _INT2VECTOR = 1006,

    // floating point
    FLOAT4 = 700,
    _FLOAT4 = 1021,
    FLOAT8 = 701,
    _FLOAT8 = 1022,
    NUMERIC = 1700,
    _NUMERIC = 1231,

    // data
    JSON = 114,
    _JSON = 199,
    JSONB = 3802,
    _JSONB = 3807,
    XML = 142,
    _XML = 143,

    // date & time
    DATE = 1082,
    _DATE = 1182,
    TIME = 1083,
    _TIME = 1183,
    TIMESTAMP = 1114,
    _TIMESTAMP = 1115,
    TIMESTAMPTZ = 1184,
    _TIMESTAMPTZ = 1185,
    INTERVAL = 1186,
    _INTERVAL = 1187,
    TIMETZ = 1266,
    _TIMETZ = 1270,
    ABSTIME = 702, // Unix system time
    _ABSTIME = 1023,
    RELTIME = 703, // Unix delta time
    _RELTIME = 1024,
    _TINTERVAL = 1025,

    // bit
    BIT = 1560, // fixed-length bit string
    _BIT = 1561,
    VARBIT = 1562, // variable-length bit string
    _VARBIT = 1563,

    UUID = 2950,
    _UUID = 2951,

    // events
    TRIGGER = 2279,
    EVENT_TRIGGER = 3838,

    // geo
    POINT = 600,
    _POINT = 1017,
    LSEG = 601,
    _LSEG = 1018,
    PATH = 602,
    _PATH = 1019,
    BOX = 603,
    _BOX = 1020,
    POLYGON = 604,
    _POLYGON = 1027,
    LINE = 628,
    _LINE = 629,
    CIRCLE = 718,
    _CIRCLE = 719,

    // network
    INET = 869,
    _INET = 1041,
    MACADDR = 829,
    _MACADDR = 1040,
    CIDR = 650,
    _CIDR = 651,

    // other
    MONEY = 790,
    _MONEY = 791,

    // text search
    TSVECTOR = 3614,
    _TSVECTOR = 3643,
    GTSVECTOR = 3642,
    _GTSVECTOR = 3644,
    TSQUERY = 3615,
    _TSQUERY = 3645,
    REGCONFIG = 3734,
    _REGCONFIG = 3735,
    REGDICTIONARY = 3769,
    _REGDICTIONARY = 3770,

    // ranges
    INT4RANGE = 3904, // range of integers
    _INT4RANGE = 3905,
    NUMRANGE = 3906, // range of numerics
    _NUMRANGE = 3907,
    TSRANGE = 3908, // range of timestamps without time zone
    _TSRANGE = 3909,
    TSTZRANGE = 3910, // range of timestamps with time zone
    _TSTZRANGE = 3911,
    DATERANGE = 3912, // range of dates
    _DATERANGE = 3913,
    INT8RANGE = 3926, // range of bigints
    _INT8RANGE = 3927,

    // registered
    REGPROCEDURE = 2202, // registered procedure (with args)
    _REGPROCEDURE = 2207,
    REGOPER = 2203, // registered operator
    _REGOPER = 2208,
    REGOPERATOR = 2204, // registered operator (with args)
    _REGOPERATOR = 2209,
    REGCLASS = 2205, // registered class
    _REGCLASS = 2210,
    REGTYPE = 2206, // registered type
    _REGTYPE = 2211,
    REGROLE = 4096, // registered role
    _REGROLE = 4097,
    REGNAMESPACE = 4089, // registered namespace
    _REGNAMESPACE = 4080,

    UNKNOWN = 705,
};

/// Array of fields returned by the server
alias immutable(PGField)[] PGFields;

/// Contains information about fields returned by the server
struct PGField
{
    /// The field name.
    string name;
    /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
    uint tableOid;
    /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
    short index;
    /// The object ID of the field's data type.
    uint oid;
    /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
    short typlen;
    /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    int modifier;
}

struct Point {
    double x, y;
}

struct LSeg {
    //Point a, b;
    double x1, y1, x2, y2;
}

alias Box = LSeg;

struct Path {
    bool open;
    Point[] path;
}

// aka closed path
struct Polygon {
    Point[] path;
}

// Ax + By + C = 0
struct Line {
    double A, B, C;
}

struct Circle {
    double x, y, r;
}

alias PGMoney = long;

// TODO:
alias PGBigInt = long;
alias PGFloat8 = double;
alias PGFloat4 = float;
alias PGReal = float;
alias PGInt2 = short;
alias PGSmallint = PGInt2;

// arbitrary precision
struct Numeric {

}
