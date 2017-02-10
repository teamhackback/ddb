module ddb.types;

import std.datetime;

const PGEpochDate = Date(2000, 1, 1);
const PGEpochDay = PGEpochDate.dayOfGregorianCal;
const PGEpochTime = TimeOfDay(0, 0, 0);
const PGEpochDateTime = DateTime(2000, 1, 1, 0, 0, 0);

enum TransactionStatus : char { OutsideTransaction = 'I', InsideTransaction = 'T', InsideFailedTransaction = 'E' };

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
    1700 : "numeric"
];

public:

enum PGType : int
{
    OID = 26,
    NAME = 19,
    REGPROC = 24,
    BOOLEAN = 16,
    BYTEA = 17,
    CHAR = 18, // 1 byte "char", used internally in PostgreSQL
    BPCHAR = 1042, // Blank Padded char(n), fixed size
    VARCHAR = 1043,
    TEXT = 25,
    INT2 = 21,
    INT4 = 23,
    INT8 = 20,
    FLOAT4 = 700,
    FLOAT8 = 701,

    // reference https://github.com/lpsmith/postgresql-simple/blob/master/src/Database/PostgreSQL/Simple/TypeInfo/Static.hs#L74
    DATE = 1082,
    TIME = 1083,
    TIMESTAMP = 1114,
    TIMESTAMPTZ = 1184,
    INTERVAL = 1186,
    TIMETZ = 1266,

    JSON = 114,
    JSONARRAY = 199
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


