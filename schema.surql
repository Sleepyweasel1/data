-- ------------------------------
-- OPTION
-- ------------------------------

OPTION IMPORT;

-- ------------------------------
-- LOGINS
-- ------------------------------

DEFINE LOGIN files_svc ON DATABASE PASSHASH '$argon2id$v=19$m=19456,t=2,p=1$MsA64sv9EdzuWu3WcwY7Fw$ii2I2F/MFqxYD3LRqxau5y0BzTEH01mIpyRp0/Mua/U';
DEFINE LOGIN user ON DATABASE PASSHASH '$argon2id$v=19$m=19456,t=2,p=1$W9FShMol98rWlwVApg4R0A$uPDwoCuLE0PUZUpkV8HIsKMiV3cnDUynbvx5sWyxMo0';

-- ------------------------------
-- TABLE: files
-- ------------------------------

DEFINE TABLE files SCHEMALESS;

DEFINE FIELD accessed ON files TYPE datetime;
DEFINE FIELD created ON files TYPE datetime;
DEFINE FIELD modified ON files TYPE datetime;
DEFINE FIELD path ON files TYPE string;
DEFINE FIELD size ON files TYPE int;
DEFINE FIELD tags ON files TYPE array VALUE { string::split(path, '\') };
DEFINE FIELD tier ON files VALUE <future> { RETURN IF <datetime> accessed > (time::now() - 4w2d) THEN 0 ELSE IF <datetime> accessed > (time::now() - 8w4d) THEN 1 ELSE IF <datetime> accessed > (time::now() - 17w1d) THEN 2 ELSE 3 END; };
