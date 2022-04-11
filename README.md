# check_orapg
*Extension for validate objects migration from Oracle to PostgreSQL*

###### check_orapg 3.0 | Author: Yudisney Vazquez (yvazquez@gmail.com)

## Description

check_orapg is a PostgreSQL extension that validates all migrated databases from Oracle sources to PostgreSQL or EDB Postgres have the totality of objects, looking into:
* Global objects, like dblinks, roles, users, role and user privileges, jobs, tablespaces, profiles and synonyms.
* Privileges by roles and users.
* Schema objects, like constraints, functions, indexes, ordinary and materialized views, packages and packages body, procedures, sequences, synonyms, tables and triggers.
* Rows by schemas and tables.
* Tables and column comments.
* Attributes by table, index, primary and foreign key.
* Constraint types.
* Synonyms list.

The extension works with PostgreSQL and EDB Postgres version 11 or superior and uses the oracle_fdw extension -for PostgreSQL- and edb_dblink_oci -for EDB Postgres-.

The extension creates a schema named check_orapg to save tables and functions used for the validation, and it has functions to connect to Oracle, copy the catalog information required to confirm that objects existing in Oracle are in PostgreSQL as well. It is important to note that, in the case of a migration to PostgreSQL, some objects are not supported, such as packages or synonyms. PostgreSQL or EDB Postgres will be chosen depending on the database to be migrated. The extension will check for the supported objects on the server.

As a result, check_orapg generates two files that can be compared using existing tools, to speed up the detection of differences between both servers.

The new version count only Oracle objects valids.

## Prerequisites for PostgreSQL

Before installing check_orapg for PostgreSQL, oracle_fdw extension must be installed and configured; it can be downloaded from https://github.com/laurenz/oracle_fdw and installed following the next steps:
1. Install dependencies (oracle-instantclient11.2-basic-11.2.0.4.0-1.x86_64.rpm, oracle-instantclient11.2-devel-11.2.0.4.0-1.x86_64.rpm, oracle-instantclient11.2-sqlplus-11.2.0.4.0-1.x86_64.rpm, make, gcc, epel-release, llvm5.0, centos-release-scl-rh, llvm-toolset-7-llvm, devtoolset-7, llvm-toolset-7, postgresql11-devel).
2. Create tnsnames.ora file in /usr/lib/oracle/11.2/client64/network/admin with the connection parameters to Oracle server (databaseName = (DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = ipAddressOracleServer)(PORT = portOracleServer))) (CONNECT_DATA = (SERVICE_NAME = databaseName)))).
3. Create enviroment variables (ORACLE_HOME=/usr/lib/oracle/11.2/client64, TNS_ADMIN=$ORACLE_HOME/network/admin, PATH=$PATH:$ORACLE_HOME/bin, LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH).
4. Add libraries routes in /etc/ld.so.conf.d/postgresql-pgdg-libs.conf (/usr/pgsql-11/lib/, /usr/lib/oracle/11.2/client64/lib/).
5. Create symbolic links (ln -s /usr/lib/oracle/11.2/client64/lib/libnnz11.so /usr/lib64/, ln -s /usr/lib/oracle/11.2/client64/lib/libclntsh.so.11.1 /usr/lib64/).
6. Install extension oracle_fdw with make and make install commands.
7. Restart PostgreSQL.
8. Create oracle_fdw extension in the database (optional discard and use CASCADE option when create the check_orapg extension).

## Prerequisites for EDB Postgres

Before installing check_orapg for EDB Postgres, make sure edb_dblink_oci extension is installed -in EDB Postgres 11 it is installed by default- and:
1. Install dependencies (oracle-instantclient11.2-basic-11.2.0.4.0-1.x86_64.rpm).
2. Create symbolic links (ln -s /usr/lib/oracle/11.2/client64/lib/libclntsh.so.11.1 /usr/lib/oracle/11.2/client64/lib/libclntsh.so, ln -s /usr/lib/oracle/11.2/client64/lib/libnnz11.so /usr/lib64/, ln -s /usr/lib/oracle/11.2/client64/lib/libclntsh.so.11.1 /usr/lib64/).
3. Add the value /usr/lib/oracle/11.2/client64/lib/ to the variable oracle_home in postgresql.conf.

## Installing check_orapg

In order to install the extension; first, copy the directory -composed by 4 files: check_orapg.control, check_orapg--3.0.sql, Makefile and Readme-, to compile in the terminal and create the extension in the database that needs to be validated; this can be done following the below steps:
* In terminal:
	* cd /tmp/check_orapg/
	* make
	* make install
* In database:
	* CREATE EXTENSION oracle_fdw; --only in PostgreSQL
	* CREATE EXTENSION check_orapg;

## Tables and functions

check_orapg has 33 tables and 36 functions that cover all the functionalities required to validate the migration between Oracle and PostgreSQL databases.

Has 31 tables related to the Oracle catalog and 2 tables for the validation:
* postgres_validation: registers the counting of objects in the PostgreSQL database.
* oracle_validation: registers the counting of objects in the Oracle database.

Has 5 main functions to allow the communication with Oracle and generate the objects count files in both servers:
* create_oracle_server(p_server text, p_ip_addr inet, p_port integer, p_db text): create the object "server" to allow the copy of Oracle catalog tables to PostgreSQL.
* create_user(p_server text, p_username text, p_pass text): creates the user mapping to allow the connection to Oracle server.
* create_oracle_tables(p_server text, p_schema text, p_schema_list text): creates and copy the Oracle catalog tables in PostgreSQL.
* generate_postgres_file(p_date date, p_schema_list text, p_location_file text, p_name_output_file text): generates, in a location set by parameter, a file with the PostgreSQL objects count, that can be compared with his Oracle similar.
* generate_oracle_file(p_date date, p_schema_list text, p_location_file text, p_name_output_file text): generates, in a location set by parameter, a file with the PostgreSQL objects count, that can be compared with his Oracle similar.

## Functionalities

* check_orapg.create_oracle_server(p_server text, p_ip_addr inet, p_port integer, p_db text): creates the server to allow the connection from PostgreSQL to Oracle.
	* p_server: name asigned to the Oracle server when it was created, ex. pg_ora
	* p_ip_addr: Oracle's server IP adress, ex. 192.112.10.31
	* p_port: Oracle's server port, ex. 1521
	* p_db: Oracle database migrated
	* Example: SELECT * FROM check_orapg.create_oracle_server('pg_ora','192.112.10.31', 1521, 'PBEC')
* check_orapg.show_servers(): shows all the servers created.
	* Example: SELECT * FROM check_orapg.show_servers()
* check_orapg.delete_server(p_server text): deletes a server, set by parameter.
	* p_server: name asigned to the Oracle server when it was created.
	* Example: SELECT * FROM check_orapg.delete_server('pg_ora')
* check_orapg.update_server(p_server text, p_ip_addr inet, p_port integer, p_db text): updates IP, port, database or all connection properties of Oracle server; if a property doesn't need to be updated, a 'null' value can be used in the asociated parameter.
	* p_server: name of the Oracle server asigned when it was created
	* p_ip_addr: new IP adress of the Oracle server, or null
	* p_port: new port of Oracle server, or null
	* p_db: new database of the Oracle server, or null
	* Example: SELECT * FROM check_orapg.update_server('pg_ora','192.112.10.45', null, null)
* check_orapg.create_user(p_server text, p_username text, p_pass text): creates user mapping used for the Oracle server connection.
	* p_server: name of the Oracle server asigned when it was created
	* p_username: username used for the Oracle server connection
	* p_pass: password used for the Oracle server connection
	* Example: SELECT * FROM check_orapg.create_user('pg_ora','daniel', 'c4rlos*')
* check_orapg.show_users(): show all the user mappings created.
	* Example: SELECT * FROM check_orapg.show_users()
* check_orapg.update_user_password(p_username text, p_server text, p_pass text): updates the password for a username on a server, set by parameter.
	* p_username: username asigned when it was created
	* p_server: name of the Oracle server asigned when it was created
	* p_pass: new password
	* Example: SELECT * FROM check_orapg.update_user_password('daniel','pg_ora','4l3x*')
* check_orapg.delete_user(p_server text, p_user text): deletes a user mapping for a specific server.
	* p_server: name of the Oracle server asigned when it was created
	* p_username: username asigned when it was created
	* Example: SELECT * FROM check_orapg.delete_user('pg_ora','daniel')
* check_orapg.create_oracle_tables(p_server text, p_schema text, p_schema_list text): creates and fills the Oracle catalog tables in PostgreSQL.
	* p_server: name of the Oracle server asigned when it was created
	* p_schema: schema where the check_orapg extension was created, null if the default schema is used
	* p_schema_list: list of schemas to validate in the migration, null if all the schemas will be migrated
	* Example: SELECT * FROM check_orapg.create_oracle_tables('pg_ora',null,'''andrew'',''allan'',''pagila''')
* check_orapg.update_all_oracle_tables(p_server text, p_schema_list text): updates all Oracle catalog tables in PostgreSQL.
	* p_server: name of the Oracle server asigned when it was created
	* p_schema: schema where the check_orapg extension was created, null if the default schema is used
	* p_schema_list: list of schemas to validate in the migration, null if all the schemas will be migrated
	* Example: SELECT * FROM check_orapg.update_all_oracle_tables('pg_ora',null,'''andrew'',''allan'',''pagila''')
* check_orapg.update_oracle_table(p_server text, p_schema text, p_schema_list text, p_table text): updates a specific Oracle catalog table in PostgreSQL.
	* p_server: name of the Oracle server asigned when it was created
	* p_schema: schema where the check_orapg extension was created, null if the default schema is used
	* p_schema_list: list of schemas to validate in the migration, null if all the schemas will be migrated
	* p_table: table to update from Oracle to PostgreSQL
	* Example: SELECT * FROM check_orapg.update_oracle_table('pg_ora',null,'mig_db_links')
* check_orapg.update_oracle_tables_rows(p_server text, p_schema text): updates the row count of all tables existing in the schema list to be validated.
	* p_server: name of the Oracle server asigned when it was created
	* p_schema: schema where the check_orapg extension was created, null if the default schema is used
	* Example: SELECT * FROM check_orapg.update_oracle_tables_rows('pg_ora', null)
* check_orapg.cluster_postgres(): returns a list of global objects from the PostgreSQL cluster (dblinks, directories, global role and user privileges, jobs, profiles, roles, users, scheduled jobs, synonyms and tablespaces).
	* Example: SELECT * FROM check_orapg.cluster_postgres() AS (s_object text, s_total int)
* check_orapg.cluster_oracle(p_schemas text): returns a list of global objects from the Oracle cluster (dblinks, directories, global role and user privileges, jobs, profiles, roles, users, scheduled jobs, synonyms and tablespaces).
	* p_schemas: list of schemas to validate in the migration
	* Example: SELECT * FROM check_orapg.cluster_oracle('''andrew'',''allan'',''pagila''') AS (s_object text, s_total int)
* check_orapg.users_privileges_postgres(p_schema_list text): returns a list of cluster users and the total of privileges asigned to everyone by tables, views, sequences, packages, functions, etc. in PostgreSQL.
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.users_privileges_postgres('''andrew'',''allan'',''pagila''') AS (s_user text, s_asigned_privileges bigint)
* check_orapg.users_privileges_oracle(p_schema_list text): returns a list of cluster users and the total of privileges asigned to everyone by tables, views, sequences, packages, functions, etc. in Oracle.
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.users_privileges_postgres('''andrew'',''allan'',''pagila''') AS (s_user text, s_asigned_privileges bigint)
* check_orapg.users_privileges_postgres(): the same as users_privileges_postgres(p_schema_list text) but looking into all PostgreSQL schemas.
	* Example: SELECT * FROM check_orapg.users_privileges_postgres() AS (s_user text, s_asigned_privileges bigint)
* check_orapg.users_privileges_oracle(): the same as users_privileges_oracle(p_schema_list text) but looking into all Oracle schemas.
	* Example: SELECT * FROM check_orapg.users_privileges_postgres() AS (s_user text, s_asigned_privileges bigint)
* check_orapg.roles_privileges_postgres(p_schema_list text): returns a list of cluster roles and the total of privileges asigned to everyone by tables, views, sequences, packages, functions, etc. in PostgreSQL.
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.users_privileges_postgres('''andrew'',''allan'',''pagila''') AS (s_user text, s_asigned_privileges bigint)
* check_orapg.roles_privileges_oracle(p_schema_list text): returns a list of cluster roles and the total of privileges asigned to everyone by tables, views, sequences, packages, functions, etc. in Oracle.
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.users_privileges_postgres('''andrew'',''allan'',''pagila''') AS (s_user text, s_asigned_privileges bigint)
* check_orapg.roles_privileges_postgres(): the same as roles_privileges_postgres(p_schema_list text) but looking into all PostgreSQL schemas.
	* Example: SELECT * FROM check_orapg.users_privileges_postgres() AS (s_user text, s_asigned_privileges bigint)
* check_orapg.roles_privileges_oracle(): the same as roles_privileges_oracle(p_schema_list text) but looking into all Oracle schemas.
	* Example: SELECT * FROM check_orapg.users_privileges_postgres() AS (s_user text, s_asigned_privileges bigint)
* check_orapg.schemas_objects_postgres(p_schema_list text): returns a list of schemas and the total of tables, ordinary views, materialized views, triggers, sequences, indexes, functions, procedures, constraints, table and column comments, synonyms, packages and packages body in PostgreSQL.
	* p_schema_list: list of schemas to validate
	* Example in PostgreSQL: SELECT * FROM check_orapg.schemas_objects_postgres('''andrew'',''allan'',''pagila''') AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_sequences int,s_indexes int, s_functions int, s_procedures int, s_constraints int, s_tables_comments int,s_columns_comments int)
	* Example in EDB Postgres: SELECT * FROM check_orapg.schemas_objects_postgres('''andrew'',''allan'',''pagila''') AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int,s_indexes int, s_packages int,s_packages_body int, s_functions int, s_procedures int, s_constraints int, s_tables_comments int,s_columns_comments int)
* check_orapg.schemas_objects_oracle(p_schema_list text): returns a list of schemas and the total of tables, ordinary views, materialized views, triggers, sequences, indexes, functions, procedures, constraints, table and column comments, synonyms, packages and packages body in Oracle.
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.schemas_objects_oracle('''andrew'',''allan'',''pagila''') AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int,s_indexes int, s_packages int,s_packages_body int, s_functions int, s_procedures int, s_constraints int)
* check_orapg.schemas_objects_postgres(): returns a list of all schemas and the total of tables, ordinary views, materialized views, triggers, sequences, indexes, functions, procedures, constraints, table and column comments, synonyms, packages and packages body in PostgreSQL.
	* Example in PostgreSQL: SELECT * FROM check_orapg.schemas_objects_postgres() AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_sequences int,s_indexes int, s_functions int, s_procedures int, s_constraints int, s_tables_comments int,s_columns_comments int)
	* Example in EDB Postgres: SELECT * FROM check_orapg.schemas_objects_postgres() AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int,s_indexes int, s_packages int,s_packages_body int, s_functions int, s_procedures int, s_constraints int, s_tables_comments int,s_columns_comments int)
* check_orapg.schemas_objects_oracle(): returns a list of all schemas and the total of tables, ordinary views, materialized views, triggers, sequences, indexes, functions, procedures, constraints, table and column comments, synonyms, packages and packages body in Oracle.
	* Example: SELECT * FROM check_orapg.schemas_objects_oracle() AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int,s_indexes int, s_packages int,s_packages_body int, s_functions int, s_procedures int, s_constraints int)
* check_orapg.tables_rows_by_schema_postgres(p_schema_list text): returns the rows by table for the schema listed in PostgreSQL. 
	* p_schema: schema where to count the table rows
	* Example: SELECT * FROM check_orapg.tables_rows_by_schema_postgres('check_orapg') AS (s_table text, s_total bigint)
* check_orapg.tables_rows_by_schema_oracle(p_schema_list text): returns the rows by table for the schema listed in Oracle. 
	* p_schema: schema where to count the table rows
	* Example: SELECT * FROM check_orapg.tables_rows_by_schema_oracle('check_orapg') AS (s_table text, s_total bigint)
* check_orapg.schema_rows_postgres(p_schema_list text): returns the rows by table in every schema listed in PostgreSQL. 
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.schema_rows_postgres('''andrew'',''allan'',''pagila''') AS (s_table text, s_total bigint)
* check_orapg.schema_rows_oracle(p_schema_list text): returns the rows by table in every schema listed in Oracle. 
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.schema_rows_postgres('''andrew'',''allan'',''pagila''') AS (s_table text, s_total bigint)
* check_orapg.register_postgres_validation(p_schema_list text): registers in postgres_validation table all the counting of objects used to check the validation of PostgreSQL migration.
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.register_postgres_validation('''andrew'',''allan'',''pagila''')
* check_orapg.register_oracle_validation(p_schema_list text): registers in oracle_validation table all the counting of objects of Oracle database used to check the validation of PostgreSQL migration.
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.register_postgres_validation('''andrew'',''allan'',''pagila''')
* check_orapg.postgres_file(p_date date, p_schema_list text): returns the result of counting all the objects saved in postgres_validation.
	* p_date: date the information was stored in postgres_validation table
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.postgres_file(current_date,'''andrew'',''allan'',''pagila'''))
* check_orapg.oracle_file(p_date date, p_schema_list text): returns the result of counting all the objects saved in oracle_validation.
	* p_date: date the information was stored in postgres_validation table
	* p_schema_list: list of schemas to validate
	* Example: SELECT * FROM check_orapg.oracle_file(current_date,'''andrew'',''allan'',''pagila'''))
* check_orapg.generate_postgres_file(p_date date, p_schema_list text, p_location_file text, p_name_output_file text): generates a file with the result of the function postgres_file(p_date date, p_schema_list text).
	* p_date: date the information was stored in postgres_validation table
	* p_schema_list: list of schemas to validate
	* p_location_file: location where to put the file to be generated
	* p_name_output_file: name of the file to be generated
	* Example: SELECT * FROM check_orapg.generate_postgres_file(current_date '''andrew'',''allan'',''pagila''', '/tmp', 'postgres_validation')
* check_orapg.generate_oracle_file(p_date date, p_schema_list text, p_location_file text, p_name_output_file text): generates a file with the result of the function oracle_file(p_date date, p_schema_list text)
	* p_date: date the information was stored in postgres_validation table
	* p_schema_list: list of schemas to validate
	* p_location_file: location where to put the file to be generated
	* p_name_output_file: name of the file to be generated
	* Example: SELECT * FROM check_orapg.generate_postgres_file(current_date, '''andrew'',''allan'',''pagila''', '/tmp', 'postgres_validation'

## Examples

### Extension installation: change to extension directory and compile
	# cd /tmp/check_orapg/
	# make
	# make install
	/bin/mkdir -p '/usr/edb/as11/share/extension'
	/bin/mkdir -p '/usr/edb/as11/share/extension'
	/bin/install -c -m 644 .//check_orapg.control '/usr/edb/as11/share/extension/'
	/bin/install -c -m 644 .//check_orapg--0.2.sql  '/usr/edb/as11/share/extension/'

### oracle_fdw extension creation: necessary only for PostgreSQL 11 databases
	$ psql -d sakila
	psql.bin (11.4.11)
	Type "help" for help.

	sakila=# create extension oracle_fdw;
	CREATE EXTENSION

### check_orapg extension creation: connect to the database to validate and create the extension
	$ psql -d sakila
	psql.bin (11.4.11)
	Type "help" for help.

	sakila=# create extension check_orapg;
	CREATE EXTENSION

### Server creation: data connection to Oracle server where the migrated database resides
	sakila=# SELECT * FROM check_orapg.create_oracle_server('pg_ora','192.112.10.31', 1521, 'PBEC');
	 create_oracle_server 
	----------------------
	 t
	(1 ligne)

### Server update: if needed to update one, or all for the data connection
	sakila=# SELECT * FROM check_orapg.update_server('pg_ora','192.112.10.45', null, null);
	 update_server 
	---------------
	 t
	(1 ligne)

### Servers listing
	sakila=# SELECT * FROM check_orapg.show_servers();
	 srvname | srvowner | srvfdw | srvtype | srvversion | srvacl |             srvoptions              
	---------+----------+--------+---------+------------+--------+-------------------------------------
	 pg_ora  |       10 |  15708 |         |            |        | {connstr=//192.112.10.45:1521/PBEC}
	(1 ligne)

### User mapping creation: used for the oracle server connection
	sakila=# SELECT * FROM check_orapg.create_user('pg_ora','yudita', 'yudi');
	 create_user 
	-------------
	 t
	(1 ligne)

### User mapping update: Updating the user's password
	sakila=# SELECT * FROM check_orapg.update_user_password('yudita','pg_ora','yudita');
	 update_user_password 
	----------------------
	 t
	(1 ligne)

### User mapping listing
	sakila=# SELECT * FROM check_orapg.show_users();
	 umid  | srvid | srvname | umuser | usename |                         umoptions                          
	-------+-------+---------+--------+---------+------------------------------------------------------------
	 94115 | 94114 | pg_ora  |      0 | public  | {user=yudita,obfuscated_password=1TQgnfq19F8VqxVzBeSqXg==}
	(1 ligne)

### User mapping deletion
	sakila=# SELECT * FROM check_orapg.delete_user('pg_ora','yudita');
	 delete_user 
	-------------
	 t
	(1 ligne)

### Oracle catalog tables creation in PostgreSQL: Oracle connection using the server and user created before, and pulling the information of the catalog associated with the list of schemas provided by parameter
	sakila=# SELECT * FROM check_orapg.create_oracle_tables('pg_ora',null,'''yudita'',''pagila''');
	 create_oracle_tables 
	----------------------
	 t
	(1 ligne)

### Oracle catalog tables update in PostgreSQL: if needed to pull new schemas or the information was updated in Oracle
	sakila=# SELECT * FROM check_orapg.update_all_oracle_tables('pg_ora',null,'''yudita'',''pagila'',''allan''');
	 update_all_oracle_tables 
	--------------------------
	 t
	(1 ligne)

### Oracle catalog on table update in PostgreSQL: if needed to pull new schemas or the information was updated in Oracle for a specific table
	sakila=# SELECT * FROM check_orapg.update_oracle_table('pg_ora',null,'''yudita'',''pagila'',''allan''','mig_db_links');
	 update_oracle_table 
	---------------------
	 t
	(1 ligne)

### PostgreSQL file generated: with the information requeired to validate the migration
	sakila=# SELECT * FROM check_orapg.generate_postgres_file(current_date, '''yudita''', '/tmp', 'postgres_validation');
	NOTICE:  Registering validation...
	NOTICE:  Validation registered in postgres_validation table
	 generate_postgres_file 
	------------------------
	 t
	(1 ligne)

### Oracle file generated: with the information requeired to validate the migration
	sakila=# SELECT * FROM check_orapg.generate_oracle_file(current_date, '''yudita'',''pagila''', '/tmp', 'oracle_validation');
	NOTICE:  Registering validation...
	NOTICE:  Validation registered in oracle_validation table
	 generate_oracle_file 
	----------------------
	 t
	(1 ligne)

## Fragment of Postgres file
	-------------------------------------------------------------------------------------------------------------------------------------
	-- OBJECTS COUNTING FOR POSTGRES VALIDATION -----------------------------------------------------------------------------------------
	-------------------------------------------------------------------------------------------------------------------------------------

	-------------------------------------------------------------------------------------------------------------------------------------
	-- GENERAL
	-------------------------------------------------------------------------------------------------------------------------------------

	-- Global objects to the cluster
	-------------------------------------------------------------------------------------------------------------------------------------
	 Object                 | Total   
	------------------------+---------
	 DBLinks                |       1
	 Directories            |       0
	 Global role privileges |       31
	 Global user privileges |       17
	 Jobs                   |       0
	 Profiles               |       8
	 Roles                  |       5
	 Scheduled jobs         |       0
	 Synonyms               |       0
	 Tablespaces            |       2
	 Users                  |       6

## Fragment of Oracle file
	-------------------------------------------------------------------------------------------------------------------------------------
	-- OBJECTS COUNTING FOR ORACLE VALIDATION -----------------------------------------------------------------------------------------
	-------------------------------------------------------------------------------------------------------------------------------------

	-------------------------------------------------------------------------------------------------------------------------------------
	-- GENERAL
	-------------------------------------------------------------------------------------------------------------------------------------

	-- Global objects to the cluster
	-------------------------------------------------------------------------------------------------------------------------------------
	 Object                 | Total   
	------------------------+---------
	 DBLinks                |       1
	 Directories            |       0
	 Global role privileges |       31
	 Global user privileges |       17
	 Jobs                   |       0
	 Profiles               |       8
	 Roles                  |       5
	 Scheduled jobs         |       0
	 Synonyms               |       0
	 Tablespaces            |       2
	 Users                  |       6

## Server deletion
	sakila=# SELECT * FROM check_orapg.delete_server('pg_ora');
	NOTICE:  drop cascades to user mapping for public on server pg_ora
	 delete_server 
	---------------
	 t
	(1 ligne)

## check_orapg extension deletion
	sakila=# drop extension check_orapg cascade;
	NOTICE:  drop cascades to 31 other objects
	DÉTAIL : drop cascades to table check_orapg.mig_users
	drop cascades to table check_orapg.mig_audit_policies
	drop cascades to table check_orapg.mig_col_comments
	drop cascades to table check_orapg.mig_cons_columns
	drop cascades to table check_orapg.mig_constraints
	drop cascades to table check_orapg.mig_jobs
	drop cascades to table check_orapg.mig_scheduler_jobs
	drop cascades to table check_orapg.mig_db_links
	drop cascades to table check_orapg.mig_directories
	drop cascades to table check_orapg.mig_ind_columns
	drop cascades to table check_orapg.mig_indexes
	drop cascades to table check_orapg.mig_obj_audit_opts
	drop cascades to table check_orapg.mig_objects
	drop cascades to table check_orapg.mig_policies
	drop cascades to table check_orapg.mig_priv_audit_opts
	drop cascades to table check_orapg.mig_profiles
	drop cascades to table check_orapg.mig_roles
	drop cascades to table check_orapg.mig_acl_configuracion
	drop cascades to table check_orapg.mig_acl_privileges
	drop cascades to table check_orapg.mig_sequences
	drop cascades to table check_orapg.mig_stmt_audit_opts
	drop cascades to table check_orapg.mig_synonyms
	drop cascades to table check_orapg.mig_tab_columns
	drop cascades to table check_orapg.mig_tab_comments
	drop cascades to table check_orapg.mig_tab_privs
	drop cascades to table check_orapg.mig_tables
	drop cascades to table check_orapg.mig_tablespaces
	drop cascades to table check_orapg.mig_triggers
	drop cascades to table check_orapg.mig_ts_quotas
	drop cascades to table check_orapg.mig_rows_tables_oracle
	drop cascades to table check_orapg.mig_audit_policy_columns
	DROP EXTENSION
