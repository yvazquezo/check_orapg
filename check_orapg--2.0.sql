CREATE SCHEMA check_orapg;

SET search_path = check_orapg, pg_catalog;



--
-- Oracle dblink management
--

-- Create Oracle p_server for dblink
-- SELECT * FROM check_orapg.create_oracle_server('pg_ora','192.112.10.31', 1521, 'PBEC');
CREATE FUNCTION create_oracle_server(p_server text, p_ip_addr inet, p_port integer, p_db text) RETURNS boolean AS
$$
DECLARE
    v_exist record;
BEGIN
    SELECT * INTO v_exist FROM pg_foreign_server WHERE srvname ilike p_server;
    IF not found THEN
		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE SERVER ' || p_server || ' FOREIGN DATA WRAPPER oci_dblink OPTIONS (connstr ''//' || host(p_ip_addr) ||':' || p_port || '/' || p_db || ''')';
		ELSE
			EXECUTE 'CREATE SERVER ' || p_server || ' FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver ''//' || host(p_ip_addr) ||':' || p_port || '/' || p_db || ''')';
		END IF;
        RETURN true;
    ELSE
        RAISE EXCEPTION 'Server % exists', p_server;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Show dblinks
-- SELECT * FROM check_orapg.show_servers();
CREATE FUNCTION show_servers() RETURNS SETOF pg_foreign_server AS
$$
DECLARE
    v_search_path text;
    v_res pg_foreign_server;
BEGIN
    SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
    EXECUTE 'set search_path to ' || v_search_path;

    IF (SELECT relname from pg_class where relname ilike 'mig_db_links') is not null THEN
	FOR v_res IN SELECT * FROM pg_foreign_server WHERE srvname not in (SELECT db_link FROM mig_db_links)
	LOOP
	    RETURN NEXT v_res;
	END LOOP;
    ELSE
	FOR v_res IN SELECT * FROM pg_foreign_server
	LOOP
	    RETURN NEXT v_res;
	END LOOP;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Delete dblinks
-- SELECT * FROM check_orapg.delete_server('pg_ora');
CREATE FUNCTION delete_server(p_server text) RETURNS boolean AS
$$
DECLARE
    v_exist record;
BEGIN
	SELECT * INTO v_exist FROM pg_foreign_server WHERE srvname ilike p_server;
    IF found THEN
		EXECUTE 'DROP SERVER ' || p_server || ' CASCADE';
		RETURN true;
    ELSE
		RAISE EXCEPTION 'Server % doesn''t exist', p_server;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Update connection parameters of dblink
-- SELECT * FROM check_orapg.update_server('pg_ora','192.112.10.45', null, null);
CREATE FUNCTION update_server(p_server text, p_ip_addr inet, p_port integer, p_db text) RETURNS boolean AS
$$
DECLARE
    v_exist text;
    v_addr_old text;
    v_port_old integer;
    v_db_old text;
    v_string_conn text;
BEGIN
    SELECT srvoptions::text INTO v_exist FROM pg_foreign_server WHERE srvname ilike p_server;
    v_addr_old := substring(substring(v_exist from 13) from 1 for position(':' in substring(v_exist from 13))-1);
    v_port_old := substring(substring(v_exist from 13) from position(':' in substring(v_exist from 13))+1 for (position('/' in substring(v_exist from 13))-1-position(':' in substring(v_exist from 13))));
    v_db_old := substring(substring(v_exist from 13) from position('/' in substring(v_exist from 13))+1 for (position('}' in substring(v_exist from 13))-1-position('/' in substring(v_exist from 13))));
    IF found THEN
        IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (drop connstr)';
			v_string_conn := 'ADD connstr ''//';
		ELSE
			v_string_conn := 'SET dbserver ''//';
		END IF;
		IF p_ip_addr is not null THEN
			IF p_port is not null THEN
				IF p_db is not null THEN
					EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (' || v_string_conn || host(p_ip_addr) ||':' || p_port || '/' || p_db ||''')';
				ELSE
					EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (' || v_string_conn || host(p_ip_addr) ||':' || p_port || '/' || v_db_old ||''')';
				END IF;
			ELSE
				IF p_db is not null THEN
					EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (' || v_string_conn || host(p_ip_addr) ||':' || v_port_old || '/' || p_db ||''')';
				ELSE
					EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (' || v_string_conn || host(p_ip_addr) ||':' || v_port_old || '/' || v_db_old ||''')';
				END IF;
			END IF;
		ELSE
			IF p_port is not null THEN
				IF p_db is not null THEN
					EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (' || v_string_conn || v_addr_old ||':' || p_port || '/' || p_db ||''')';
				ELSE
					EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (' || v_string_conn || v_addr_old ||':' || p_port || '/' || v_db_old ||''')';
				END IF;
			ELSE
				IF bd is not null THEN
					EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (' || v_string_conn || v_addr_old ||':' || v_port_old || '/' || bd ||''')';
				ELSE
					EXECUTE 'ALTER SERVER ' || p_server || ' OPTIONS (' || v_string_conn || v_addr_old ||':' || v_port_old || '/' || v_db_old ||''')';
				END IF;
			END IF;
		END IF;
		RETURN true;
    ELSE
        RAISE EXCEPTION 'Server % doesn''t exist', p_server;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Create user mapping for dblink
-- SELECT * FROM check_orapg.create_user('pg_ora','yudita', 'yudi');
CREATE FUNCTION create_user(p_server text, p_username text, p_pass text) RETURNS boolean AS
$$
DECLARE
    v_exist record;
BEGIN
    SELECT * INTO v_exist FROM pg_foreign_server WHERE srvname ilike p_server;
    IF found THEN
        EXECUTE 'CREATE USER MAPPING IF NOT EXISTS FOR public SERVER ' || p_server || ' OPTIONS ("user" ''' || p_username || ''', password ''' || p_pass ||''')';
        RETURN true;
    ELSE
        RAISE EXCEPTION 'Server % doesn''t exist', p_server;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Show users mapping
-- SELECT * FROM check_orapg.show_users();
CREATE FUNCTION show_users() RETURNS SETOF pg_user_mappings AS
$$
DECLARE
    v_res pg_user_mappings;
BEGIN
    FOR v_res IN SELECT * FROM pg_user_mappings
    LOOP
        RETURN NEXT v_res;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- Update user mapping password
-- SELECT * FROM check_orapg.update_user_password('yudita','pg_ora','yudita');
CREATE FUNCTION update_user_password(p_username text, p_server text, p_pass text) RETURNS boolean AS
$$
DECLARE
    v_exist record;
    v_user_registered text := (SELECT substring(umoptions::text from position('=' in umoptions::text)+1 for (position(',' in umoptions::text)-position('=' in umoptions::text)-1)) FROM pg_user_mappings);
BEGIN
--    SELECT * INTO v_exist FROM pg_user_mappings WHERE srvname ilike p_server AND v_user_registered ilike p_username;
	SELECT * INTO v_exist FROM pg_user_mappings WHERE srvname ilike p_server;
    IF found THEN
        SELECT * INTO v_exist FROM pg_user_mappings WHERE srvname ilike p_server AND v_user_registered ilike p_username;
        IF found THEN
			EXECUTE 'ALTER USER MAPPING FOR public SERVER '|| p_server || ' OPTIONS (SET PASSWORD ''' || p_pass || ''')';
			RETURN true;
		ELSE
			RAISE EXCEPTION 'User % doesn''t exist', v_user_registered;
		END IF;
    ELSE
        RAISE EXCEPTION 'Server % doesn''t exist', p_server;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.delete_user('pg_ora','yuditA');
CREATE FUNCTION delete_user(p_server text, p_user text) RETURNS boolean AS
$$
DECLARE
    v_exist record;
BEGIN
    EXECUTE 'SELECT * FROM pg_user_mapping WHERE umoptions[1] = ''user=' || lower(p_user) || '''' INTO v_exist;
    IF v_exist is not null THEN
		EXECUTE 'DROP USER MAPPING FOR public SERVER ' || p_server;
		RETURN true;
    ELSE
		RAISE EXCEPTION 'User % doesn''t exist', lower(p_user);
    END IF;
END;
$$ LANGUAGE plpgsql;



--
-- Tables management
--

-- Foreign and physic tables creation
-- SELECT * FROM check_orapg.create_oracle_tables('pg_ora',null,'''yudita'',''pagila''');
CREATE FUNCTION create_oracle_tables(p_server text, p_schema text, p_schema_list text) RETURNS boolean AS
$$
DECLARE
    v_exist text;
    v_list record;
    v_tab text;
BEGIN
    SELECT srvname::text INTO v_exist FROM pg_foreign_server WHERE srvname ilike p_server;
    IF found THEN
		IF p_schema is null then
			p_schema := 'check_orapg';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_users AS SELECT * FROM SYS.DBA_USERS@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_users (
				username character varying(30) NOT NULL,
				user_id numeric NOT NULL,
				password character varying(30),
				account_status character varying(32) NOT NULL,
				lock_date timestamp without time zone,
				expiry_date timestamp without time zone,
				default_tablespace character varying(30) NOT NULL,
				temporary_tablespace character varying(30) NOT NULL,
				created timestamp without time zone NOT NULL,
				profile character varying(30) NOT NULL,
				initial_rsrc_consumer_group character varying(30),
				external_name character varying(4000),
				password_versions character varying(8),
				editions_enabled character varying(1),
				authentication_type character varying(8)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_USERS)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_users AS SELECT * FROM ' || p_schema || '.' || 'ora_users';
		END IF;

		IF p_schema_list is null then
			EXECUTE 'select string_agg(username, '','') from ' || p_schema || '.' || 'mig_users' INTO p_schema_list;
			p_schema_list := '''' || replace(p_schema_list, ',', ''',''') || '''';
		END IF;

		IF position('EnterpriseDB' in version()) = 0 THEN
			p_schema_list := '''' || replace(upper(p_schema_list), ',', ''',''') || '''';
		END IF;
		p_schema_list := upper(p_schema_list);
		
		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_audit_policies AS SELECT * FROM SYS.DBA_AUDIT_POLICIES@' || p_server || ' WHERE policy_owner in (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_audit_policies (
				object_schema character varying(30) NOT NULL,
				object_name character varying(30) NOT NULL,
				policy_owner character varying(30) NOT NULL,
				policy_name character varying(30) NOT NULL,
				policy_text character varying(4000),
				policy_column character varying(30),
				pf_schema character varying(30),
				pf_package character varying(30),
				pf_function character varying(30),
				enabled character varying(3),
				sel character varying(3),
				ins character varying(3),
				upd character varying(3),
				del character varying(3),
				audit_trail character varying(12),
				policy_column_options character varying(11)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_AUDIT_POLICIES WHERE policy_owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_audit_policies AS SELECT * FROM ' || p_schema || '.' || 'ora_audit_policies';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_col_comments AS SELECT * FROM SYS.DBA_COL_COMMENTS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_col_comments (
				owner character varying(30) NOT NULL,
				table_name character varying(30) NOT NULL,
				column_name character varying(30) NOT NULL,
				comments character varying(4000)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_COL_COMMENTS WHERE owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_col_comments AS SELECT * FROM ' || p_schema || '.' || 'ora_col_comments';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_cons_columns AS SELECT * FROM SYS.ALL_CONS_COLUMNS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_cons_columns (
				owner character varying(30) NOT NULL,
				constraint_name character varying(30) NOT NULL,
				table_name character varying(30) NOT NULL,
				column_name character varying(4000),
				"position" numeric
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.ALL_CONS_COLUMNS WHERE owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_cons_columns AS SELECT * FROM ' || p_schema || '.' || 'ora_cons_columns';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_constraints AS SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_CONSTRAINTS WHERE owner IN (''' || replace(p_schema_list, ',', ''',''') || ''')'') AS t1(owner text,constraint_name text,constraint_type text,table_name text,search_condition text,r_owner text,r_constraint_name text,delete_rule text,status text,"deferrable" text,deferred text,validated text,generated text,bad text,rely text,last_change text,index_owner text,index_name text,invalid text,view_related text)';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_constraints (
				owner character varying(90),
				constraint_name character varying(30) NOT NULL,
				constraint_type character varying(1),
				table_name character varying(30) NOT NULL,
				search_condition character varying,
				r_owner character varying(90),
				r_constraint_name character varying(30),
				delete_rule character varying(9),
				status character varying(8),
				"deferrable" character varying(14),
				deferred character varying(9),
				validated character varying(13),
				generated character varying(14),
				bad character varying(3),
				rely character varying(4),
				last_change timestamp without time zone,
				index_owner character varying(30),
				index_name character varying(30),
				invalid character varying(7),
				view_related character varying(14)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_CONSTRAINTS WHERE owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_constraints AS SELECT * FROM ' || p_schema || '.' || 'ora_constraints';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_jobs AS SELECT * FROM SYS.DBA_JOBS@' || p_server || ' WHERE schema_user IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_jobs (
				job numeric NOT NULL,
				log_user character varying(30) NOT NULL,
				priv_user character varying(30) NOT NULL,
				schema_user character varying(30) NOT NULL,
				last_date timestamp without time zone,
				last_sec character varying(24),
				this_date timestamp without time zone,
				this_sec character varying(24),
				next_date timestamp without time zone NOT NULL,
				next_sec character varying(24),
				total_time numeric,
				broken character varying(1),
				"interval" character varying(200) NOT NULL,
				failures numeric,
				what character varying(4000),
				nls_env character varying(4000),
				misc_env bytea,
				instance numeric
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_JOBS WHERE schema_user IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_jobs AS SELECT * FROM ' || p_schema || '.' || 'ora_jobs';
		END IF;
		
		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_scheduler_jobs AS SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_SCHEDULER_JOBS WHERE owner IN (''' || replace(p_schema_list, ',', ''',''') || ''')'')
AS t1(owner character varying(30),job_name character varying(30),job_subname character varying(30),job_style character varying(11),job_creator character varying(30),
				client_id character varying(64),global_uid character varying(32),program_owner character varying(4000),program_name character varying(4000),
				job_type character varying(16),job_action character varying(4000),number_of_arguments numeric,schedule_owner character varying(4000),
				schedule_name character varying(4000),schedule_type character varying(12),start_date timestamp(6) with time zone,
				repeat_interval character varying(4000),event_queue_owner character varying(30),event_queue_name character varying(30),
				event_queue_agent character varying(256),event_condition character varying(4000),event_rule character varying(65),
				file_watcher_owner character varying(195),file_watcher_name character varying(195),end_date timestamp(6) with time zone,
				job_class character varying(30),enabled character varying(5),auto_drop character varying(5),restartable character varying(5),
				state character varying(15),job_priority numeric,run_count numeric,max_runs numeric,failure_count numeric,max_failures numeric,
				retry_count numeric,last_start_date timestamp(6) with time zone,last_run_duration interval,next_run_date timestamp(6) with time zone,
				schedule_limit interval,max_run_duration interval,logging_level character varying(11),stop_on_window_close character varying(5),
				instance_stickiness character varying(5),raise_events character varying(4000),system character varying(5),job_weight numeric,
				nls_env character varying(4000),source character varying(128),number_of_destinations numeric,destination_owner character varying(384),
				destination character varying(384),credential_owner character varying(30),credential_name character varying(30),
				instance_id numeric,deferred_drop character varying(5),allow_runs_in_restricted_mode character varying(5),
				comments character varying(240),flags numeric)';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_scheduler_jobs (
				owner character varying(30),
				job_name character varying(30),
				job_subname character varying(30),
				job_style character varying(11),
				job_creator character varying(30),
				client_id character varying(64),
				global_uid character varying(32),
				program_owner character varying(4000),
				program_name character varying(4000),
				job_type character varying(16),
				job_action character varying(4000),
				number_of_arguments numeric,
				schedule_owner character varying(4000),
				schedule_name character varying(4000),
				schedule_type character varying(12),
				start_date timestamp(6) with time zone,
				repeat_interval character varying(4000),
				event_queue_owner character varying(30),
				event_queue_name character varying(30),
				event_queue_agent character varying(256),
				event_condition character varying(4000),
				event_rule character varying(65),
				file_watcher_owner character varying(195),
				file_watcher_name character varying(195),
				end_date timestamp(6) with time zone,
				job_class character varying(30),
				enabled character varying(5),
				auto_drop character varying(5),
				restartable character varying(5),
				state character varying(15),
				job_priority numeric,
				run_count numeric,
				max_runs numeric,
				failure_count numeric,
				max_failures numeric,
				retry_count numeric,
				last_start_date timestamp(6) with time zone,
				last_run_duration interval,
				next_run_date timestamp(6) with time zone,
				schedule_limit interval,
				max_run_duration interval,
				logging_level character varying(11),
				stop_on_window_close character varying(5),
				instance_stickiness character varying(5),
				raise_events character varying(4000),
				system character varying(5),
				job_weight numeric,
				nls_env character varying(4000),
				source character varying(128),
				number_of_destinations numeric,
				destination_owner character varying(384),
				destination character varying(384),
				credential_owner character varying(30),
				credential_name character varying(30),
				instance_id numeric,
				deferred_drop character varying(5),
				allow_runs_in_restricted_mode character varying(5),
				comments character varying(240),
				flags numeric
			)SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_SCHEDULER_JOBS WHERE owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_scheduler_jobs AS SELECT * FROM ' || p_schema || '.' || 'ora_scheduler_jobs';
		END IF;
		
		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_db_links AS SELECT * FROM SYS.DBA_DB_LINKS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_db_links (
				owner character varying(30) NOT NULL,
				db_link character varying(128) NOT NULL,
				username character varying(30),
				host character varying(2000),
				created timestamp without time zone NOT NULL
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_DB_LINKS WHERE owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_db_links AS SELECT * FROM ' || p_schema || '.' || 'ora_db_links';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_directories AS SELECT * FROM SYS.DBA_DIRECTORIES@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_directories (
				owner character varying(30) NOT NULL,
				directory_name character varying(30) NOT NULL,
				directory_path character varying(4000)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_DIRECTORIES WHERE owner IN(' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_directories AS SELECT * FROM ' || p_schema || '.' || 'ora_directories';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_ind_columns AS SELECT * FROM SYS.DBA_IND_COLUMNS@' || p_server || ' WHERE index_owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_ind_columns (
				index_owner character varying(30) NOT NULL,
				index_name character varying(30) NOT NULL,
				table_owner character varying(30) NOT NULL,
				table_name character varying(30) NOT NULL,
				column_name character varying(4000),
				column_position numeric NOT NULL,
				column_length numeric NOT NULL,
				char_length numeric,
				descend character varying(4)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_IND_COLUMNS WHERE index_owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_ind_columns AS SELECT * FROM ' || p_schema || '.' || 'ora_ind_columns';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_indexes AS SELECT * FROM SYS.DBA_INDEXES@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_indexes (
				owner character varying(30) NOT NULL,
				index_name character varying(30) NOT NULL,
				index_type character varying(27),
				table_owner character varying(30) NOT NULL,
				table_name character varying(30) NOT NULL,
				table_type character(15),
				uniqueness character varying(9),
				compression character varying(8),
				prefix_length numeric,
				tablespace_name character varying(30),
				ini_trans numeric,
				max_trans numeric,
				initial_extent numeric,
				next_extent numeric,
				min_extents numeric,
				max_extents numeric,
				pct_increase numeric,
				pct_threshold numeric,
				include_column numeric,
				freelists numeric,
				freelist_groups numeric,
				pct_free numeric,
				logging character varying(3),
				blevel numeric,
				leaf_blocks numeric,
				distinct_keys numeric,
				avg_leaf_blocks_per_key numeric,
				avg_data_blocks_per_key numeric,
				clustering_factor numeric,
				status character varying(8),
				num_rows numeric,
				sample_size numeric,
				last_analyzed timestamp without time zone,
				degree character varying(40),
				instances character varying(40),
				partitioned character varying(3),
				temporary character varying(1),
				generated character varying(1),
				secondary character varying(1),
				buffer_pool character varying(7),
				flash_cache text,
				cell_flash_cache text,
				user_stats character varying(3),
				duration character varying(15),
				pct_direct_access numeric,
				ityp_owner character varying(30),
				ityp_name character varying(30),
				parameters character varying(1000),
				global_stats character varying(3),
				domidx_status character varying(12),
				domidx_opstatus character varying(6),
				funcidx_status character varying(8),
				join_index character varying(3),
				iot_redundant_pkey_elim character varying(3),
				dropped character varying(3),
				visibility text,
				domidx_management text,
				segment_created text
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_INDEXES WHERE owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_indexes AS SELECT * FROM ' || p_schema || '.' || 'ora_indexes';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_obj_audit_opts AS SELECT owner, object_name, object_type,alt,aud,com,del,gra,ind,ins,loc,ren,sel,upd,ref,exe FROM SYS.DBA_OBJ_AUDIT_OPTS@' || p_server || ' WHERE alt !=''-/-'' or aud !=''-/-'' or com !=''-/-'' or del !=''-/-'' or gra !=''-/-''
			or ind !=''-/-''  or ins !=''-/-'' or loc !=''-/-'' or ren !=''-/-'' or sel !=''-/-''
			or upd !=''-/-'' or ref !=''-/-'' or exe !=''-/-'' and owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_obj_audit_opts (
				owner character varying(30),
				object_name character varying(30),
				object_type character varying(23),
				alt character varying(7),
				aud character varying(7),
				com character varying(7),
				del character varying(7),
				gra character varying(7),
				ind character varying(7),
				ins character varying(7),
				loc character varying(7),
				ren character varying(7),
				sel character varying(7),
				upd character varying(7),
				ref character(3),
				exe character varying(7)
			) SERVER '|| v_exist || ' options (table ''(SELECT owner, object_name, object_type,alt,aud,com,del,gra,ind,ins,loc,ren,sel,upd,ref,exe FROM SYS.DBA_OBJ_AUDIT_OPTS WHERE alt !=''''-/-'''' or aud !=''''-/-'''' or com !=''''-/-'''' or del !=''''-/-'''' or gra !=''''-/-''''
			or ind !=''''-/-''''  or ins !=''''-/-'''' or loc !=''''-/-'''' or ren !=''''-/-'''' or sel !=''''-/-''''
			or upd !=''''-/-'''' or ref !=''''-/-'''' or exe !=''''-/-'''' and owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_obj_audit_opts AS SELECT * FROM ' || p_schema || '.' || 'ora_obj_audit_opts';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_objects AS SELECT * FROM SYS.DBA_OBJECTS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_objects (
				owner character varying(30),
				object_name character varying(128),
				subobject_name character varying(30),
				object_id numeric,
				data_object_id numeric,
				object_type character varying(19),
				created timestamp without time zone,
				last_ddl_time timestamp without time zone,
				"timestamp" character varying(19),
				status character varying(7),
				temporary character varying(1),
				generated character varying(1),
				secondary character varying(1),
				namespace numeric,
				edition_name character varying(30)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_OBJECTS WHERE owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_objects AS SELECT * FROM ' || p_schema || '.' || 'ora_objects';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_policies AS SELECT * FROM SYS.DBA_POLICIES@' || p_server || ' WHERE object_owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_policies (
				object_owner character varying(30) NOT NULL,
				object_name character varying(30) NOT NULL,
				policy_group character varying(30) NOT NULL,
				policy_name character varying(30) NOT NULL,
				pf_owner character varying(30) NOT NULL,
				package character varying(30),
				function character varying(30) NOT NULL,
				sel character varying(3),
				ins character varying(3),
				upd character varying(3),
				del character varying(3),
				idx character varying(3),
				chk_option character varying(3),
				enable character varying(3),
				static_policy character varying(3),
				policy_type character varying(24),
				long_predicate character varying(3)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_POLICIES WHERE object_owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_policies AS SELECT * FROM ' || p_schema || '.' || 'ora_policies';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_priv_audit_opts AS SELECT * FROM SYS.DBA_PRIV_AUDIT_OPTS@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_priv_audit_opts (
				user_name character varying(30),
				proxy_name character varying(30),
				privilege character varying(40) NOT NULL,
				success character varying(10),
				failure character varying(10)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_PRIV_AUDIT_OPTS)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_priv_audit_opts AS SELECT * FROM ' || p_schema || '.' || 'ora_priv_audit_opts';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_profiles AS SELECT * FROM SYS.DBA_PROFILES@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_profiles (
				profile character varying(30) NOT NULL,
				resource_name character varying(32) NOT NULL,
				resource_type character varying(8),
				"limit" character varying(40)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_PROFILES)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_profiles AS SELECT * FROM ' || p_schema || '.' || 'ora_profiles';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_roles AS SELECT * FROM SYS.DBA_ROLES@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_roles (
				role character varying(30) NOT NULL,
				password_required character varying(8),
				authentication_type character varying(11)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_ROLES)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_roles AS SELECT * FROM ' || p_schema || '.' || 'ora_roles';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_acl_configuracion AS SELECT host, acl, lower_port, upper_port FROM SYS.DBA_NETWORK_ACLS@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_acl_configuracion (
				host character varying(1000) NOT NULL,
				acl character varying(4000),
				lport numeric(5,0),
				uport numeric(5,0)
			) SERVER '|| v_exist || ' options (table ''(SELECT host, acl, lower_port, upper_port FROM SYS.DBA_NETWORK_ACLS)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_acl_configuracion AS SELECT * FROM ' || p_schema || '.' || 'ora_acl_configuracion';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_acl_privileges AS SELECT acl,principal,privilege,is_grant,start_date,end_date FROM dba_network_acl_privileges@' || p_server || ' p, dba_users@' || p_server || ' u WHERE u.username=p.principal
													  UNION ALL
													  SELECT acl,principal,privilege,is_grant,start_date,end_date
													  FROM dba_network_acl_privileges@' || p_server || ' p, dba_roles@' || p_server || ' r WHERE r.role=p.principal' ;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_acl_privileges (
				acl character varying(4000),
				principal character varying(4000),
				privilege character varying(23),
				is_grant character varying(15),
				start_date character varying(17),
				end_date character varying(17)
			) SERVER '|| v_exist || ' options (table ''(SELECT acl,principal,privilege,is_grant,start_date,end_date
													  FROM dba_network_acl_privileges p, dba_users u WHERE u.username=p.principal
													  UNION ALL
													  SELECT acl,principal,privilege,is_grant,start_date,end_date
													  FROM dba_network_acl_privileges p, dba_roles r WHERE r.role=p.principal)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_acl_privileges AS SELECT * FROM ' || p_schema || '.' || 'ora_acl_privileges';
		END IF;
		
		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_sequences AS SELECT * FROM SYS.DBA_SEQUENCES@' || p_server || ' WHERE sequence_owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_sequences (
				sequence_owner character varying(30) NOT NULL,
				sequence_name character varying(30) NOT NULL,
				min_value numeric,
				max_value numeric,
				increment_by numeric NOT NULL,
				cycle_flag character varying(1),
				order_flag character varying(1),
				cache_size numeric NOT NULL,
				last_number numeric NOT NULL
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_SEQUENCES WHERE sequence_owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_sequences AS SELECT * FROM ' || p_schema || '.' || 'ora_sequences';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_stmt_audit_opts AS SELECT * FROM SYS.DBA_STMT_AUDIT_OPTS@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_stmt_audit_opts (
				user_name character varying(30),
				proxy_name character varying(30),
				audit_option character varying(40) NOT NULL,
				success character varying(10),
				failure character varying(10)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_STMT_AUDIT_OPTS)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_stmt_audit_opts AS SELECT * FROM ' || p_schema || '.' || 'ora_stmt_audit_opts';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_synonyms AS SELECT * FROM DBA_SYNONYMS@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_synonyms (
				owner character varying(30) NOT NULL,
				synonym_name character varying(30) NOT NULL,
				table_owner character varying(30),
				table_name character varying(30) NOT NULL,
				db_link character varying(128)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_SYNONYMS)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_synonyms AS SELECT * FROM ' || p_schema || '.' || 'ora_synonyms';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_columns AS SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
													  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID, DEFAULT_LENGTH, NUM_DISTINCT, LOW_VALUE,
													  HIGH_VALUE, DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE, CHARACTER_SET_NAME, 
													  CHAR_COL_DECL_LENGTH, GLOBAL_STATS, USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED, V80_FMT_IMAGE,
													  DATA_UPGRADED, HISTOGRAM FROM SYS.DBA_TAB_COLUMNS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_tab_columns (
				owner character varying(30) NOT NULL,
				table_name character varying(30) NOT NULL,
				column_name character varying(30) NOT NULL,
				data_type character varying(106),
				data_type_mod character varying(3),
				data_type_owner character varying(90),
				data_length numeric NOT NULL,
				data_precision numeric,
				data_scale numeric,
				nullable character varying(1),
				column_id numeric,
				default_length numeric,
				num_distinct numeric,
				low_value bytea,
				high_value bytea,
				density numeric,
				num_nulls numeric,
				num_buckets numeric,
				last_analyzed timestamp without time zone,
				sample_size numeric,
				character_set_name character varying(44),
				char_col_decl_length numeric,
				global_stats character varying(3),
				user_stats character varying(3),
				avg_col_len numeric,
				char_length numeric,
				char_used character varying(1),
				v80_fmt_image character varying(3),
				data_upgraded character varying(3),
				histogram character varying(15)
			) SERVER '|| v_exist || ' options (table ''(SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
													  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID, DEFAULT_LENGTH, NUM_DISTINCT, LOW_VALUE,
													  HIGH_VALUE, DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE, CHARACTER_SET_NAME, 
													  CHAR_COL_DECL_LENGTH, GLOBAL_STATS, USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED, V80_FMT_IMAGE,
													  DATA_UPGRADED, HISTOGRAM FROM SYS.DBA_TAB_COLUMNS WHERE owner IN (' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_columns AS SELECT * FROM ' || p_schema || '.' || 'ora_tab_columns';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_comments AS SELECT * FROM SYS.DBA_TAB_COMMENTS@' || p_server || ' WHERE owner IN(' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_tab_comments (
				owner character varying(30) NOT NULL,
				table_name character varying(30) NOT NULL,
				table_type character varying(11),
				comments character varying(4000)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_TAB_COMMENTS WHERE owner IN(' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_comments AS SELECT * FROM ' || p_schema || '.' || 'ora_tab_comments';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_privs AS SELECT * FROM SYS.DBA_TAB_PRIVS@' || p_server || ' WHERE owner IN(' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_tab_privs (
				grantee character varying(30) NOT NULL,
				owner character varying(30) NOT NULL,
				table_name character varying(30) NOT NULL,
				grantor character varying(30) NOT NULL,
				privilege character varying(40) NOT NULL,
				grantable character varying(3),
				hierarchy character varying(3)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_TAB_PRIVS WHERE owner IN(' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_privs AS SELECT * FROM ' || p_schema || '.' || 'ora_tab_privs';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tables AS SELECT * FROM SYS.DBA_TABLES@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_tables (
				owner character varying(30) NOT NULL,
				table_name character varying(30) NOT NULL,
				tablespace_name character varying(30),
				cluster_name character varying(30),
				iot_name character varying(30),
				status character varying(8),
				pct_free numeric,
				pct_used numeric,
				ini_trans numeric,
				max_trans numeric,
				initial_extent numeric,
				next_extent numeric,
				min_extents numeric,
				max_extents numeric,
				pct_increase numeric,
				freelists numeric,
				freelist_groups numeric,
				logging character varying(3),
				backed_up character varying(1),
				num_rows numeric,
				blocks numeric,
				empty_blocks numeric,
				avg_space numeric,
				chain_cnt numeric,
				avg_row_len numeric,
				avg_space_freelist_blocks numeric,
				num_freelist_blocks numeric,
				degree character varying(30),
				instances character varying(30),
				cache character varying(15),
				table_lock character varying(8),
				sample_size numeric,
				last_analyzed timestamp without time zone,
				partitioned character varying(3),
				iot_type character varying(12),
				temporary character varying(1),
				secondary character varying(1),
				nested character varying(3),
				buffer_pool character varying(7),
				flash_cache character varying(7),
				cell_flash_cache character varying(7),
				row_movement character varying(8),
				global_stats character varying(3),
				user_stats character varying(3),
				duration character varying(15),
				skip_corrupt character varying(8),
				monitoring character varying(3),
				cluster_owner character varying(30),
				dependencies character varying(8),
				compression character varying(8),
				compress_for character varying(12),
				dropped character varying(3),
				read_only character varying(3),
				segment_created character varying(3)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_TABLES WHERE owner IN(' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tables AS SELECT * FROM ' || p_schema || '.' || 'ora_tables';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tablespaces AS SELECT * FROM SYS.DBA_TABLESPACES@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_tablespaces (
				tablespace_name character varying(30) NOT NULL,
				block_size numeric NOT NULL,
				initial_extent numeric,
				next_extent numeric,
				min_extents numeric NOT NULL,
				max_extents numeric,
				max_size numeric,
				pct_increase numeric,
				min_extlen numeric,
				status character varying(9),
				contents character varying(9),
				logging character varying(9),
				force_logging character varying(3),
				extent_management character varying(10),
				allocation_type character varying(9),
				plugged_in character varying(3),
				segment_space_management character varying(6),
				def_tab_compression character varying(8),
				retention character varying(11),
				bigfile character varying(3),
				predicate_evaluation character varying(7),
				encrypted character varying(3),
				compress_for character varying(12)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_TABLESPACES)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tablespaces AS SELECT * FROM ' || p_schema || '.' || 'ora_tablespaces';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_triggers AS SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_TRIGGERS WHERE owner IN (''' || replace(p_schema_list, ',', ''',''') || ''')'')
AS t1(owner character varying(30),trigger_name character varying(30),trigger_type character varying(16),triggering_event character varying(227),
table_owner character varying(30),base_object_type character varying(16),table_name character varying(30),column_name character varying(4000),
referencing_names character varying(128),when_clause character varying(4000),status character varying(8),description character varying(4000),
action_type character varying(11),trigger_body text,crossedition character varying,before_statement character varying(3),before_row character varying(3),
after_row character varying(3),after_statement character varying(3),instead_of_row character varying(3),fire_once character varying(3),apply_server_only character varying(3))';
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_triggers (
				owner character varying(30),
				trigger_name character varying(30),
				trigger_type character varying(16),
				triggering_event character varying(227),
				table_owner character varying(30),
				base_object_type character varying(16),
				table_name character varying(30),
				column_name character varying(4000),
				referencing_names character varying(128),
				when_clause character varying(4000),
				status character varying(8),
				description character varying(4000),
				action_type character varying(11),
				trigger_body text,
				crossedition character varying,
				before_statement character varying(3),
				before_row character varying(3),
				after_row character varying(3),
				after_statement character varying(3),
				instead_of_row character varying(3),
				fire_once character varying(3),
				apply_server_only character varying(3)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_TRIGGERS WHERE owner IN(' || p_schema_list || '))'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_triggers AS SELECT * FROM ' || p_schema || '.' || 'ora_triggers';
		END IF;

		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_ts_quotas AS SELECT * FROM SYS.DBA_TS_QUOTAS@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_ts_quotas (
				tablespace_name character varying(30) NOT NULL,
				username character varying(30) NOT NULL,
				bytes numeric,
				max_bytes numeric,
				blocks numeric,
				max_blocks numeric,
				dropped character varying(3)
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_TS_QUOTAS)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_ts_quotas AS SELECT * FROM ' || p_schema || '.' || 'ora_ts_quotas';
		END IF;
		
		EXECUTE
		'CREATE TABLE '|| p_schema || '.' || 'mig_rows_tables_oracle (
			owner varchar,
			table_name varchar,
			total bigint
		)';
		
		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_audit_policy_columns AS SELECT * FROM SYS.DBA_AUDIT_POLICY_COLUMNS@' || p_server;
		ELSE
			EXECUTE
			'CREATE FOREIGN TABLE ' || p_schema || '.' || 'ora_audit_policy_columns (
				object_schema character varying(30) NOT NULL,
				object_name character varying(30) NOT NULL,
				policy_name character varying(30) NOT NULL,
				policy_column character varying(30) NOT NULL
			) SERVER '|| v_exist || ' options (table ''(SELECT * FROM SYS.DBA_AUDIT_POLICY_COLUMNS)'')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_audit_policy_columns AS SELECT * FROM ' || p_schema || '.' || 'ora_audit_policy_columns';
		END IF;

		v_tab := p_schema || '.mig_tables';
		FOR v_list IN EXECUTE 'SELECT owner, table_name FROM ' || v_tab || ' WHERE owner NOT IN (''SYS'',''OUTLN'',''SYSTEM'',''DBSNMP'',''APPQOSSYS'',''WMSYS'')'
		LOOP
			IF position('EnterpriseDB' in version()) > 0 THEN
				EXECUTE 'INSERT INTO ' || p_schema || '.' || 'mig_rows_tables_oracle VALUES (''' || v_list.owner ||''', ''' || v_list.table_name || ''', (SELECT count(*) FROM ' || v_list.owner || '."' || v_list.table_name || '"@' || p_server || '))';
			ELSE
				EXECUTE 'CREATE FOREIGN TABLE ' || p_schema || '.' || v_list.owner || '_' || v_list.table_name || '(
				total bigint
				) SERVER '|| v_exist || ' options (table ''(SELECT count(*) FROM ' || v_list.owner || '.' || v_list.table_name || ')'')';
				EXECUTE 'INSERT INTO ' || p_schema || '.' || 'mig_rows_tables_oracle VALUES (''' || v_list.owner ||''', ''' || v_list.table_name || ''', (SELECT total FROM '|| p_schema || '.' || v_list.owner || '_' || v_list.table_name || '))';
				EXECUTE 'DROP FOREIGN TABLE ' || p_schema || '.' || v_list.owner || '_' || v_list.table_name;
			END IF;
		END LOOP;

		RETURN true;
    ELSE
        RAISE EXCEPTION 'Server % doesn''t exist', p_server;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- Foreign and physic tables upgrade
-- SELECT * FROM check_orapg.update_all_oracle_tables('pg_ora',null,'''yudita'',''pagila'',''allan''');
CREATE FUNCTION update_all_oracle_tables(p_server text, p_schema text, p_schema_list text) RETURNS boolean AS
$$
DECLARE
    v_exist text;
    v_array text[];
    v_i int;
    v_search_path text;
BEGIN
    SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
    EXECUTE 'set search_path to ' || v_search_path;

    SELECT srvname::text INTO v_exist FROM pg_foreign_server WHERE srvname ilike p_server;
    IF found THEN
		IF p_schema is null then
			p_schema := 'check_orapg';
		END IF;
		IF p_schema_list is null then
			EXECUTE 'select string_agg(username, '','') from ' || p_schema || '.' || mig_users INTO p_schema_list;
			p_schema_list := '''' || replace(p_schema_list, ',', ''',''') || '''';
		ELSE
			EXECUTE 'SELECT string_to_array('''''|| replace(p_schema_list, ',', ''',''') || ''''','','')' INTO v_array;
			FOR v_i IN 1..array_length(v_array,1)
			LOOP
				IF replace(v_array[v_i],'''','') NOT IN (SELECT lower(username) FROM mig_users)  THEN
					RAISE EXCEPTION 'Schema % doesn''t exists', v_array[v_i];
				END IF;
			END LOOP;
		END IF;

		p_schema_list := upper(p_schema_list);
		
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_audit_policies';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_col_comments';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_cons_columns';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_constraints';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_jobs';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_scheduler_jobs';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_db_links';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_directories';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_ind_columns';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_indexes';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_obj_audit_opts';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_objects';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_policies';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_priv_audit_opts';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_profiles';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_roles';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_acl_configuracion';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_acl_privileges';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_sequences';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_stmt_audit_opts';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_synonyms';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_tab_columns';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_tab_comments';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_tab_privs';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_tables';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_tablespaces';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_triggers';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_ts_quotas';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_users';
		EXECUTE 'DROP TABLE ' || p_schema || '.' || 'mig_audit_policy_columns';
		IF position('EnterpriseDB' in version()) > 0 THEN
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_audit_policies AS SELECT * FROM SYS.DBA_AUDIT_POLICIES@' || p_server || ' WHERE policy_owner in (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_col_comments AS SELECT * FROM SYS.DBA_COL_COMMENTS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_cons_columns AS SELECT * FROM SYS.ALL_CONS_COLUMNS@' || p_server  || ' WHERE owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_constraints AS SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_CONSTRAINTS WHERE owner IN (''' || replace(p_schema_list, ',', ''',''') || ''')'') AS t1(owner text,constraint_name text,constraint_type text,table_name text,search_condition text,r_owner text,r_constraint_name text,delete_rule text,status text,"deferrable" text,deferred text,validated text,generated text,bad text,rely text,last_change text,index_owner text,index_name text,invalid text,view_related text)';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_jobs AS SELECT * FROM SYS.DBA_JOBS@' || p_server || ' WHERE schema_user IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_scheduler_jobs AS SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_SCHEDULER_JOBS WHERE owner IN (''' || replace(p_schema_list, ',', ''',''') || ''')'')
AS t1(owner character varying(30),job_name character varying(30),job_subname character varying(30),job_style character varying(11),job_creator character varying(30),
				client_id character varying(64),global_uid character varying(32),program_owner character varying(4000),program_name character varying(4000),
				job_type character varying(16),job_action character varying(4000),number_of_arguments numeric,schedule_owner character varying(4000),
				schedule_name character varying(4000),schedule_type character varying(12),start_date timestamp(6) with time zone,
				repeat_interval character varying(4000),event_queue_owner character varying(30),event_queue_name character varying(30),
				event_queue_agent character varying(256),event_condition character varying(4000),event_rule character varying(65),
				file_watcher_owner character varying(195),file_watcher_name character varying(195),end_date timestamp(6) with time zone,
				job_class character varying(30),enabled character varying(5),auto_drop character varying(5),restartable character varying(5),
				state character varying(15),job_priority numeric,run_count numeric,max_runs numeric,failure_count numeric,max_failures numeric,
				retry_count numeric,last_start_date timestamp(6) with time zone,last_run_duration interval,next_run_date timestamp(6) with time zone,
				schedule_limit interval,max_run_duration interval,logging_level character varying(11),stop_on_window_close character varying(5),
				instance_stickiness character varying(5),raise_events character varying(4000),system character varying(5),job_weight numeric,
				nls_env character varying(4000),source character varying(128),number_of_destinations numeric,destination_owner character varying(384),
				destination character varying(384),credential_owner character varying(30),credential_name character varying(30),
				instance_id numeric,deferred_drop character varying(5),allow_runs_in_restricted_mode character varying(5),
				comments character varying(240),flags numeric)';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_db_links AS SELECT * FROM SYS.DBA_DB_LINKS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_directories AS SELECT * FROM SYS.DBA_DIRECTORIES@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_ind_columns AS SELECT * FROM SYS.DBA_IND_COLUMNS@' || p_server || ' WHERE index_owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_indexes AS SELECT * FROM SYS.DBA_INDEXES@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_obj_audit_opts AS SELECT owner, object_name, object_type,alt,aud,com,del,gra,ind,ins,loc,ren,sel,upd,ref,exe FROM SYS.DBA_OBJ_AUDIT_OPTS@' || p_server || ' WHERE alt !=''-/-'' or aud !=''-/-'' or com !=''-/-'' or del !=''-/-'' or gra !=''-/-''
			or ind !=''-/-''  or ins !=''-/-'' or loc !=''-/-'' or ren !=''-/-'' or sel !=''-/-''
			or upd !=''-/-'' or ref !=''-/-'' or exe !=''-/-'' and owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_objects AS SELECT * FROM SYS.DBA_OBJECTS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_policies AS SELECT * FROM SYS.DBA_POLICIES@' || p_server || ' WHERE object_owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_priv_audit_opts AS SELECT * FROM SYS.DBA_PRIV_AUDIT_OPTS@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_profiles AS SELECT * FROM SYS.DBA_PROFILES@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_roles AS SELECT * FROM SYS.DBA_ROLES@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_acl_configuracion AS SELECT host, acl, lower_port, upper_port FROM SYS.DBA_NETWORK_ACLS@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_acl_privileges AS SELECT acl,principal,privilege,is_grant,start_date,end_date FROM dba_network_acl_privileges@' || p_server || ' p, dba_users@' || p_server || ' u WHERE u.username=p.principal
													  UNION ALL
													  SELECT acl,principal,privilege,is_grant,start_date,end_date
													  FROM dba_network_acl_privileges@' || p_server || ' p, dba_roles@' || p_server || ' r WHERE r.role=p.principal' ;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_sequences AS SELECT * FROM SYS.DBA_SEQUENCES@' || p_server || ' WHERE sequence_owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_stmt_audit_opts AS SELECT * FROM SYS.DBA_STMT_AUDIT_OPTS@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_synonyms AS SELECT * FROM DBA_SYNONYMS@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_columns AS SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER,
													  DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID, DEFAULT_LENGTH, NUM_DISTINCT, LOW_VALUE,
													  HIGH_VALUE, DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE, CHARACTER_SET_NAME, 
													  CHAR_COL_DECL_LENGTH, GLOBAL_STATS, USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED, V80_FMT_IMAGE,
													  DATA_UPGRADED, HISTOGRAM FROM SYS.DBA_TAB_COLUMNS@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_comments AS SELECT * FROM SYS.DBA_TAB_COMMENTS@' || p_server || ' WHERE owner IN(' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_privs AS SELECT * FROM SYS.DBA_TAB_PRIVS@' || p_server || ' WHERE owner IN(' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tables AS SELECT * FROM SYS.DBA_TABLES@' || p_server || ' WHERE owner IN (' || p_schema_list || ')';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tablespaces AS SELECT * FROM SYS.DBA_TABLESPACES@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_triggers AS SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_TRIGGERS WHERE owner IN (''' || replace(p_schema_list, ',', ''',''') || ''')'')
AS t1(owner character varying(30),trigger_name character varying(30),trigger_type character varying(16),triggering_event character varying(227),
table_owner character varying(30),base_object_type character varying(16),table_name character varying(30),column_name character varying(4000),
referencing_names character varying(128),when_clause character varying(4000),status character varying(8),description character varying(4000),
action_type character varying(11),trigger_body text,crossedition character varying,before_statement character varying(3),before_row character varying(3),
after_row character varying(3),after_statement character varying(3),instead_of_row character varying(3),fire_once character varying(3),apply_server_only character varying(3))';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_ts_quotas AS SELECT * FROM SYS.DBA_TS_QUOTAS@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_users AS SELECT * FROM SYS.DBA_USERS@' || p_server;
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_audit_policy_columns AS SELECT * FROM SYS.DBA_AUDIT_POLICY_COLUMNS@' || p_server;
		ELSE
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_audit_policies AS SELECT * FROM ' || p_schema || '.' || 'ora_audit_policies';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_col_comments AS SELECT * FROM ' || p_schema || '.' || 'ora_col_comments';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_cons_columns AS SELECT * FROM ' || p_schema || '.' || 'ora_cons_columns';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_constraints AS SELECT * FROM ' || p_schema || '.' || 'ora_constraints';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_jobs AS SELECT * FROM ' || p_schema || '.' || 'ora_jobs';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_scheduler_jobs AS SELECT * FROM ' || p_schema || '.' || 'ora_scheduler_jobs';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_db_links AS SELECT * FROM ' || p_schema || '.' || 'ora_db_links';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_directories AS SELECT * FROM ' || p_schema || '.' || 'ora_directories';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_ind_columns AS SELECT * FROM ' || p_schema || '.' || 'ora_ind_columns';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_indexes AS SELECT * FROM ' || p_schema || '.' || 'ora_indexes';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_obj_audit_opts AS SELECT * FROM ' || p_schema || '.' || 'ora_obj_audit_opts';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_objects AS SELECT * FROM ' || p_schema || '.' || 'ora_objects';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_policies AS SELECT * FROM ' || p_schema || '.' || 'ora_policies';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_priv_audit_opts AS SELECT * FROM ' || p_schema || '.' || 'ora_priv_audit_opts';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_profiles AS SELECT * FROM ' || p_schema || '.' || 'ora_profiles';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_roles AS SELECT * FROM ' || p_schema || '.' || 'ora_roles';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_acl_configuracion AS SELECT * FROM ' || p_schema || '.' || 'ora_acl_configuracion';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_acl_privileges AS SELECT * FROM ' || p_schema || '.' || 'ora_acl_privileges';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_sequences AS SELECT * FROM ' || p_schema || '.' || 'ora_sequences';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_stmt_audit_opts AS SELECT * FROM ' || p_schema || '.' || 'ora_stmt_audit_opts';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_synonyms AS SELECT * FROM ' || p_schema || '.' || 'ora_synonyms';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_columns AS SELECT * FROM ' || p_schema || '.' || 'ora_tab_columns';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_comments AS SELECT * FROM ' || p_schema || '.' || 'ora_tab_comments';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tab_privs AS SELECT * FROM ' || p_schema || '.' || 'ora_tab_privs';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tables AS SELECT * FROM ' || p_schema || '.' || 'ora_tables';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_tablespaces AS SELECT * FROM ' || p_schema || '.' || 'ora_tablespaces';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_triggers AS SELECT * FROM ' || p_schema || '.' || 'ora_triggers';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_ts_quotas AS SELECT * FROM ' || p_schema || '.' || 'ora_ts_quotas';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_users AS SELECT * FROM ' || p_schema || '.' || 'ora_users';
			EXECUTE 'CREATE TABLE ' || p_schema || '.' || 'mig_audit_policy_columns AS SELECT * FROM ' || p_schema || '.' || 'ora_audit_policy_columns';
		END IF;
		EXECUTE 'SELECT * FROM ' || p_schema || '.update_oracle_tables_rows(''' || p_server || ''', ''' || p_schema || ''')';
		RETURN true;
    ELSE
        RAISE EXCEPTION 'Server % doesn''t exist', p_server;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.update_oracle_table('pg_ora',null,'mig_db_links');
CREATE FUNCTION update_oracle_table(p_server text, p_schema text, p_schema_list text, p_table text) RETURNS boolean AS
$$
DECLARE
    v_exist text;
    v_table_name text;
    v_table_schema text;
    v_search_path text;
    v_array text[];
BEGIN
    SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
    EXECUTE 'set search_path to ' || v_search_path;

    SELECT srvname::text INTO v_exist FROM pg_foreign_server WHERE srvname ilike p_server;
    IF found THEN
		IF p_schema is null THEN
			p_schema := 'check_orapg';
		END IF;
		IF p_schema_list is null then
			EXECUTE 'select string_agg(username, '','') from ' || p_schema || '.' || mig_users INTO p_schema_list;
			p_schema_list := '''' || replace(p_schema_list, ',', ''',''') || '''';
		ELSE
			EXECUTE 'SELECT string_to_array('''''|| replace(p_schema_list, ',', ''',''') || ''''','','')' INTO v_array;
			FOR v_i IN 1..array_length(v_array,1)
			LOOP
				IF replace(v_array[v_i],'''','') NOT IN (SELECT lower(username) FROM mig_users)  THEN
					RAISE EXCEPTION 'Schema % doesn''t exists', v_array[v_i];
				END IF;
			END LOOP;
		END IF;
		p_schema_list := upper(p_schema_list);
		EXECUTE 'SELECT relname::text, nspname::text FROM pg_class c JOIN pg_namespace n ON (c.relnamespace=n.oid) WHERE relname ilike ''' || p_table || ''' AND nspname ilike ''' || p_schema || '''' INTO v_table_name, v_table_schema;
		IF v_table_name is not null and v_table_schema is not null THEN
			IF position('EnterpriseDB' in version()) > 0 THEN
				EXECUTE 'TRUNCATE ' || v_table_schema || '.' || p_table;
				CASE
					WHEN replace(p_table, 'mig_','dba_') in ('dba_audit_policies','dba_col_comments','dba_jobs','dba_db_links','dba_directories','dba_ind_columns','dba_indexes','dba_objects','dba_policies','dba_priv_audit_opts','dba_profiles','dba_roles','dba_sequences','dba_stmt_audit_opts','dba_synonyms','dba_tab_comments','dba_tab_privs','dba_tables','dba_tablespaces','dba_ts_quotas','dba_users','dba_audit_policy_columns') THEN
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT * FROM SYS.' || replace(p_table, 'mig_','dba_') ||'@' || p_server;
					WHEN replace(p_table, 'mig_','dba_') ilike ('dba_constraints') THEN
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_CONSTRAINTS'') AS t1(owner text,constraint_name text,constraint_type text,table_name text,search_condition text,r_owner text,r_constraint_name text,delete_rule text,status text,"deferrable" text,deferred text,validated text,generated text,bad text,rely text,last_change text,index_owner text,index_name text,invalid text,view_related text)';
					WHEN replace(p_table, 'mig_','dba_') ilike ('dba_scheduler_jobs') THEN
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_SCHEDULER_JOBS'')
AS t1(owner character varying(30),job_name character varying(30),job_subname character varying(30),job_style character varying(11),job_creator character varying(30),
				client_id character varying(64),global_uid character varying(32),program_owner character varying(4000),program_name character varying(4000),
				job_type character varying(16),job_action character varying(4000),number_of_arguments numeric,schedule_owner character varying(4000),
				schedule_name character varying(4000),schedule_type character varying(12),start_date timestamp(6) with time zone,
				repeat_interval character varying(4000),event_queue_owner character varying(30),event_queue_name character varying(30),
				event_queue_agent character varying(256),event_condition character varying(4000),event_rule character varying(65),
				file_watcher_owner character varying(195),file_watcher_name character varying(195),end_date timestamp(6) with time zone,
				job_class character varying(30),enabled character varying(5),auto_drop character varying(5),restartable character varying(5),
				state character varying(15),job_priority numeric,run_count numeric,max_runs numeric,failure_count numeric,max_failures numeric,
				retry_count numeric,last_start_date timestamp(6) with time zone,last_run_duration interval,next_run_date timestamp(6) with time zone,
				schedule_limit interval,max_run_duration interval,logging_level character varying(11),stop_on_window_close character varying(5),
				instance_stickiness character varying(5),raise_events character varying(4000),system character varying(5),job_weight numeric,
				nls_env character varying(4000),source character varying(128),number_of_destinations numeric,destination_owner character varying(384),
				destination character varying(384),credential_owner character varying(30),credential_name character varying(30),
				instance_id numeric,deferred_drop character varying(5),allow_runs_in_restricted_mode character varying(5),
				comments character varying(240),flags numeric)';
					WHEN replace(p_table, 'mig_','dba_') ilike ('dba_triggers') THEN
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT * FROM dblink_ora_record(''' || p_server || ''',''select * from SYS.DBA_TRIGGERS'')
AS t1(owner character varying(30),trigger_name character varying(30),trigger_type character varying(16),triggering_event character varying(227),
table_owner character varying(30),base_object_type character varying(16),table_name character varying(30),column_name character varying(4000),
referencing_names character varying(128),when_clause character varying(4000),status character varying(8),description character varying(4000),
action_type character varying(11),trigger_body text,crossedition character varying,before_statement character varying(3),before_row character varying(3),
after_row character varying(3),after_statement character varying(3),instead_of_row character varying(3),fire_once character varying(3),apply_server_only character varying(3))';
					WHEN replace(p_table, 'mig_','all_') ilike ('all_cons_columns') THEN
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT * FROM SYS.ALL_CONS_COLUMNS@' || p_server;
					WHEN replace(p_table, 'mig_','dba_') ilike ('DBA_OBJ_AUDIT_OPTS') THEN
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT owner, object_name, object_type,alt,aud,com,del,gra,ind,ins,loc,ren,sel,upd,ref,exe FROM SYS.DBA_OBJ_AUDIT_OPTS@' || p_server || ' WHERE alt !=''''-/-'''' or aud !=''''-/-'''' or com !=''''-/-'''' or del !=''''-/-'''' or gra !=''''-/-''''
						or ind !=''''-/-''''  or ins !=''''-/-'''' or loc !=''''-/-'''' or ren !=''''-/-'''' or sel !=''''-/-''''
						or upd !=''''-/-'''' or ref !=''''-/-'''' or exe !=''''-/-''''';
					WHEN replace(p_table, 'mig_','dba_') ilike ('DBA_NETWORK_ACLS') THEN
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT host, acl, lower_port, upper_port FROM SYS.DBA_NETWORK_ACLS@' || p_server;
					WHEN replace(p_table, 'mig_','dba_') ilike ('dba_network_acl_privileges') THEN
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT host, acl, lower_port, upper_port FROM dba_network_acl_privileges@' || p_server || 'p, dba_users@' || p_server || 'u WHERE u.username=p.principal
										  UNION ALL
										  SELECT acl,principal,privilege,is_grant,start_date,end_date
										  FROM dba_network_acl_privileges@' || p_server || 'p, dba_users@' || p_server || 'u WHERE r.role=p.principal' ;
					ELSE
						EXECUTE 'INSERT INTO ' || v_table_schema || '.' || p_table || ' SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID, DEFAULT_LENGTH, NUM_DISTINCT, LOW_VALUE,
										  HIGH_VALUE, DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE, CHARACTER_SET_NAME, 
										  CHAR_COL_DECL_LENGTH, GLOBAL_STATS, USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED, V80_FMT_IMAGE,
										  DATA_UPGRADED, HISTOGRAM FROM SYS.DBA_TAB_COLUMNS@' || p_server;
				END CASE;
			ELSE
				EXECUTE 'DROP TABLE ' || v_table_schema || '.' || p_table;
				EXECUTE 'CREATE TABLE ' || v_table_schema || '.' || p_table || ' AS SELECT * FROM ' || v_table_schema || '.' || replace(p_table, 'mig_', 'ora_');
			END IF;
		ELSE
			RAISE EXCEPTION 'Table % doesn''t exist', p_table;
		END IF;
		RETURN true;
    ELSE
        RAISE EXCEPTION 'Server % doesn''t exist', p_server;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.update_oracle_tables_rows('pg_ora', null);
CREATE FUNCTION update_oracle_tables_rows(p_server text, p_schema text) RETURNS boolean AS
$$
DECLARE
	v_exist text;
	v_list record;
	v_table text;
BEGIN
	SELECT srvname::text INTO v_exist FROM pg_foreign_server WHERE srvname ilike p_server;
	IF found THEN
		if p_schema is null THEN
			p_schema := 'check_orapg';
		END IF;
		v_table := p_schema || '.mig_tables';
		EXECUTE 'truncate table ' || p_schema || '.mig_rows_tables_oracle';
		FOR v_list IN EXECUTE 'SELECT owner, table_name FROM ' || v_table || ' WHERE owner NOT IN (''SYS'',''OUTLN'',''SYSTEM'',''DBSNMP'',''APPQOSSYS'',''WMSYS'')'
		LOOP
			IF position('EnterpriseDB' in version()) > 0 THEN
				EXECUTE 'INSERT INTO ' || p_schema || '.' || 'mig_rows_tables_oracle VALUES (''' || v_list.owner ||''', ''' || v_list.table_name || ''', (SELECT count(*) FROM ' || v_list.owner || '."' || v_list.table_name || '"@' || p_server || '))';
			ELSE
				EXECUTE 'CREATE FOREIGN TABLE ' || p_schema || '.' || v_list.owner || '_' || v_list.table_name || '(
				total bigint
				) SERVER '|| v_exist || ' options (table ''(SELECT count(*) FROM ' || v_list.owner || '."' || v_list.table_name || '")'')';
				EXECUTE 'INSERT INTO ' || p_schema || '.' || 'mig_rows_tables_oracle VALUES (''' || v_list.owner ||''', ''' || v_list.table_name || ''', (SELECT total FROM '|| p_schema || '.' || v_list.owner || '_' || v_list.table_name || '))';
				EXECUTE 'DROP FOREIGN TABLE ' || p_schema || '.' || v_list.owner || '_' || v_list.table_name;
			END IF;
		END LOOP;
		RETURN true;
	ELSE
		RAISE EXCEPTION 'Server % doesn''t exist', p_server;
	END IF;
END;
$$ LANGUAGE plpgsql;


-- PostgreSQL validation
CREATE TABLE postgres_validation (
    schema_ref text NOT NULL,
    name_ref text NOT NULL,
    description text NOT NULL,
    total bigint,
    date_ref timestamp without time zone,
    table_attr_pkfkidx text
);


-- Oracle validation
CREATE TABLE oracle_validation (
    schema_ref text NOT NULL,
    name_ref text NOT NULL,
    description text NOT NULL,
    total bigint,
    date_ref timestamp without time zone,
    table_attr_pkfkidx text
);


-- Diferences with Oracle
-- CREATE TABLE oracle_diferences (
--     description text,
--     role_ref text,
--     schema_ref text,
--     object_ref text,
--     privilege text,
--     sentence text,
--     date_ref timestamp without time zone,
--     execution_error text,
--     execution_date timestamp without time zone
-- );



--
-- Validation functions
--


-- GLobal objects
-- SELECT * FROM check_orapg.cluster_postgres() AS (s_object text, s_total int) ORDER BY 1;
CREATE FUNCTION cluster_postgres() RETURNS SETOF record AS
$$
DECLARE
	v_search_path text;
	v_res record;
	v_total_tbspc int;
	v_total_users int;
	v_total_roles int;
	v_total_directories int;
	v_total_profiles int;
	v_total_dblinks int;
	v_total_jobs int;
	v_total_schjobs int;
	v_total_synonyms int;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
    EXECUTE 'set search_path to ' || v_search_path;
    
    SELECT count(*) INTO v_total_tbspc FROM pg_tablespace;
	RETURN NEXT ('Tablespaces'::text, v_total_tbspc);
	SELECT count(*) INTO v_total_users FROM pg_roles WHERE rolcanlogin = 't' and rolname NOT IN ('postgres','enterprisedb','pgbouncer');
	RETURN NEXT ('Users'::text, v_total_users);
	SELECT count(*) INTO v_total_roles FROM pg_roles WHERE rolcanlogin = 'f' AND rolname NOT IN ('pg_monitor','pg_read_all_settings','pg_read_all_stats','pg_signal_backend','pg_stat_scan_tables','pg_execute_server_program','pg_read_server_files','pg_write_server_files');
	RETURN NEXT ('Roles'::text, v_total_roles);
	SELECT count(*) INTO v_total_dblinks FROM pg_foreign_server WHERE srvname not in (SELECT srvname FROM show_servers());
	RETURN NEXT ('DBLinks'::text, v_total_dblinks);
	IF position('EnterpriseDB' in version()) > 0 THEN
		SELECT count(*) INTO v_total_directories FROM dba_directories;
		RETURN NEXT ('Directories'::text, v_total_directories);
		SELECT count(*) INTO v_total_profiles FROM dba_profiles WHERE resource_name not ilike 'PASSWORD_ALLOW_HASHED';
		RETURN NEXT ('Profiles'::text, v_total_profiles);
		SELECT count(*) INTO v_total_synonyms FROM pg_synonym;
		RETURN NEXT ('Synonyms'::text, v_total_synonyms);
	ELSE
		RETURN NEXT ('Directories'::text, 0);
		RETURN NEXT ('Profiles'::text, 0);
		RETURN NEXT ('Synonyms'::text, 0);
	END IF;
	IF (SELECT nspname FROM pg_namespace n JOIN pg_class c ON (n.oid=c.relnamespace) WHERE nspname ILIKE 'pgagent') = 'pgagent' THEN
		SELECT count(*) INTO v_total_jobs FROM pgagent.pga_job;
		RETURN NEXT ('Jobs'::text, v_total_jobs);
		SELECT count(*) INTO v_total_schjobs FROM pgagent.pga_schedule;
		RETURN NEXT ('Scheduled jobs'::text, v_total_schjobs);
	ELSE
		RETURN NEXT ('Jobs'::text, 0);
		RETURN NEXT ('Scheduled jobs'::text, 0);
	END IF;  
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.cluster_oracle('yudita') AS (s_object text, s_total int) ORDER BY 1;
CREATE FUNCTION cluster_oracle(p_schemas text) RETURNS SETOF record AS
$$
DECLARE
	v_res record;
	v_total_tbspc int;
	v_total_users int;
	v_total_roles int;
	v_total_directories int;
	v_total_profiles int;
	v_total_dblinks int;
	v_total_jobs int;
	v_total_schjobs int;
	v_total_synonyms int;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	SELECT count(*) INTO v_total_tbspc FROM mig_tablespaces;
	RETURN NEXT ('Tablespaces'::text, v_total_tbspc);
	SELECT count(*) INTO v_total_users FROM mig_users WHERE username NOT IN ('ANONYMOUS','APEX_PUBLIC_USER','APPQOSSYS','CTXSYS','DIP','EXFSYS','FLOWS_FILES','MDDATA','OLAPSYS','ORACLE_OCM','ORDDATA','OUTLN','OWBSYS','SCOTT','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','SYS','SYSTEM','WMSYS','XDB','XS$NULL');
	RETURN NEXT ('Users'::text, v_total_users);
	SELECT count(*) INTO v_total_roles FROM mig_roles WHERE role NOT IN ('ADM_PARALLEL_EXECUTE_TASK','APEX_ADMINISTRATOR_ROLE','AQ_USER_ROLE',
		'AUTHENTICATEDUSER','CONNECT','CSW_USR_ROLE','CTXAPP','DATAPUMP_EXP_FULL_DATABASE','DATAPUMP_IMP_FULL_DATABASE','DBA','EJBCLIENT',
		'EXECUTE_CATALOG_ROLE','EXP_FULL_DATABASE','GATHER_SYSTEM_STATISTICS','GLOBAL_AQ_USER_ROLE','HS_ADMIN_EXECUTE_ROLE','HS_ADMIN_ROLE',
		'HS_ADMIN_SELECT_ROLE','IMP_FULL_DATABASE','JAVA_ADMIN','JAVADEBUGPRIV','JAVA_DEPLOY','JAVAIDPRIV','JAVASYSPRIV','JAVAUSERPRIV','JMXSERVER',
		'LOGSTDBY_ADMINISTRATOR','OEM_ADVISOR','OEM_MONITOR','DBFS_ROLE','ORDADMIN','RECOVERY_CATALOG_OWNER','RESOURCE','SCHEDULER_ADMIN',
		'SELECT_CATALOG_ROLE','SPATIAL_CSW_ADMIN','SPATIAL_WFS_ADMIN','WFS_USR_ROLE','WM_ADMIN_ROLE','XDBADMIN','XDB_SET_INVOKER',
		'XDB_WEBSERVICES','XDB_WEBSERVICES_OVER_HTTP','XDB_WEBSERVICES_WITH_PUBLIC','DELETE_CATALOG_ROLE');
	RETURN NEXT ('Roles'::text, v_total_roles);
	SELECT count(*) INTO v_total_dblinks FROM mig_db_links;
	RETURN NEXT ('DBLinks'::text, v_total_dblinks);
	SELECT count(*) INTO v_total_directories FROM mig_directories;
	RETURN NEXT ('Directories'::text, v_total_directories);
	SELECT count(*) INTO v_total_profiles FROM mig_profiles WHERE resource_type='PASSWORD';
	RETURN NEXT ('Profiles'::text, v_total_profiles);
	SELECT count(*) INTO v_total_synonyms FROM mig_synonyms WHERE table_owner NOT IN ('SYS','APEX_030200','APPQOSSYS','DBSNMP','FLOWS_FILES','OLAPSYS','ORDDATA','OWBSYS','SYSMAN','SYSTEM','WMSYS','XDB','QPROD1') AND table_owner IN (upper($1),'PUBLIC');
	RETURN NEXT ('Synonyms'::text, v_total_synonyms);  
	SELECT count(*) INTO v_total_jobs FROM mig_jobs WHERE schema_user NOT IN ('SYS');
	RETURN NEXT ('Jobs'::text, v_total_jobs);
	SELECT count(*) INTO v_total_schjobs FROM mig_scheduler_jobs WHERE owner NOT IN ('SYS');
	RETURN NEXT ('Scheduled jobs'::text, v_total_schjobs);
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- User privileges for a schema list
-- SELECT * FROM check_orapg.users_privileges_postgres('''public'',''check_orapg''') AS (s_user text, s_asigned_privileges bigint) ORDER BY 1;
-- SELECT sum(s_asigned_privileges) FROM check_orapg.users_privileges_postgres('''public'',''check_orapg''') AS (s_user text, s_asigned_privileges bigint) ORDER BY 1;
CREATE FUNCTION users_privileges_postgres(p_schema_list text) RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
BEGIN
	v_query := '
	select rolname::text, count(*) filter (where s_schema is not null)
	from (with login_roles as (
		select pr.rolname from pg_roles pr where pr.rolcanlogin = ''t'' and pr.rolname not in (''postgres'',''enterprisedb'',''pgbouncer''))
	select rg.rolname::text,b.s_schema::text,b.s_object::text,b.s_privilege::text from login_roles rg left join (
	--Tables/views/foreign tablas
	select grantee::text as s_role, table_schema::text as s_schema, table_name::text as s_object, privilege_type::text as s_privilege
	from information_schema.table_privileges where table_schema in ('||$1||') and grantor <> grantee
	--Sequences/materialized views
	union
	(select s_role, s_schema, s_object,
		case
		when s_p = ''r'' then ''SELECT''
		when s_p = ''U'' then ''USAGE''
		when s_p = ''w'' then ''UPDATE''
		when s_p = ''a'' then ''INSERT''
		when s_p = ''d'' then ''DELETE''
		when s_p = ''D'' then ''TRUNCATE''
		when s_p = ''x'' then ''REFERENCES''
		when s_p = ''t'' then ''TRIGGER''
		when s_p = ''X'' then ''EXECUTE''
		when s_p = ''C'' then ''CREATE''
		when s_p = ''c'' then ''CONNECT''
		when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
	from (with cw_class as (
		select pn.nspname::text, pc.relname::text, unnest(pc.relacl)::text as s_privilege
		from pg_class pc join pg_namespace pn on (pc.relnamespace=pn.oid)
		where pn.nspname in ('||$1||') and pc.relkind in (''S'',''m'')) --(''r'',''v'',''m'',''S'',''f''))
	select substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text as s_role, nspname::text as s_schema, relname::text as s_object,
	unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
	from cw_class) y where s_p <> ''*'')
	--Functions/procedures
	union
	(select s_role, s_schema, s_object,
		case
		when s_p = ''r'' then ''SELECT''
		when s_p = ''U'' then ''USAGE''
		when s_p = ''w'' then ''UPDATE''
		when s_p = ''a'' then ''INSERT''
		when s_p = ''d'' then ''DELETE''
		when s_p = ''D'' then ''TRUNCATE''
		when s_p = ''x'' then ''REFERENCES''
		when s_p = ''t'' then ''TRIGGER''
		when s_p = ''X'' then ''EXECUTE''
		when s_p = ''C'' then ''CREATE''
		when s_p = ''c'' then ''CONNECT''
		when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
	from (with cw_proc as (
		select pn.nspname::text, pc.proname::text, unnest(pc.proacl)::text as s_privilege from pg_proc pc join pg_namespace pn on (pc.pronamespace=pn.oid) where pn.nspname in ('||$1||'))
	select case when substring(s_privilege from 1 for position(''='' in s_privilege) - 1) = '''' then nspname::text else substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text end as s_role, nspname::text as s_schema, proname::text as s_object, --privilege::text
	unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
	from cw_proc) y where s_p <> ''*'')';
	IF position('EnterpriseDB' in version()) > 0 THEN
		v_query := v_query || 
		'--Packages
		union
		(select s_role, s_schema, s_object,
			case
			when s_p = ''r'' then ''SELECT''
			when s_p = ''U'' then ''USAGE''
			when s_p = ''w'' then ''UPDATE''
			when s_p = ''a'' then ''INSERT''
			when s_p = ''d'' then ''DELETE''
			when s_p = ''D'' then ''TRUNCATE''
			when s_p = ''x'' then ''REFERENCES''
			when s_p = ''t'' then ''TRIGGER''
			when s_p = ''X'' then ''EXECUTE''
			when s_p = ''C'' then ''CREATE''
			when s_p = ''c'' then ''CONNECT''
			when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
		from (with cw_pkg as (
			select pn.nspname::text, pk.pkgname::text, unnest(pk.pkgacl)::text as s_privilege
			from edb_package pk join pg_namespace pn on (pk.pkgnamespace=pn.oid)
			where pn.nspname in ('||$1||'))
		select substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text as s_role, nspname::text as s_schema, pkgname::text as s_object,
		unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
		from cw_pkg) y where s_p <> ''*'' and s_role <> '''')
		) b on (rg.rolname=b.s_role)
		/*where rolname <> s_schema or (rolname is not null and s_schema is null)*/) z
		group by rolname
		order by 1';
	ELSE
		v_query := v_query || 
		') b on (rg.rolname=b.s_role)
		/*where rolname <> s_schema or (rolname is not null and s_schema is null)*/) z
		group by rolname
		order by 1';
	END IF;
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.users_privileges_oracle('''YUDITA''') AS (s_user text, s_asigned_privileges bigint) order by 1;
-- SELECT sum(s_asigned_privileges) FROM check_orapg.users_privileges_oracle('''YUDITA''') AS (s_user text, s_asigned_privileges bigint) order by 1;
CREATE FUNCTION users_privileges_oracle(p_schema_list text) RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	v_query := '
	with login_roles as (
	select pr.username from mig_users pr where pr.username not in (''ANONYMOUS'',
		''APEX_PUBLIC_USER'',''APPQOSSYS'',''CTXSYS'',''DIP'',''EXFSYS'',''FLOWS_FILES'',''MDDATA'',''OLAPSYS'',
		''ORACLE_OCM'',''ORDDATA'',''OUTLN'',''OWBSYS'',''SCOTT'',''SPATIAL_CSW_ADMIN_USR'',''SPATIAL_WFS_ADMIN_USR'',
		''SYS'',''SYSTEM'',''WMSYS'',''XDB'',''XS$NULL''))
	select rl.username::text, count(*) filter (where grantee is not null)
	from login_roles rl left join
	(select * from mig_tab_privs where grantee in (select username from mig_users) and owner in ('||$1||')
	and privilege not in (''DEBUG'')
	and table_name not in (''QUEST_SL_TEMP_EXPLAIN1'',''TBL_TEMP_COMP'') and grantee <> ''DBA'') tp on (rl.username=tp.grantee)
		and (privilege <> ''ALTER'' or table_name not in (select object_name from mig_objects where object_type = ''SEQUENCE''))
		and privilege not in (''DEBUG'',''INDEX'',''MERGE VIEW'',''FLASHBACK'')
		and lower(table_name) not in (''prcs_arr'',''value_array'',''usuarios_array'',''sys_plsql_211337_29_1'',''sys_plsql_211337_55_1'',''sys_plsql_211337_90_1'',''name_array'')
		and grantor <> ''SYS''
	group by rl.username order by 1';
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- User privileges for all schemas
-- SELECT * FROM check_orapg.users_privileges_postgres() AS (s_user text, s_asigned_privileges bigint) ORDER BY 1;
-- SELECT sum(s_asigned_privileges) FROM check_orapg.users_privileges_postgres() AS (s_user text, s_asigned_privileges bigint) ORDER BY 1;
CREATE FUNCTION users_privileges_postgres() RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
BEGIN
	v_query := '
	select rolname::text, count(*) filter (where s_schema is not null)
	from (with login_roles as (
		select pr.rolname from pg_roles pr where pr.rolcanlogin = ''t'' and pr.rolname not in (''postgres'',''enterprisedb'',''pgbouncer''))
	select rg.rolname::text,b.s_schema::text,b.s_object::text,b.s_privilege::text from login_roles rg left join (
	--Tables/views/foreign tables
	select grantee::text as s_role, table_schema::text as s_schema, table_name::text as s_object, privilege_type::text as s_privilege
	from information_schema.table_privileges where table_schema not in (''pg_catalog'',''information_schema'',''pgagent'')
	--Sequences/materialized views
	union
	(select s_role, s_schema, s_object,
		case
		when s_p = ''r'' then ''SELECT''
		when s_p = ''U'' then ''USAGE''
		when s_p = ''w'' then ''UPDATE''
		when s_p = ''a'' then ''INSERT''
		when s_p = ''d'' then ''DELETE''
		when s_p = ''D'' then ''TRUNCATE''
		when s_p = ''x'' then ''REFERENCES''
		when s_p = ''t'' then ''TRIGGER''
		when s_p = ''X'' then ''EXECUTE''
		when s_p = ''C'' then ''CREATE''
		when s_p = ''c'' then ''CONNECT''
		when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
	from (with cw_class as (
		select pn.nspname::text, pc.relname::text, unnest(pc.relacl)::text as s_privilege
		from pg_class pc join pg_namespace pn on (pc.relnamespace=pn.oid)
		where pn.nspname not in (''pg_catalog'',''information_schema'',''pgagent'') and pc.relkind in (''S'',''m'')) --(''r'',''v'',''m'',''S'',''f''))
	select substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text as s_role, nspname::text as s_schema, relname::text as s_object, --privilege::text
	unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
	from cw_class) y where s_p <> ''*'')
	--Functions/procedures
	union
	(select s_role, s_schema, s_object,
		case
		when s_p = ''r'' then ''SELECT''
		when s_p = ''U'' then ''USAGE''
		when s_p = ''w'' then ''UPDATE''
		when s_p = ''a'' then ''INSERT''
		when s_p = ''d'' then ''DELETE''
		when s_p = ''D'' then ''TRUNCATE''
		when s_p = ''x'' then ''REFERENCES''
		when s_p = ''t'' then ''TRIGGER''
		when s_p = ''X'' then ''EXECUTE''
		when s_p = ''C'' then ''CREATE''
		when s_p = ''c'' then ''CONNECT''
		when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
	from (with cw_proc as (
		select pn.nspname::text, pc.proname::text, unnest(pc.proacl)::text as s_privilege from pg_proc pc join pg_namespace pn on (pc.pronamespace=pn.oid) where pn.nspname not in (''pg_catalog'',''information_schema'',''pgagent''))
	select case when substring(s_privilege from 1 for position(''='' in s_privilege) - 1) = '''' then nspname::text else substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text end as s_role, nspname::text as s_schema, proname::text as s_object, --privilege::text
	unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
	from cw_proc) y where s_p <> ''*'')';
	IF position('EnterpriseDB' in version()) > 0 THEN
		v_query := v_query || 
		'--Packages
		union
		(select s_role, s_schema, s_object,
			case
			when s_p = ''r'' then ''SELECT''
			when s_p = ''U'' then ''USAGE''
			when s_p = ''w'' then ''UPDATE''
			when s_p = ''a'' then ''INSERT''
			when s_p = ''d'' then ''DELETE''
			when s_p = ''D'' then ''TRUNCATE''
			when s_p = ''x'' then ''REFERENCES''
			when s_p = ''t'' then ''TRIGGER''
			when s_p = ''X'' then ''EXECUTE''
			when s_p = ''C'' then ''CREATE''
			when s_p = ''c'' then ''CONNECT''
			when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
		from (with cw_pkg as (
			select pn.nspname::text, pk.pkgname::text, unnest(pk.pkgacl)::text as s_privilege
			from edb_package pk join pg_namespace pn on (pk.pkgnamespace=pn.oid)
			where pn.nspname not in (''pg_catalog'',''information_schema'',''pgagent''))
		select substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text as s_role, nspname::text as s_schema, pkgname::text as s_object,
		unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
		from cw_pkg) y where s_p <> ''*'' and s_role <> '''')
		) b on (rg.rolname=b.s_role)
		where rolname <> s_schema or (rolname is not null and s_schema is null)) z
		group by rolname
		order by 1';
	ELSE
		v_query := v_query || 
		') b on (rg.rolname=b.s_role)
		where rolname <> s_schema or (rolname is not null and s_schema is null)) z
		group by rolname
		order by 1';
	END IF;
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.users_privileges_oracle() AS (s_role text, s_asigned_privileges bigint) ORDER BY 1;
-- SELECT sum(s_asigned_privileges) FROM check_orapg.users_privileges_oracle() AS (s_role text, s_asigned_privileges bigint) ORDER BY 1;
CREATE FUNCTION users_privileges_oracle() RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	v_query := '
	with roles_login as (
	select pr.role from mig_roles pr where role not in (''ADM_PARALLEL_EXECUTE_TASK'',''APEX_ADMINISTRATOR_ROLE'',
		''AQ_USER_ROLE'',''AUTHENTICATEDUSER'',''CONNECT'',''CSW_USR_ROLE'',''CTXAPP'',''DATAPUMP_EXP_FULL_DATABASE'',''DATAPUMP_IMP_FULL_DATABASE'',
		''DBA'',''EJBCLIENT'',''EXECUTE_CATALOG_ROLE'',''EXP_FULL_DATABASE'',''GATHER_SYSTEM_STATISTICS'',''GLOBAL_AQ_USER_ROLE'',
		''HS_ADMIN_EXECUTE_ROLE'',''HS_ADMIN_ROLE'',''HS_ADMIN_SELECT_ROLE'',''IMP_FULL_DATABASE'',''JAVA_ADMIN'',''JAVADEBUGPRIV'',
		''JAVA_DEPLOY'',''JAVAIDPRIV'',''JAVASYSPRIV'',''JAVAUSERPRIV'',''JMXSERVER'',''LOGSTDBY_ADMINISTRATOR'',''OEM_ADVISOR'',''OEM_MONITOR'',
		''DBFS_ROLE'',''ORDADMIN'',''RECOVERY_CATALOG_OWNER'',''RESOURCE'',''SCHEDULER_ADMIN'',''SELECT_CATALOG_ROLE'',''SPATIAL_CSW_ADMIN'',
		''SPATIAL_WFS_ADMIN'',''WFS_USR_ROLE'',''WM_ADMIN_ROLE'',''XDBADMIN'',''XDB_SET_INVOKER'',''XDB_WEBSERVICES'',''XDB_WEBSERVICES_OVER_HTTP'',
		''XDB_WEBSERVICES_WITH_PUBLIC'',''DELETE_CATALOG_ROLE''))
	select rl.role::text, count(*) filter (where grantee is not null)
	from roles_login rl left join
	(select * from mig_TAB_PRIVS where grantee in (select role from mig_roles) and owner not in (''SYS'')
	and privilege not in (''DEBUG'')
	and table_name not in (''QUEST_SL_TEMP_EXPLAIN1'',''TBL_TEMP_COMP'') and grantee <> ''DBA'') tp on (rl.role=tp.grantee)
		and (privilege <> ''ALTER'' or table_name not in (select object_name from mig_objects where object_type = ''SEQUENCE''))
		and privilege not in (''DEBUG'',''INDEX'',''MERGE VIEW'',''FLASHBACK'')
		and lower(table_name) not in (''prcs_arr'',''value_array'',''usuarios_array'',''sys_plsql_211337_29_1'',''sys_plsql_211337_55_1'',''sys_plsql_211337_90_1'',''name_array'')
		and grantor <> ''SYS''
	group by rl.role order by 1';
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Role privileges from a schema list
-- SELECT * FROM check_orapg.roles_privileges_postgres('''public'',''check_orapg''') AS (s_role text, s_asigned_privileges bigint) ORDER BY 1;
-- SELECT sum(s_asigned_privileges) FROM check_orapg.roles_privileges_postgres('''public'',''check_orapg''') AS (s_role text, s_asigned_privileges bigint) ORDER BY 1;
CREATE FUNCTION roles_privileges_postgres(p_schema_list text) RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
BEGIN
	v_query := '
	select rolname::text, count(*) filter (where s_schema is not null) as v_total
	from (with group_roles as (
		select pr.rolname from pg_roles pr where pr.rolcanlogin = ''f'' and pr.rolname not in (''enterprisedb'',''pgbouncer'',''postgres'',''pg_monitor'',''pg_read_all_settings'',''pg_read_all_stats'',''pg_signal_backend'',''pg_stat_scan_tables'',''pg_execute_server_program'',''pg_read_server_files'',''pg_write_server_files'',''pg_execute_server_program'',''pg_read_server_files'',''pg_write_server_files''))
	select rg.rolname::text,b.s_schema::text,b.s_object::text,b.s_privilege::text from group_roles rg left join (
	--Tables/views/foreign tables
	select grantee::text as s_role, table_schema::text as s_schema, table_name::text as s_object, privilege_type::text as s_privilege
	from information_schema.table_privileges where table_schema in ('||$1||')
	--Sequences/materialized views
	union
	(select s_role, s_schema, s_object,
		case
		when s_p = ''r'' then ''SELECT''
		when s_p = ''U'' then ''USAGE''
		when s_p = ''w'' then ''UPDATE''
		when s_p = ''a'' then ''INSERT''
		when s_p = ''d'' then ''DELETE''
		when s_p = ''D'' then ''TRUNCATE''
		when s_p = ''x'' then ''REFERENCES''
		when s_p = ''t'' then ''TRIGGER''
		when s_p = ''X'' then ''EXECUTE''
		when s_p = ''C'' then ''CREATE''
		when s_p = ''c'' then ''CONNECT''
		when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
	from (with cw_class as (
		select pn.nspname::text, pc.relname::text, unnest(pc.relacl)::text as s_privilege
		from pg_class pc join pg_namespace pn on (pc.relnamespace=pn.oid)
		where pn.nspname in ('||$1||') and pc.relkind in (''S'',''m'')) --(''r'',''v'',''m'',''S'',''f''))
	select substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text as s_role, nspname::text as s_schema, relname::text as s_object, --privilege::text
	unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
	from cw_class) y where s_p <> ''*'')
	--Functions/procedures
	union
	(select s_role, s_schema, s_object,
		case
		when s_p = ''r'' then ''SELECT''
		when s_p = ''U'' then ''USAGE''
		when s_p = ''w'' then ''UPDATE''
		when s_p = ''a'' then ''INSERT''
		when s_p = ''d'' then ''DELETE''
		when s_p = ''D'' then ''TRUNCATE''
		when s_p = ''x'' then ''REFERENCES''
		when s_p = ''t'' then ''TRIGGER''
		when s_p = ''X'' then ''EXECUTE''
		when s_p = ''C'' then ''CREATE''
		when s_p = ''c'' then ''CONNECT''
		when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
	from (with cw_proc as (
		select pn.nspname::text, pc.proname::text, unnest(pc.proacl)::text as s_privilege from pg_proc pc join pg_namespace pn on (pc.pronamespace=pn.oid) where pn.nspname in ('||$1||'))
	select case when substring(s_privilege from 1 for position(''='' in s_privilege) - 1) = '''' then nspname::text else substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text end as s_role, nspname::text as s_schema, proname::text as s_object, --privilege::text
	unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
	from cw_proc) y where s_p <> ''*'')';
	IF position('EnterpriseDB' in version()) > 0 THEN
		v_query := v_query || 
		'--Packages
		union
		(select s_role, s_schema, s_object,
			case
			when s_p = ''r'' then ''SELECT''
			when s_p = ''U'' then ''USAGE''
			when s_p = ''w'' then ''UPDATE''
			when s_p = ''a'' then ''INSERT''
			when s_p = ''d'' then ''DELETE''
			when s_p = ''D'' then ''TRUNCATE''
			when s_p = ''x'' then ''REFERENCES''
			when s_p = ''t'' then ''TRIGGER''
			when s_p = ''X'' then ''EXECUTE''
			when s_p = ''C'' then ''CREATE''
			when s_p = ''c'' then ''CONNECT''
			when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
		from (with cw_pkg as (
			select pn.nspname::text, pk.pkgname::text, unnest(pk.pkgacl)::text as s_privilege
			from edb_package pk join pg_namespace pn on (pk.pkgnamespace=pn.oid)
			where pn.nspname in ('||$1||'))
		select substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text as s_role, nspname::text as s_schema, pkgname::text as s_object,
		unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
		from cw_pkg) y where s_p <> ''*'' and s_role <> '''')
		) b on (rg.rolname=b.s_role)
		where rolname <> s_schema or (rolname is not null and s_schema is null)) z
		group by rolname
		order by 1';
	ELSE
		v_query := v_query || 
		') b on (rg.rolname=b.s_role)
		where rolname <> s_schema or (rolname is not null and s_schema is null)) z
		group by rolname
		order by 1';
	END IF;
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.roles_privileges_oracle('''YUDITA''') AS (s_role text, s_asigned_privileges bigint) order by 1
-- SELECT sum(s_asigned_privileges) FROM check_orapg.roles_privileges_oracle('''YUDITA''') AS (s_role text, s_asigned_privileges bigint) ORDER BY 1;
CREATE FUNCTION roles_privileges_oracle(p_schema text) RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	v_query := '
	with login_roles as (
	select pr.role from mig_roles pr where role not in (''ADM_PARALLEL_EXECUTE_TASK'',''APEX_ADMINISTRATOR_ROLE'',
		''AQ_USER_ROLE'',''AUTHENTICATEDUSER'',''CONNECT'',''CSW_USR_ROLE'',''CTXAPP'',''DATAPUMP_EXP_FULL_DATABASE'',''DATAPUMP_IMP_FULL_DATABASE'',
		''DBA'',''EJBCLIENT'',''EXECUTE_CATALOG_ROLE'',''EXP_FULL_DATABASE'',''GATHER_SYSTEM_STATISTICS'',''GLOBAL_AQ_USER_ROLE'',
		''HS_ADMIN_EXECUTE_ROLE'',''HS_ADMIN_ROLE'',''HS_ADMIN_SELECT_ROLE'',''IMP_FULL_DATABASE'',''JAVA_ADMIN'',''JAVADEBUGPRIV'',
		''JAVA_DEPLOY'',''JAVAIDPRIV'',''JAVASYSPRIV'',''JAVAUSERPRIV'',''JMXSERVER'',''LOGSTDBY_ADMINISTRATOR'',''OEM_ADVISOR'',''OEM_MONITOR'',
		''DBFS_ROLE'',''ORDADMIN'',''RECOVERY_CATALOG_OWNER'',''RESOURCE'',''SCHEDULER_ADMIN'',''SELECT_CATALOG_ROLE'',''SPATIAL_CSW_ADMIN'',
		''SPATIAL_WFS_ADMIN'',''WFS_USR_ROLE'',''WM_ADMIN_ROLE'',''XDBADMIN'',''XDB_SET_INVOKER'',''XDB_WEBSERVICES'',''XDB_WEBSERVICES_OVER_HTTP'',
		''XDB_WEBSERVICES_WITH_PUBLIC'',''DELETE_CATALOG_ROLE''))
	select rl.role::text, count(*) filter (where grantee is not null)
	from login_roles rl left join
	(select * from mig_TAB_PRIVS where grantee in (select role from mig_roles) and owner in ('||$1||')
	and privilege not in (''DEBUG'')
	and table_name not in (''QUEST_SL_TEMP_EXPLAIN1'',''TBL_TEMP_COMP'') and grantee <> ''DBA'') tp on (rl.role=tp.grantee)
		and (privilege <> ''ALTER'' or table_name not in (select object_name from mig_objects where object_type = ''SEQUENCE''))
		and privilege not in (''DEBUG'',''INDEX'',''MERGE VIEW'',''FLASHBACK'')
		and lower(table_name) not in (''prcs_arr'',''value_array'',''usuarios_array'',''sys_plsql_211337_29_1'',''sys_plsql_211337_55_1'',''sys_plsql_211337_90_1'',''name_array'')
		and grantor <> ''SYS''
	group by rl.role order by 1';
	FOR v_res IN EXECUTE v_query
	LOOP
	RETURN NEXT v_res;
		END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Role privileges for all schemas
-- SELECT * FROM check_orapg.roles_privileges_postgres() AS (s_role text, s_asigned_privileges bigint) ORDER BY 1;
-- SELECT sum(s_asigned_privileges) FROM check_orapg.roles_privileges_postgres() AS (s_role text, s_asigned_privileges bigint) ORDER BY 1;
CREATE FUNCTION roles_privileges_postgres() RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
BEGIN
	v_query := '
	select rolname::text, count(*) filter (where s_schema is not null) as v_total
	from (with group_roles as (
		select pr.rolname from pg_roles pr where pr.rolcanlogin = ''f'' and pr.rolname not in (''enterprisedb'',''pgbouncer'',''postgres'',''pg_monitor'',''pg_read_all_settings'',''pg_read_all_stats'',''pg_signal_backend'',''pg_stat_scan_tables'',''pg_execute_server_program'',''pg_read_server_files'',''pg_write_server_files'',''pg_execute_server_program'',''pg_read_server_files'',''pg_write_server_files''))
	select rg.rolname::text,b.s_schema::text,b.s_object::text,b.s_privilege::text from group_roles rg left join (
	--Tables/Views/Foreign tables
	select grantee::text as s_role, table_schema::text as s_schema, table_name::text as s_object, privilege_type::text as s_privilege
	from information_schema.table_privileges where table_schema not in (''pg_catalog'',''information_schema'',''pgagent'')
	--Privilegios sobre secuencias y vistas materializadas
	union
	(select s_role, s_schema, s_object,
		case
		when s_p = ''r'' then ''SELECT''
		when s_p = ''U'' then ''USAGE''
		when s_p = ''w'' then ''UPDATE''
		when s_p = ''a'' then ''INSERT''
		when s_p = ''d'' then ''DELETE''
		when s_p = ''D'' then ''TRUNCATE''
		when s_p = ''x'' then ''REFERENCES''
		when s_p = ''t'' then ''TRIGGER''
		when s_p = ''X'' then ''EXECUTE''
		when s_p = ''C'' then ''CREATE''
		when s_p = ''c'' then ''CONNECT''
		when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
	from (with cw_class as (
		select pn.nspname::text, pc.relname::text, unnest(pc.relacl)::text as s_privilege
		from pg_class pc join pg_namespace pn on (pc.relnamespace=pn.oid)
		where pn.nspname in (''pg_catalog'',''information_schema'',''pgagent'') and pc.relkind in (''S'',''m'')) --(''r'',''v'',''m'',''S'',''f''))
	select substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text as s_role, nspname::text as s_schema, relname::text as s_object, --privilege::text
	unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
	from cw_class) y where s_p <> ''*'')
	--Functions/Procedures
	union
	(select s_role, s_schema, s_object,
		case
		when s_p = ''r'' then ''SELECT''
		when s_p = ''U'' then ''USAGE''
		when s_p = ''w'' then ''UPDATE''
		when s_p = ''a'' then ''INSERT''
		when s_p = ''d'' then ''DELETE''
		when s_p = ''D'' then ''TRUNCATE''
		when s_p = ''x'' then ''REFERENCES''
		when s_p = ''t'' then ''TRIGGER''
		when s_p = ''X'' then ''EXECUTE''
		when s_p = ''C'' then ''CREATE''
		when s_p = ''c'' then ''CONNECT''
		when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
	from (with cw_proc as (
		select pn.nspname::text, pc.proname::text, unnest(pc.proacl)::text as s_privilege from pg_proc pc join pg_namespace pn on (pc.pronamespace=pn.oid) where pn.nspname in (''pg_catalog'',''information_schema'',''pgagent''))
	select case when substring(s_privilege from 1 for position(''='' in s_privilege) - 1) = '''' then nspname::text else substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text end as s_role, nspname::text as s_schema, proname::text as s_object, --privilege::text
	unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
	from cw_proc) y where s_p <> ''*'')';
	IF position('EnterpriseDB' in version()) > 0 THEN
		v_query := v_query || 
		'--Paqckages
		union
		(select s_role, s_schema, s_object,
			case
			when s_p = ''r'' then ''SELECT''
			when s_p = ''U'' then ''USAGE''
			when s_p = ''w'' then ''UPDATE''
			when s_p = ''a'' then ''INSERT''
			when s_p = ''d'' then ''DELETE''
			when s_p = ''D'' then ''TRUNCATE''
			when s_p = ''x'' then ''REFERENCES''
			when s_p = ''t'' then ''TRIGGER''
			when s_p = ''X'' then ''EXECUTE''
			when s_p = ''C'' then ''CREATE''
			when s_p = ''c'' then ''CONNECT''
			when s_p = ''T'' then ''TEMPORARY''
		end as s_privilege
		from (with cw_pkg as (
			select pn.nspname::text, pk.pkgname::text, unnest(pk.pkgacl)::text as s_privilege
			from edb_package pk join pg_namespace pn on (pk.pkgnamespace=pn.oid)
			where pn.nspname in (''pg_catalog'',''information_schema'',''pgagent''))
		select substring(s_privilege from 1 for position(''='' in s_privilege) - 1)::text as s_role, nspname::text as s_schema, pkgname::text as s_object,
		unnest(string_to_array((select string_agg(s_a, '','') from (select regexp_split_to_table(substring(s_privilege from position(''='' in s_privilege) + 1 for (position(''/'' in s_privilege) - 1)-position(''='' in s_privilege)), ''\s*'') as s_a) s_x),'','')) as s_p
		from cw_pkg) y where s_p <> ''*'' and s_role <> '''')
		) b on (rg.rolname=b.s_role)
		where rolname <> s_schema or (rolname is not null and s_schema is null)) z
		group by rolname
		order by 1';
	ELSE
		v_query := v_query || 
		') b on (rg.rolname=b.s_role)
		where rolname <> s_schema or (rolname is not null and s_schema is null)) z
		group by rolname
		order by 1';
	END IF;
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.roles_privileges_oracle() AS (s_role text, s_asigned_privileges bigint) order by 1
-- SELECT sum(s_asigned_privileges) FROM check_orapg.roles_privileges_oracle() AS (s_role text, s_asigned_privileges bigint) ORDER BY 1;
CREATE FUNCTION roles_privileges_oracle() RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	v_query := '
	with login_roles as (
	select pr.role from mig_roles pr where role not in (''ADM_PARALLEL_EXECUTE_TASK'',''APEX_ADMINISTRATOR_ROLE'',
		''AQ_USER_ROLE'',''AUTHENTICATEDUSER'',''CONNECT'',''CSW_USR_ROLE'',''CTXAPP'',''DATAPUMP_EXP_FULL_DATABASE'',''DATAPUMP_IMP_FULL_DATABASE'',
		''DBA'',''EJBCLIENT'',''EXECUTE_CATALOG_ROLE'',''EXP_FULL_DATABASE'',''GATHER_SYSTEM_STATISTICS'',''GLOBAL_AQ_USER_ROLE'',
		''HS_ADMIN_EXECUTE_ROLE'',''HS_ADMIN_ROLE'',''HS_ADMIN_SELECT_ROLE'',''IMP_FULL_DATABASE'',''JAVA_ADMIN'',''JAVADEBUGPRIV'',
		''JAVA_DEPLOY'',''JAVAIDPRIV'',''JAVASYSPRIV'',''JAVAUSERPRIV'',''JMXSERVER'',''LOGSTDBY_ADMINISTRATOR'',''OEM_ADVISOR'',''OEM_MONITOR'',
		''DBFS_ROLE'',''ORDADMIN'',''RECOVERY_CATALOG_OWNER'',''RESOURCE'',''SCHEDULER_ADMIN'',''SELECT_CATALOG_ROLE'',''SPATIAL_CSW_ADMIN'',
		''SPATIAL_WFS_ADMIN'',''WFS_USR_ROLE'',''WM_ADMIN_ROLE'',''XDBADMIN'',''XDB_SET_INVOKER'',''XDB_WEBSERVICES'',''XDB_WEBSERVICES_OVER_HTTP'',
		''XDB_WEBSERVICES_WITH_PUBLIC'',''DELETE_CATALOG_ROLE''))
	select rl.role::text, count(*) filter (where grantee is not null)
	from login_roles rl left join
	(select * from mig_TAB_PRIVS where grantee in (select role from mig_roles) and owner not in (''SYS'')
	and privilege not in (''DEBUG'')
	and table_name not in (''QUEST_SL_TEMP_EXPLAIN1'',''TBL_TEMP_COMP'') and grantee <> ''DBA'') tp on (rl.role=tp.grantee)
		and (privilege <> ''ALTER'' or table_name not in (select object_name from mig_objects where object_type = ''SEQUENCE''))
		and privilege not in (''DEBUG'',''INDEX'',''MERGE VIEW'',''FLASHBACK'')
		and lower(table_name) not in (''prcs_arr'',''value_array'',''usuarios_array'',''sys_plsql_211337_29_1'',''sys_plsql_211337_55_1'',''sys_plsql_211337_90_1'',''name_array'')
		and grantor <> ''SYS''
	group by rl.role order by 1';
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Objects for a schema list
/* 
-- En PostgreSQL
SELECT * FROM check_orapg.schemas_objects_postgres('''check_orapg'',''public''')
AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_sequences int,
s_indexes int, s_functions int, s_procedures int, s_constraints int, s_tables_comments int,s_columns_comments int); 

-- En EDB Postgres
SELECT * FROM check_orapg.schemas_objects_postgres('''check_orapg'',''public''')
AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int,
s_indexes int, s_packages int,s_packages_body int, s_functions int, s_procedures int, s_constraints int, s_tables_comments int,
s_columns_comments int); */
CREATE FUNCTION schemas_objects_postgres(p_schema_list text) RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
BEGIN
	v_query := '
	with
	total_tables as
	(
		select table_schema as s_schema, count(*) as s_tables
		from information_schema.tables
		where table_type=''BASE TABLE'' and table_schema in ('||$1||') and table_name not like ''dr$%''
		group by table_schema),
	total_views as
	(
		select schemaname as s_schema, count(*) as s_views
		from pg_views
		where schemaname in ('||$1||')
		group by schemaname),
	total_mviews as
	(
		select schemaname as s_schema, count(*) as s_mviews 
		from pg_matviews
		where schemaname in ('||$1||')
		group by schemaname),
	total_triggers as
	(
		select lower(nspname) as s_schema, count(*) as s_triggers
		from pg_trigger t join pg_class c on (t.tgrelid=c.oid) join pg_namespace n on (c.relnamespace=n.oid)
		where lower(nspname) in ('||$1||') and tgconstrrelid = 0 and tgconstrindid = 0 and tgconstraint = 0
		group by nspname),
	total_sequences as
	(
		select schemaname as s_schema, count(*) as s_sequences
		from pg_sequences
		where schemaname in ('||$1||')
		group by s_schema),
	total_indexes as
	(
		select schemaname as s_schema, count(*) as s_indexes
		from pg_indexes
		where schemaname in ('||$1||') and tablename not like ''dr$%''
		group by schemaname),
	total_proc as
	(
		select nspname as s_schema, count(*) filter (where prokind = ''f'' and pc.oid not in (select tgfoid from pg_trigger) and pc.proname not in (''oracle_fdw_handler'',''oracle_fdw_validator'',''oracle_close_connections'',''oracle_diag'')) as s_functions, count(*) filter (where prokind = ''p'') as s_procedures
		from pg_proc pc join pg_namespace pn on (pc.pronamespace=pn.oid)
		where pn.nspname in ('||$1||')
		group by nspname),
	total_cons as
	(
		select pn.nspname as s_schema, count(*) as s_constraints
		from pg_constraint pc join pg_namespace pn on (pc.connamespace=pn.oid) join pg_class ss on (pc.conrelid=ss.oid)
		where pn.nspname in ('||$1||') and ss.relname not ilike ''dr$%'' --and conname not like ''sys_c%''
		group by pn.nspname),
	total_table_comm as
	(
		select pn.nspname as s_schema, count(*) as s_total
		from pg_description pd join pg_class pc on (pd.objoid=pc.oid) join pg_namespace pn on (pc.relnamespace=pn.oid)
		where pd.objsubid=0 and pn.nspname in ('||$1||') and pc.relkind in (''v'',''r'',''m'')
		group by pn.nspname),
	total_col_comm as
	(
		select pn.nspname as s_schema, count(*) as s_total
		from pg_description pd join pg_class pc on (pd.objoid=pc.oid) join pg_namespace pn on (pc.relnamespace=pn.oid)
		where pd.objsubid<>0 and pn.nspname in ('||$1||')
		group by pn.nspname)';
	IF position('EnterpriseDB' in version()) > 0 THEN
		v_query := v_query || ',
		total_synonyms as
		(
			select lower(schema_name) as s_schema, count(*) as s_synonyms
			from sys.all_synonyms
			where lower(schema_name) in ('||$1||')
			group by schema_name),
		total_packages as
		(
			select nspname as s_schema, count(*) as s_total_packages, count(*) filter(where pkgbodysrc is not null) as s_total_bodies
			from edb_package join pg_namespace on (pkgnamespace=pg_namespace.oid)
			where /*pkgowner<>10 and*/ nspname in ('||$1||')
			group by nspname order by 1)
		select distinct nsp.nspname::text as "Schemas",
			coalesce(tt.s_tables,0)::integer as "Tables",
			coalesce(tv.s_views,0)::integer as "Ordinaries views",
			coalesce(tvm.s_mviews,0)::integer as "Materialized views",
			coalesce(td.s_triggers,0)::integer as "Triggers",
			coalesce(ts.s_synonyms,0)::integer as "Synonyms",
			coalesce(tc.s_sequences,0)::integer as "Sequences",
			coalesce(ti.s_indexes,0)::integer as "Indexes",
			coalesce(tp.s_total_packages,0)::integer as "Packages",
			coalesce(tp.s_total_bodies,0)::integer as "Packages body",
			coalesce(tpc.s_functions,0)::integer as "Functions",
			coalesce(tpc.s_procedures,0)::integer as "Procedures",
			coalesce(tr.s_constraints,0)::integer as "Constraints",
			coalesce(tct.s_total,0)::integer as "Table comments",
			coalesce(tcc.s_total,0)::integer as "Column comments"
		from pg_namespace nsp
			left join total_tables tt on (nsp.nspname=tt.s_schema)
			left join total_views tv on (nsp.nspname=tv.s_schema)
			left join total_mviews tvm on (nsp.nspname=tvm.s_schema)
			left join total_triggers td on (nsp.nspname=td.s_schema)
			left join total_synonyms ts on (nsp.nspname=ts.s_schema)
			left join total_sequences tc on (nsp.nspname=tc.s_schema)
			left join total_indexes ti on (nsp.nspname=ti.s_schema)
			left join total_packages tp on (nsp.nspname=tp.s_schema)
			left join total_proc tpc on (nsp.nspname=tpc.s_schema)
			left join total_cons tr on (nsp.nspname=tr.s_schema)
			left join total_table_comm tct on (nsp.nspname=tct.s_schema)
			left join total_col_comm tcc on (nsp.nspname=tcc.s_schema)
		where s_tables<>0 or s_views<>0 or s_mviews<>0 or s_triggers<>0 or s_synonyms<>0 or s_sequences<>0 or s_indexes<>0 or s_total_packages<>0
			  or s_total_bodies<>0 or s_functions<>0 or s_procedures<>0
		order by "Schemas" /*s_schemas*/';
	ELSE
		v_query := v_query || '
		select  distinct nsp.nspname::text as "Schemas",
			coalesce(tt.s_tables,0)::integer as "Tables",
			coalesce(tv.s_views,0)::integer as "Ordinaries views",
			coalesce(tvm.s_mviews,0)::integer as "Materialized views",
			coalesce(td.s_triggers,0)::integer as "Triggers",
			coalesce(tc.s_sequences,0)::integer as "Sequences",
			coalesce(ti.s_indexes,0)::integer as "Indexes",
			coalesce(tpc.s_functions,0)::integer as "Functions",
			coalesce(tpc.s_procedures,0)::integer as "Procedures",
			coalesce(tr.s_constraints,0)::integer as "Constraints",
			coalesce(tct.s_total,0)::integer as "Table comments",
			coalesce(tcc.s_total,0)::integer as "Column comments"
		from pg_namespace nsp
			left join total_tables tt on (nsp.nspname=tt.s_schema)
			left join total_views tv on (nsp.nspname=tv.s_schema)
			left join total_mviews tvm on (nsp.nspname=tvm.s_schema)
			left join total_triggers td on (nsp.nspname=td.s_schema)
			left join total_sequences tc on (nsp.nspname=tc.s_schema)
			left join total_indexes ti on (nsp.nspname=ti.s_schema)
			left join total_proc tpc on (nsp.nspname=tpc.s_schema)
			left join total_cons tr on (nsp.nspname=tr.s_schema)
			left join total_table_comm tct on (nsp.nspname=tct.s_schema)
			left join total_col_comm tcc on (nsp.nspname=tcc.s_schema)
		where s_tables<>0 or s_views<>0 or s_mviews<>0 or s_triggers<>0 or s_sequences<>0 or s_indexes<>0 or s_functions<>0 or s_procedures<>0
		order by "Schemas" /*s_schemas*/';
	END IF;
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


/*
SELECT * FROM check_orapg.schemas_objects_oracle('''YUDITA''') AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int,s_indexes int, s_packages int,s_packages_body int, s_functions int, s_procedures int, s_constraints int);
*/
CREATE FUNCTION schemas_objects_oracle(p_schema_list text) RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	p_schema_list := upper(p_schema_list);
	v_query := '
	WITH
	total_tables AS
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_tables
		FROM mig_objects
		WHERE OWNER IN ('||$1||')
		AND object_type = ''TABLE''
		AND object_name NOT IN (SELECT object_name FROM mig_OBJECTS WHERE object_type LIKE ''MATERIALIZED VIEW'')
		AND object_name NOT LIKE ''QUEST_SL_TEMP_EXPLAIN%'' AND object_name NOT LIKE ''SYS_NT0s2%'' and object_name not ilike ''dr$%''
		AND temporary <> ''Y''
		GROUP BY OWNER),
	total_views as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_views
		FROM mig_OBJECTS
		WHERE OWNER IN ('||$1||')
		AND object_type LIKE ''VIEW''
		GROUP BY OWNER),
	total_mviews as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_mviews
		FROM mig_OBJECTS
		WHERE OWNER IN ('||$1||')
		AND object_type LIKE ''MATERIALIZED VIEW''
		GROUP BY OWNER),
	total_triggers as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_triggers
		FROM mig_OBJECTS
		WHERE OWNER IN ('||$1||')
		AND object_type LIKE ''TRIGGER''
		GROUP BY OWNER
		ORDER BY 1),
	total_synonyms as
	(
		SELECT TABLE_OWNER AS s_schema, COUNT(*) AS s_synonyms
		FROM mig_SYNONYMS
		WHERE TABLE_OWNER IN ('||$1||')
		GROUP BY TABLE_OWNER),
	total_sequences as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_sequences
		FROM mig_OBJECTS
		WHERE OWNER IN ('||$1||')
		AND object_type LIKE ''SEQUENCE''
		GROUP BY OWNER
		ORDER BY 1),
	total_indexes as
	(
		SELECT o.OWNER AS s_schema, COUNT(*) AS s_indexes
		FROM mig_objects o join mig_indexes i on (o.object_name=i.index_name and o.owner=i.owner)
		WHERE o.OWNER IN ('||$1||') and object_type = ''INDEX''
		AND table_name NOT LIKE ''QUEST_SL_TEMP_EXPLAIN%'' AND table_name NOT LIKE ''SYS_NT0s2%'' and table_name not ilike ''dr$%''
		and index_type <> ''LOB''
		GROUP BY o.OWNER),
	total_packages as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_total_packages
		FROM mig_OBJECTS
		WHERE OWNER IN ('||$1||')
		AND object_type LIKE ''PACKAGE''
		GROUP BY OWNER),
	total_bodies as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_total_bodies
		FROM mig_OBJECTS
		WHERE OWNER IN ('||$1||')
		AND object_type LIKE ''PACKAGE BODY''
		GROUP BY OWNER
		ORDER BY 1),
	total_func as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_functions
		FROM mig_OBJECTS
		WHERE OWNER IN ('||$1||')
		AND object_type LIKE ''FUNCTION''
		GROUP BY OWNER),
	total_proc as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_procedures
		FROM mig_OBJECTS
		WHERE OWNER IN ('||$1||')
		AND object_type LIKE ''PROCEDURE''
		GROUP BY OWNER),
	total_cons as
	(
		SELECT owner AS s_schema, COUNT(*) filter (WHERE constraint_type in (''P'', ''U'', ''R''))+COUNT(*) filter (WHERE constraint_type in (''C'') and search_condition not ilike ''%not null%'') AS s_constraints
		from (select distinct c.owner, c.constraint_name, c.constraint_type, c.table_name, c.search_condition
		from mig_constraints c join mig_cons_columns cc on (c.owner=cc.owner and c.constraint_name=cc.constraint_name)
		where c.owner in ('||$1||') and c.table_name not like ''SYS_NT0s2D%'' and c.table_name not ilike ''dr$%'' and c.table_name not ilike ''bin$%'' and c.status = ''ENABLED''
		) y group by owner)
	SELECT  username::text AS s_schemas,
			coalesce(tt.s_tables,0)::int AS "Tables",
			coalesce(tv.s_views,0)::int AS "Ordinaries views",
			coalesce(tvm.s_mviews,0)::int AS "Materialized views",
			coalesce(td.s_triggers,0)::int AS "Triggers",
			coalesce(ts.s_synonyms,0)::int AS "Synonyms",
			coalesce(tc.s_sequences,0)::int AS "Sequences",
			coalesce(ti.s_indexes,0)::int AS "Indexes",
			coalesce(tp.s_total_packages,0)::int AS "Packages",
			coalesce(tpc.s_total_bodies,0)::int AS "Packages body",
			coalesce(tf.s_functions,0)::int AS "Functions",
			coalesce(tpd.s_procedures,0)::int AS "Procedures",
			coalesce(tr.s_constraints,0)::int AS "Constraints"
	FROM    mig_USERS au
			LEFT JOIN total_tables tt ON (au.username=tt.s_schema)
			LEFT JOIN total_views tv ON (au.username=tv.s_schema)
			LEFT JOIN total_mviews tvm ON (au.username=tvm.s_schema)
			LEFT JOIN total_triggers td ON (au.username=td.s_schema)
			LEFT JOIN total_synonyms ts ON (au.username=ts.s_schema)
			LEFT JOIN total_sequences tc ON (au.username=tc.s_schema)
			LEFT JOIN total_indexes ti ON (au.username=ti.s_schema)
			LEFT JOIN total_packages tp ON (au.username=tp.s_schema)
			LEFT JOIN total_bodies tpc ON (au.username=tpc.s_schema)
			LEFT JOIN total_func tf ON (au.username=tf.s_schema)
			LEFT JOIN total_proc tpd ON (au.username=tpd.s_schema)
			LEFT JOIN total_cons tr ON (au.username=tr.s_schema)
	WHERE s_tables<>0 OR s_views<>0 OR s_mviews<>0 OR s_triggers<>0 OR s_synonyms<>0 OR s_sequences<>0 OR s_indexes<>0
	ORDER BY username';
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Objects for all schemas
/* 
-- En PostgreSQL
SELECT * FROM check_orapg.schemas_objects_postgres()
AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_sequences int,
s_indexes int, s_functions int, s_procedures int, s_constraints int, s_tables_comments int,
s_columns_comments int); 

-- En EDB Postgres
SELECT * FROM check_orapg.schemas_objects_postgres()
AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int,
s_indexes int, s_packages int,s_packages_body int, s_functions int, s_procedures int, s_constraints int, s_tables_comments int,
s_columns_comments int); */
CREATE FUNCTION schemas_objects_postgres() RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
BEGIN
	v_query := '
	with
	total_tables as
	(
		select table_schema as s_schema, count(*) as s_tables
		from information_schema.tables
		where table_type=''BASE TABLE'' and table_schema not in (''pg_catalog'',''information_schema'',''pgagent'') and table_name not like ''dr$%''
		group by table_schema),
	total_views as
	(
		select schemaname as s_schema, count(*) as s_views
		from pg_views
		where schemaname not in (''pg_catalog'',''information_schema'',''pgagent'')
		group by schemaname),
	total_mviews as
	(
		select schemaname as s_schema, count(*) as s_mviews 
		from pg_matviews
		where schemaname not in (''pg_catalog'',''information_schema'',''pgagent'')
		group by schemaname),
	total_triggers as
	(
		select lower(nspname) as s_schema, count(*) as s_triggers
		from pg_trigger t join pg_class c on (t.tgrelid=c.oid) join pg_namespace n on (c.relnamespace=n.oid)
		where lower(nspname) not in (''pg_catalog'',''information_schema'',''pgagent'') and tgconstrrelid = 0 and tgconstrindid = 0 and tgconstraint = 0
		group by nspname),
	total_sequences as
	(
		select schemaname as s_schema, count(*) as s_sequences
		from pg_sequences
		where schemaname not in (''pg_catalog'',''information_schema'',''pgagent'')
		group by s_schema),
	total_indexes as
	(
		select schemaname as s_schema, count(*) as s_indexes
		from pg_indexes
		where schemaname not in (''pg_catalog'',''information_schema'',''pgagent'') and tablename not like ''dr$%''
		group by schemaname),
	total_proc as
	(
		select nspname as s_schema, count(*) filter (where prokind = ''f'' and pc.oid not in (select tgfoid from pg_trigger) and pc.proname not in (''oracle_fdw_handler'',''oracle_fdw_validator'',''oracle_close_connections'',''oracle_diag'')) as s_functions, count(*) filter (where prokind = ''p'') as s_procedures
		from pg_proc pc join pg_namespace pn on (pc.pronamespace=pn.oid)
		where pn.nspname not in (''pg_catalog'',''information_schema'',''pgagent'')
		group by nspname),
	total_cons as
	(
		select pn.nspname as s_schema, count(*) as s_constraints
		from pg_constraint pc join pg_namespace pn on (pc.connamespace=pn.oid) join pg_class ss on (pc.conrelid=ss.oid)
		where pn.nspname not in (''pg_catalog'',''information_schema'',''pgagent'') and ss.relname not ilike ''dr$%'' --and conname not like ''sys_c%''
		group by pn.nspname),
	total_table_comm as
	(
		select pn.nspname as s_schema, count(*) as s_total
		from pg_description pd join pg_class pc on (pd.objoid=pc.oid) join pg_namespace pn on (pc.relnamespace=pn.oid)
		where pd.objsubid=0 and pn.nspname not in (''pg_catalog'',''information_schema'',''pgagent'') and pc.relkind in (''v'',''r'',''m'')
		group by pn.nspname),
	total_col_comm as
	(
		select pn.nspname as s_schema, count(*) as s_total
		from pg_description pd join pg_class pc on (pd.objoid=pc.oid) join pg_namespace pn on (pc.relnamespace=pn.oid)
		where pd.objsubid<>0 and pn.nspname not in (''pg_catalog'',''information_schema'',''pgagent'')
		group by pn.nspname)';
	IF position('EnterpriseDB' in version()) > 0 THEN
		v_query := v_query || ',
		total_synonyms as
		(
			select lower(schema_name) as s_schema, count(*) as s_synonyms
			from sys.all_synonyms
			where lower(schema_name) not in (''pg_catalog'',''information_schema'',''pgagent'')
			group by schema_name),
		total_packages as
		(
			select nspname as s_schema, count(*) as s_total_packages, count(*) filter(where pkgbodysrc is not null) as s_total_bodies
			from edb_package join pg_namespace on (pkgnamespace=pg_namespace.oid)
			where /*pkgowner<>10 and*/ nspname not in (''pg_catalog'',''information_schema'',''pgagent'',''sys'')
			group by nspname order by 1)
		select  distinct nsp.nspname::text as "Schemas",
			coalesce(tt.s_tables,0)::integer as "Tables",
			coalesce(tv.s_views,0)::integer as "Ordinaries views",
			coalesce(tvm.s_mviews,0)::integer as "Materialized views",
			coalesce(td.s_triggers,0)::integer as "Triggers",
			coalesce(ts.s_synonyms,0)::integer as "Synonyms",
			coalesce(tc.s_sequences,0)::integer as "Sequences",
			coalesce(ti.s_indexes,0)::integer as "Indexes",
			coalesce(tp.s_total_packages,0)::integer as "Packages",
			coalesce(tp.s_total_bodies,0)::integer as "Packages body",
			coalesce(tpc.s_functions,0)::integer as "Functions",
			coalesce(tpc.s_procedures,0)::integer as "Procedures",
			coalesce(tr.s_constraints,0)::integer as "Constraints",
			coalesce(tct.s_total,0)::integer as "Table comments",
			coalesce(tcc.s_total,0)::integer as "Column comments"
		from pg_namespace nsp
			left join total_tables tt on (nsp.nspname=tt.s_schema)
			left join total_views tv on (nsp.nspname=tv.s_schema)
			left join total_mviews tvm on (nsp.nspname=tvm.s_schema)
			left join total_triggers td on (nsp.nspname=td.s_schema)
			left join total_synonyms ts on (nsp.nspname=ts.s_schema)
			left join total_sequences tc on (nsp.nspname=tc.s_schema)
			left join total_indexes ti on (nsp.nspname=ti.s_schema)
			left join total_packages tp on (nsp.nspname=tp.s_schema)
			left join total_proc tpc on (nsp.nspname=tpc.s_schema)
			left join total_cons tr on (nsp.nspname=tr.s_schema)
			left join total_table_comm tct on (nsp.nspname=tct.s_schema)
			left join total_col_comm tcc on (nsp.nspname=tcc.s_schema)
		where s_tables<>0 or s_views<>0 or s_mviews<>0 or s_triggers<>0 or s_synonyms<>0 or s_sequences<>0 or s_indexes<>0 or s_total_packages<>0
			  or s_total_bodies<>0 or s_functions<>0 or s_procedures<>0
		order by s_schemas';
	ELSE
		v_query := v_query || '
		select  distinct nsp.nspname::text as "Schemas",
			coalesce(tt.s_tables,0)::integer as "Tables",
			coalesce(tv.s_views,0)::integer as "Ordinaries views",
			coalesce(tvm.s_mviews,0)::integer as "Materialized views",
			coalesce(td.s_triggers,0)::integer as "Triggers",
			coalesce(tc.s_sequences,0)::integer as "Sequences",
			coalesce(ti.s_indexes,0)::integer as "Indexes",
			coalesce(tpc.s_functions,0)::integer as "Functions",
			coalesce(tpc.s_procedures,0)::integer as "Procedures",
			coalesce(tr.s_constraints,0)::integer as "Constraints",
			coalesce(tct.s_total,0)::integer as "Table comments",
			coalesce(tcc.s_total,0)::integer as "Column comments"
		from pg_namespace nsp
			left join total_tables tt on (nsp.nspname=tt.s_schema)
			left join total_views tv on (nsp.nspname=tv.s_schema)
			left join total_mviews tvm on (nsp.nspname=tvm.s_schema)
			left join total_triggers td on (nsp.nspname=td.s_schema)
			left join total_sequences tc on (nsp.nspname=tc.s_schema)
			left join total_indexes ti on (nsp.nspname=ti.s_schema)
			left join total_proc tpc on (nsp.nspname=tpc.s_schema)
			left join total_cons tr on (nsp.nspname=tr.s_schema)
			left join total_table_comm tct on (nsp.nspname=tct.s_schema)
			left join total_col_comm tcc on (nsp.nspname=tcc.s_schema)
		where s_tables<>0 or s_views<>0 or s_mviews<>0 or s_triggers<>0 or s_sequences<>0 or s_indexes<>0 or s_functions<>0 or s_procedures<>0
		order by s_schemas';
	END IF;
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


/*
SELECT * FROM check_orapg.schemas_objects_oracle() AS (s_schemas text, s_tables int, s_ordinaries_views int, s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int, s_indexes int, s_packages int,s_packages_body int, s_functions int, s_procedures int, s_constraints int);
*/
CREATE FUNCTION schemas_objects_oracle() RETURNS SETOF record AS
$$
DECLARE
	v_query text;
	v_res record;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	v_query := '
	WITH
	total_tables AS
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_tables
		FROM mig_objects
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type = ''TABLE''
		AND object_name NOT IN (SELECT object_name FROM mig_OBJECTS WHERE object_type LIKE ''MATERIALIZED VIEW'')
		AND object_name NOT LIKE ''QUEST_SL_TEMP_EXPLAIN%'' AND object_name NOT LIKE ''SYS_NT0s2%'' and object_name not ilike ''dr$%''
		AND temporary <> ''Y''
		GROUP BY OWNER),
	total_views as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_views
		FROM mig_OBJECTS
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type LIKE ''VIEW''
		GROUP BY OWNER),
	total_mviews as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_mviews
		FROM mig_OBJECTS
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type LIKE ''MATERIALIZED VIEW''
		GROUP BY OWNER),
	total_triggers as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS triggers
		FROM mig_OBJECTS
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type LIKE ''TRIGGER''
		GROUP BY OWNER
		ORDER BY 1),
	total_synonyms as
	(
		SELECT TABLE_OWNER AS s_schema, COUNT(*) AS s_synonyms
		FROM mig_SYNONYMS
		WHERE TABLE_OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		GROUP BY TABLE_OWNER),
	total_sequences as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_sequences
		FROM mig_OBJECTS
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type LIKE ''SEQUENCE''
		GROUP BY OWNER
		ORDER BY 1),
	total_indexes as
	(
		SELECT o.OWNER AS s_schema, COUNT(*) AS s_indexes
		FROM mig_objects o join mig_indexes i on (o.object_name=i.index_name and o.owner=i.owner)
		WHERE o.OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'') and object_type = ''INDEX''
		AND table_name NOT LIKE ''QUEST_SL_TEMP_EXPLAIN%'' AND table_name NOT LIKE ''SYS_NT0s2%'' and table_name not ilike ''dr$%''
		and index_type <> ''LOB''
		GROUP BY o.OWNER),
	total_packages as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_total_packages
		FROM mig_OBJECTS
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type LIKE ''PACKAGE''
		GROUP BY OWNER),
	total_bodies as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_total_bodies
		FROM mig_OBJECTS
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type LIKE ''PACKAGE BODY''
		GROUP BY OWNER
		ORDER BY 1),
	total_func as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS s_functions
		FROM mig_OBJECTS
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type LIKE ''FUNCTION''
		GROUP BY OWNER),
	total_proc as
	(
		SELECT OWNER AS s_schema, COUNT(*) AS procedures
		FROM mig_OBJECTS
		WHERE OWNER NOT IN (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'')
		AND object_type LIKE ''PROCEDURE''
		GROUP BY OWNER),
	total_cons as
	(
		SELECT owner AS s_schema, COUNT(*) filter (WHERE constraint_type in (''P'', ''U'', ''R''))+COUNT(*) filter (WHERE constraint_type in (''C'') and search_condition not ilike ''%not null%'') AS s_constraints
		from (select distinct c.owner, c.constraint_name, c.constraint_type, c.table_name
		from mig_constraints c join mig_cons_columns cc on (c.owner=cc.owner and c.constraint_name=cc.constraint_name)
		where c.owner NOT in (''SYS'',''SYSTEM'',''XDB'',''APPQOSSYS'',''DBSNMP'',''FLOWS_FILES'',''OLAPSYS'',''ORDDATA'',''OUTLN'',''OWBSYS'',''WMSYS'') and c.table_name not like ''SYS_NT0s2D%'' and c.table_name not ilike ''dr$%'' and c.table_name not ilike ''bin$%'' and c.status = ''ENABLED''
		) y group by owner)
	SELECT  username::text AS "Schemas",
		coalesce(tt.s_tables,0)::int AS "Tables",
		coalesce(tv.s_views,0)::int AS "Ordinaries views",
		coalesce(tvm.s_mviews,0)::int AS "Materialized views",
		coalesce(td.s_triggers,0)::int AS "Triggers",
		coalesce(ts.s_synonyms,0)::int AS "Synonyms",
		coalesce(tc.s_sequences,0)::int AS "Sequences",
		coalesce(ti.s_indexes,0)::int AS "Indexes",
		coalesce(tp.s_total_packages,0)::int AS "Packages",
		coalesce(tpc.s_total_bodies,0)::int AS "Packages body",
		coalesce(tf.s_functions,0)::int AS "Functions",
		coalesce(tpd.s_procedures,0)::int AS "Procedures",
		coalesce(tr.s_constraints,0)::int AS "Constraints"
	FROM mig_USERS au
		LEFT JOIN total_tables tt ON (au.username=tt.s_schema)
		LEFT JOIN total_views tv ON (au.username=tv.s_schema)
		LEFT JOIN total_mviews tvm ON (au.username=tvm.s_schema)
		LEFT JOIN total_triggers td ON (au.username=td.s_schema)
		LEFT JOIN total_synonyms ts ON (au.username=ts.s_schema)
		LEFT JOIN total_sequences tc ON (au.username=tc.s_schema)
		LEFT JOIN total_indexes ti ON (au.username=ti.s_schema)
		LEFT JOIN total_packages tp ON (au.username=tp.s_schema)
		LEFT JOIN total_bodies tpc ON (au.username=tpc.s_schema)
		LEFT JOIN total_func tf ON (au.username=tf.s_schema)
		LEFT JOIN total_proc tpd ON (au.username=tpd.s_schema)
		LEFT JOIN total_cons tr ON (au.username=tr.s_schema)
	WHERE s_tables<>0 OR s_views<>0 OR mviewss_<>0 OR s_triggers<>0 OR s_synonyms<>0 OR s_sequences<>0 OR s_indexes<>0
	ORDER BY username';
	FOR v_res IN EXECUTE v_query
	LOOP
		RETURN NEXT v_res;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Rows of a schema
-- SELECT * FROM check_orapg.tables_rows_by_schema_postgres('check_orapg') AS (s_table text, s_total bigint);
CREATE FUNCTION tables_rows_by_schema_postgres(p_schema text) RETURNS SETOF record AS
$$
DECLARE
	v_tables record;
	v_total bigint;
BEGIN
	FOR v_tables IN SELECT table_schema AS v_schema, table_name AS v_table,'"'|| table_schema||'"."'||table_name||'"' AS v_full FROM information_schema.tables WHERE table_schema NOT LIKE 'pg_%' AND table_schema<>'information_schema' AND lower(table_type)='base table' AND table_schema= $1 AND table_name <> 'info' AND table_name NOT ILIKE 'dr$%' ORDER BY table_name LOOP
		EXECUTE 'select count(*) from ' || v_tables.v_full INTO v_total;
		RETURN NEXT (v_tables.v_full::text, v_total::bigint);
	END LOOP;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.tables_rows_by_schema_oracle('YUDITA') AS (s_table text, s_total bigint)
CREATE FUNCTION tables_rows_by_schema_oracle(p_schema text) RETURNS setof record AS
$$
DECLARE
	v_list record;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	FOR v_list IN SELECT owner AS s_schema, table_name AS s_table, owner||'.'||table_name AS s_full, total FROM mig_rows_tables_oracle
	WHERE owner ILIKE $1 AND table_name NOT ILIKE 'dr$%' AND table_name NOT IN (SELECT object_name FROM mig_objects WHERE object_type = 'MATERIALIZED VIEW') AND table_name NOT IN (SELECT table_name FROM mig_tables WHERE temporary = 'Y')
	LOOP
		RETURN NEXT (lower(v_list.s_full)::text, v_list.total::bigint);
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Rows by schema
-- SELECT * FROM check_orapg.schema_rows_postgres('''check_orapg'',''public''') AS (s_schema text, s_total bigint);
CREATE FUNCTION schema_rows_postgres(p_schema_list text) RETURNS SETOF record AS
$$
DECLARE
	v_i int;
	v_j text[];
	v_function_schema text;
	v_total_schemas int;
	v_total bigint;
BEGIN
	SELECT n.nspname INTO v_function_schema FROM pg_proc c JOIN pg_namespace n ON (c.pronamespace=n.oid) WHERE c.proname ilike 'tables_rows_by_schema_postgres';
	EXECUTE 'select array_length(array['||$1||'], 1)' INTO v_total_schemas;
	FOR v_i IN 1..v_total_schemas LOOP
		FOR v_j IN EXECUTE 'select array['||$1||']'
		LOOP
			EXECUTE 'select sum(s_total) from ' || v_function_schema ||'.' || 'tables_rows_by_schema_postgres('''||v_j[v_i]||''') as (s_table text, s_total bigint)' into v_total;
			RETURN NEXT (v_j[v_i], v_total);
		END LOOP;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.schema_rows_oracle('''public'',''yudita''') AS (s_schema text, s_total bigint)
CREATE FUNCTION schema_rows_oracle(p_schema_list text) RETURNS SETOF record AS $$
DECLARE
	v_list record;
	v_search_path text;
begin
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	FOR v_list IN EXECUTE 'select owner as s_schema, coalesce(sum(total),0) as s_total
	from mig_rows_tables_oracle
	where lower(owner) in ('||$1||') and table_name not ilike ''dr$%'' and table_name not in (select object_name from mig_objects where object_type = ''MATERIALIZED VIEW'')
		and table_name not in (select table_name from mig_tables where temporary = ''Y'')
	group by owner' 
	LOOP
		RETURN NEXT (lower(v_list.s_schema), v_list.s_total::bigint);
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Register Postgres validation
-- SELECT * FROM check_orapg.register_postgres_validation('''check_orapg'',''public''');
-- SELECT * FROM check_orapg.postgres_validation ORDER BY 1, 2;
CREATE FUNCTION register_postgres_validation(p_schema_list text) RETURNS void AS
$$
DECLARE
	v_list_schemas text;
	v_query record;
	v_function_call text;
	v_total bigint;
	v_total_schemas int;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	-- Global objects to the cluster
	FOR v_query IN SELECT * FROM cluster_postgres() AS (s_object text, s_total int)
	LOOP
		INSERT INTO postgres_validation VALUES ('-', v_query.s_object, 'Global', v_query.s_total, current_date);
	END LOOP;
	
	-- Objects by schema
	v_list_schemas := replace($1,'''', '''''')::text;
	IF position('EnterpriseDB' IN version()) = 0 THEN
		v_function_call := 'SELECT * FROM schemas_objects_postgres('''||v_list_schemas||''') AS (s_schemas text, s_tables int, "s_ordinaries_views" int,
			"s_materialized_views" int, "s_triggers" int, s_sequences int,s_indexes int, s_functions int, s_procedures int, s_constraints int, 
			"s_tables_comments" int,"s_columns_comments" int) ORDER BY 1';
	ELSE
		v_function_call := 'SELECT * FROM schemas_objects_postgres('''||v_list_schemas||''') AS (s_schemas text, s_tables int, "s_ordinaries_views" int,
			"s_materialized_views" int, "s_triggers" int, "s_synonyms" int, s_sequences int, s_indexes int, "s_packages" int,"s_packages_body" int,
			s_functions int, s_procedures int, s_constraints int, "s_tables_comments" int, "s_columns_comments" int)';
	END IF;
	FOR v_query IN EXECUTE v_function_call
	LOOP
		IF position('EnterpriseDB' in version()) = 0 THEN
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Table', '-', v_query.s_tables, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Ordinary view', '-', v_query.s_ordinaries_views, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Materialized view', '-', v_query.s_materialized_views, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Trigger', '-', v_query.s_triggers, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Synonym', '-', 0, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Sequence', '-', v_query.s_sequences, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Index', '-', v_query.s_indexes, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Package', '-', 0, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Package body', '-', 0, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Function', '-', v_query.s_functions, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Procedure', '-', v_query.s_procedures, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Constraint', '-', v_query.s_constraints, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Table comment', '-', v_query.s_tables_comments, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Column comment', '-', v_query.s_columns_comments, current_date);
		ELSE
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Table', '-', v_query.s_tables, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Ordinary view', '-', v_query.s_ordinaries_views, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Materialized view', '-', v_query.s_materialized_views, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Trigger', '-', v_query.s_triggers, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Synonym', '-', v_query.s_synonyms, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Sequence', '-', v_query.s_sequences, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Index', '-', v_query.s_indexes, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Package', '-', v_query.s_packages, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Package body', '-', v_query.s_packages_body, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Function', '-', v_query.s_functions, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Procedure', '-', v_query.s_procedures, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Constraint', '-', v_query.s_constraints, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Table comment', '-', v_query.s_tables_comments, current_date);
			INSERT INTO postgres_validation VALUES (v_query.s_schemas, 'Column comment', '-', v_query.s_columns_comments, current_date);
		END IF;
	END LOOP;
	
	-- Rows by schema
	FOR v_query IN EXECUTE 'SELECT * FROM schema_rows_postgres('''||v_list_schemas||''') AS (s_schema text, s_total bigint) ORDER BY 1'
	LOOP
		INSERT INTO postgres_validation VALUES (v_query.s_schema, '-', 'Rows by schema', v_query.s_total, current_date);
	END LOOP;
	
	-- Rows by table
	v_list_schemas := replace($1,'''','''')::text;
	FOR v_query IN EXECUTE 'select table_schema as s_schema, table_name as s_table from information_schema.tables where lower(table_type)=''base table'' and table_schema in ('||v_list_schemas||') and table_name not like ''dr$%'''
	LOOP
		EXECUTE 'select count(*) from "'||v_query.s_schema||'"."'||v_query.s_table||'"' INTO v_total;
		INSERT INTO postgres_validation VALUES ('-', (lower(v_query.s_schema)||'.'||lower(v_query.s_table))::text, 'Rows by table', v_total, current_date);
	END LOOP;
	
	v_list_schemas := replace($1,'''', '''''')::text;
	
	-- Asigned privileges by users
	EXECUTE 'select sum(s_asigned_privileges) from users_privileges_postgres('''||v_list_schemas||''') as (s_user text, s_asigned_privileges bigint)' INTO v_total;
	INSERT INTO postgres_validation VALUES ('-', 'Global user privileges', 'Global', v_total, current_date);
	FOR v_query IN EXECUTE 'select * from users_privileges_postgres('''||v_list_schemas||''') as (s_user text, s_asigned_privileges bigint) order by 1'
	LOOP
		INSERT INTO postgres_validation VALUES ('-', v_query.s_user, 'User privileges', v_query.s_asigned_privileges, current_date);
	END LOOP;
	
	-- Asigned privileges by roles
	EXECUTE 'select sum(s_asigned_privileges) from roles_privileges_postgres('''||v_list_schemas||''') as (s_role text, s_asigned_privileges bigint)' INTO v_total;
	INSERT INTO postgres_validation VALUES ('-', 'Global role privileges', 'Global', v_total, current_date);
	FOR v_query IN EXECUTE 'select * from roles_privileges_postgres('''||v_list_schemas||''') as (s_role text, s_asigned_privileges bigint) order by 1'
	LOOP
		INSERT INTO postgres_validation VALUES ('-', v_query.s_role, 'Role privileges', v_query.s_asigned_privileges, current_date);
	END LOOP;
	
	-- Attributes by tables
	FOR v_query IN EXECUTE 'select pn.nspname as s_schema, pc.relname as s_table, pc.relnatts as s_total
			from pg_class pc join pg_namespace pn on (pc.relnamespace=pn.oid)
			where pc.relkind in (''r'',''v'',''m'') and pn.nspname in ('||$1||') and pc.relname not like ''dr$%'''
	LOOP
		INSERT INTO postgres_validation VALUES (v_query.s_schema, v_query.s_table, 'Attributes by table',v_query.s_total, current_date);
	END LOOP;
	
	-- Attributes by indexes
	FOR v_query IN EXECUTE 'select pn.nspname as s_schema, pc.relname as s_index, array_length(string_to_array(replace(px.indkey::text,'' '','',''),'',''),1) as s_total, (select relname from pg_class where oid=indrelid) as s_table_name
			from pg_class pc join pg_index px on (pc.oid=px.indexrelid) join pg_namespace pn on (pc.relnamespace=pn.oid)
			where pn.nspname in ('||$1||') and px.indrelid not in (select oid from pg_class where relname ilike ''dr$%'')'
	LOOP
		INSERT INTO postgres_validation VALUES (v_query.s_schema, v_query.s_index, 'Attributes by index', v_query.s_total, current_date, v_query.s_table_name);
	END LOOP;
	
	-- Attributes by primary key
	FOR v_query IN EXECUTE 'select pn.nspname as s_schema, pc.conname as s_pk, array_length(string_to_array(replace(pc.conkey::text,'' '','',''),'',''),1) as s_total, c.relname
			from pg_constraint pc join pg_namespace pn on (pc.connamespace=pn.oid) join pg_class c on (pc.conrelid=c.oid)
			where pc.contype= ''p'' and pn.nspname in ('||$1||') and c.relname not ilike ''dr$%'''
	LOOP
		INSERT INTO postgres_validation VALUES (v_query.s_schema, v_query.s_pk, 'Attributes by primary key', v_query.s_total, current_date, v_query.relname);
	END LOOP;
	
	-- Attributes by foreign key
	FOR v_query IN EXECUTE 'select pn.nspname as s_schema, pc.conname as s_fk, array_length(string_to_array(replace(pc.confkey::text,'' '','',''),'',''),1) as s_total, c.relname
			from pg_constraint pc join pg_namespace pn on (pc.connamespace=pn.oid) join pg_class c on (pc.conrelid=c.oid)
			where pc.contype= ''f'' and pn.nspname in ('||$1||') and c.relname not ilike ''dr$%'''
	LOOP
		INSERT INTO postgres_validation VALUES (v_query.s_schema, v_query.s_fk, 'Attributes by foreign key', v_query.s_total, current_date, v_query.relname);
	END LOOP;
	
	-- Constraints by schema
	FOR v_query IN EXECUTE 'select pn.nspname as s_schema, count(*) filter (where contype=''p'') as s_pk, count(*)filter (where contype=''f'') as s_fk, count(*) filter (where contype=''c'') as s_check, count(*) filter (where contype=''u'') as s_unique
		from pg_constraint pc join pg_namespace pn on (pc.connamespace=pn.oid) join pg_class c on (pc.conrelid=c.oid)
		where pn.nspname in('||$1||') and c.relname not ilike ''dr$%''
		group by pn.nspname'
	LOOP
		INSERT INTO postgres_validation VALUES (v_query.s_schema, 'Primary key constraint', '-', v_query.s_pk, current_date);
		INSERT INTO postgres_validation VALUES (v_query.s_schema, 'Foreign key constraint', '-', v_query.s_fk, current_date);
		INSERT INTO postgres_validation VALUES (v_query.s_schema, 'Check constraint', '-', v_query.s_check, current_date);
		INSERT INTO postgres_validation VALUES (v_query.s_schema, 'Unique constraint', '-', v_query.s_unique, current_date);
	END LOOP;
	
	-- Synonyms
	IF position('EnterpriseDB' in version()) > 0 THEN
		FOR v_query IN EXECUTE 'select pn.nspname, ps.synname, ps.synobjschema||''.''||ps.synobjname as sinonimo, case when ps.synlink is null then ''-'' else ps.synlink end as s_synlink
				from pg_synonym ps join pg_namespace pn on (ps.synnamespace=pn.oid)
				where pn.nspname in ('||$1||',''public'') and lower(ps.synobjname) not like ''%db_sun450\_%'' and (ps.synobjschema in ('||$1||',''public'') or synlink is not null)'
		LOOP
			INSERT INTO postgres_validation VALUES (v_query.synname, v_query.sinonimo, v_query.s_synlink, -1, current_date);
		END LOOP;
	END IF;
	
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Register Oracle validation
-- select * from check_orapg.register_oracle_validation('''YUDITA''')
CREATE FUNCTION register_oracle_validation(p_schema_list text) RETURNS void AS
$$
DECLARE
	v_list_schemas text;
	v_query record;
	v_total bigint;
	v_i int;
	v_j text[];
	v_total_schemas int;
	v_search_path text;
BEGIN
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	v_list_schemas := upper(replace(upper($1),'''', ''''''))::text;
	
	-- Global objects to the cluster
	FOR v_query IN EXECUTE 'SELECT * FROM cluster_oracle('''|| v_list_schemas ||''') AS (s_object text, s_total int)'
	LOOP
		INSERT INTO oracle_validation VALUES ('-', v_query.s_object, 'Global', v_query.s_total, current_date);
	END LOOP;
	
	-- Objects by schema
	FOR v_query IN EXECUTE 'select * from schemas_objects_oracle('''|| v_list_schemas ||''') as (s_schemas text, s_tables int, s_ordinaries_views int,
			s_materialized_views int, s_triggers int, s_synonyms int, s_sequences int, s_indexes int, s_packages int, s_packages_body int,
			s_functions int, s_procedures int, s_constraints int) order by 1'
	LOOP
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Table', '-', v_query.s_tables, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Ordinary view', '-', v_query.s_ordinaries_views, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Materialized view', '-', v_query.s_materialized_views, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Trigger', '-', v_query.s_triggers, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Synonym', '-', v_query.s_synonyms, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Sequence', '-', v_query.s_sequences, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Index', '-', v_query.s_indexes, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Package', '-', v_query.s_packages, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Package body', '-', v_query.s_packages_body, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Function', '-', v_query.s_functions, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Procedure', '-', v_query.s_procedures, current_date);
		INSERT INTO oracle_validation VALUES (lower(v_query.s_schemas), 'Constraint', '-', v_query.s_constraints, current_date);
	END LOOP;
	
	-- Rows by schema
	FOR v_query IN EXECUTE 'select * from schema_rows_oracle('''||lower(v_list_schemas)||''') as (s_schema text, s_total bigint) order by 1'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.s_schema, '-', 'Rows by schema', v_query.s_total, current_date);
	END LOOP;
	
	-- Rows by table
	EXECUTE'select array_length(array['||upper($1)||'], 1)' INTO v_total_schemas;
	FOR v_i IN 1..v_total_schemas
	LOOP
		FOR v_j IN EXECUTE 'select array['||upper($1)||']'
		loop
			FOR v_query IN EXECUTE 'select * from tables_rows_by_schema_oracle('''||v_j[v_i]||''') as (s_table text, s_total bigint)'
			LOOP
				INSERT INTO oracle_validation VALUES ('-', lower(v_query.s_table), 'Rows by table', v_query.s_total, current_date);
			END LOOP;
		END LOOP;
	END LOOP;
	
	-- Asigned privileges by users
	EXECUTE 'select sum(s_asigned_privileges) from users_privileges_oracle('''||v_list_schemas||''') as (s_user text, s_asigned_privileges bigint)' INTO v_total;
	INSERT INTO oracle_validation VALUES ('-', 'Global user privileges', 'Global', v_total, current_date);
	FOR v_query IN EXECUTE 'select * from users_privileges_oracle('''||v_list_schemas||''') as (s_user text, s_asigned_privileges bigint) order by 1'
	LOOP
		INSERT INTO oracle_validation VALUES ('-', lower(v_query.s_user), 'User privileges', v_query.s_asigned_privileges, current_date);
	END LOOP;
	
	-- Asigned privileges by roles
	EXECUTE 'select sum(s_asigned_privileges) from roles_privileges_oracle('''||v_list_schemas||''') as (s_role text, s_asigned_privileges bigint)' INTO v_total;
	INSERT INTO oracle_validation VALUES ('-', 'Global role privileges', 'Global', v_total, current_date);
	FOR v_query IN EXECUTE 'select * from roles_privileges_oracle('''||v_list_schemas||''') as (s_role text, s_asigned_privileges bigint) order by 1'
	LOOP
		INSERT INTO oracle_validation VALUES ('-', lower(v_query.s_role), 'Role privileges', v_query.s_asigned_privileges, current_date);
	END LOOP;
	
	-- Attributes by tables
	FOR v_query IN EXECUTE 'select lower(owner) as s_schema, lower(table_name) as s_table, count(*) as s_total
			from mig_tab_columns
			where owner in ('||upper($1)||') and table_name not ilike ''dr$%'' and table_name not ilike ''bin$%''
				and table_name not in (select table_name from mig_tables where temporary = ''Y'')
			group by lower(owner), lower(table_name)'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.s_schema, v_query.s_table, 'Attributes by table', v_query.s_total, current_date);
	END LOOP;
	
	-- Attributes by indexes
	FOR v_query IN EXECUTE 'select distinct x.*, lower(table_name) as s_table_name
			from (select lower(index_owner) as s_schema, lower(index_name) as s_index, count(*) as s_total
			from mig_ind_columns
			where index_owner in ('||upper($1)||') and table_name not ilike ''dr$%'' and table_name not ilike ''bin$%''
			group by lower(index_owner), lower(index_name)) x join mig_ind_columns mic on (s_index=lower(index_name) and s_schema=lower(index_owner))'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.s_schema, v_query.s_index, 'Attributes by index', v_query.s_total, current_date, v_query.s_table_name);
	END LOOP;
	
	-- Attributes by primary key
	FOR v_query IN EXECUTE 'select lower(c.owner) as s_schema, lower(cc.constraint_name) as s_pk, count(*) as s_total, lower(c.table_name) as s_table_name
			from mig_constraints c join mig_cons_columns cc on (c.owner=cc.owner and c.constraint_name=cc.constraint_name)
			where c.owner in ('||upper($1)||') and c.constraint_type=''P'' and c.table_name not ilike ''dr$%'' and c.table_name not ilike ''bin$%'' and c.status = ''ENABLED''
			group by lower(c.owner), lower(cc.constraint_name), lower(c.table_name)'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.s_schema, v_query.s_pk, 'Attributes by primary key', v_query.s_total, current_date, v_query.s_table_name);
	END LOOP;
	
	-- Attributes by foreign key
	FOR v_query IN EXECUTE 'select lower(c.owner) as s_schema, lower(cc.constraint_name) as s_fk, count(*) as s_total, lower(c.table_name) as s_table_name
			from mig_constraints c join mig_cons_columns cc on (c.owner=cc.owner and c.constraint_name=cc.constraint_name)
			where c.owner in ('||upper($1)||') and c.constraint_type=''R'' and c.table_name not ilike ''dr$%'' and c.table_name not ilike ''bin$%'' and c.status = ''ENABLED''
			group by lower(c.owner), lower(cc.constraint_name), lower(c.table_name)'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.s_schema, v_query.s_fk, 'Attributes by foreign key', v_query.s_total, current_date, v_query.s_table_name);
	END LOOP;
	
	-- Table comments
	FOR v_query IN EXECUTE 'select lower(nspname) as s_schema, coalesce(s_total,0) as s_total from
			(select nspname
			from pg_namespace
			where upper(nspname) in ('||upper($1)||')) n
			left join
			(select lower(c.owner) as s_schema, count(*) as s_total
			from mig_tab_comments c --join mig_tables t on c.owner||''.''||c.table_name=t.owner||''.''||t.table_name
			where c.owner in ('||upper($1)||') and c.comments is not null and c.table_name not ilike ''bin$%'' and c.table_name not ilike ''dr$%'' --and t.temporary <> ''Y''
				and c.table_name not in (select table_name from mig_tables where temporary = ''Y'')
			group by lower(c.owner)) c on n.nspname=c.s_schema'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.s_schema, 'Table comment', '-', v_query.s_total, current_date);
	END LOOP;
	
	-- Column comments
	FOR v_query IN EXECUTE 'select lower(nspname) as s_schema, coalesce(s_total,0) as s_total from
			(select nspname
			from pg_namespace
			where upper(nspname) in ('||upper($1)||')) n
			left join
			(select lower(c.owner) as s_schema, count(*) as s_total
			from mig_col_comments c --join mig_tables t on c.owner||''.''||c.table_name=t.owner||''.''||t.table_name
			where c.owner in ('||upper($1)||') and c.comments is not null and c.table_name not ilike ''bin$%'' and c.table_name not ilike ''dr$%'' --and t.temporary <> ''Y''
				and c.table_name not in (select table_name from mig_tables where temporary = ''Y'')
			group by lower(c.owner)) c on n.nspname=c.s_schema'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.s_schema, 'Column comment', '-', v_query.s_total, current_date);
	END LOOP;
	
	-- Constraints by schema
	FOR v_query IN EXECUTE 'select lower(owner) as s_schema, count(*) filter (where constraint_type=''P'') as s_pk, count(*) filter (where constraint_type=''R'') as s_fk, count(*) filter (where constraint_type=''C'' and c.search_condition not ilike ''%not null%'') as s_check, count(*) filter (where constraint_type=''U'') as s_unique
			from (select distinct c.owner, c.constraint_name, c.constraint_type, c.table_name, c.search_condition
			from mig_constraints c join mig_cons_columns cc on (c.owner=cc.owner and c.constraint_name=cc.constraint_name)
			where c.owner in ('||upper($1)||') and c.table_name not like ''SYS_NT0s2D%'' and c.table_name not ilike ''dr$%'' and c.table_name not ilike ''bin$%'' and c.status = ''ENABLED'' and c.table_name not like ''SYS_NT%''
			) c group by lower(owner)'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.s_schema, 'Primary key constraint', '-', v_query.s_pk, current_date);
		INSERT INTO oracle_validation VALUES (v_query.s_schema, 'Foreign key constraint', '-', v_query.s_fk, current_date);
		INSERT INTO oracle_validation VALUES (v_query.s_schema, 'Check constraint', '-', v_query.s_check, current_date);
		INSERT INTO oracle_validation VALUES (v_query.s_schema, 'Unique constraint', '-', v_query.s_unique, current_date);
	END LOOP;
	
	-- Synonyms
	FOR v_query IN EXECUTE 'select lower(owner) as nspname, lower(synonym_name) as synname, lower(table_owner)||''.''||lower(table_name) as s_synonym, case when db_link is null then ''-'' else db_link end as s_synlink
			from mig_synonyms
			where table_owner in ('||upper($1)||',''PUBLIC'') and (db_link not ilike ''db_sun450\_%'' or db_link is null)'
	LOOP
		INSERT INTO oracle_validation VALUES (v_query.synname, v_query.s_synonym, v_query.s_synlink, -1, current_date);
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- Generate file validation
-- SELECT * FROM check_orapg.postgres_file('2019-08-07','''check_orapg'',''public'''));
CREATE FUNCTION postgres_file(p_date date, p_schema_list text) RETURNS SETOF text AS
$$
DECLARE
	v_res record;
	v_schema_list text;
	v_date text;
	v_search_path text;
	schema_size int;
	object_size int;
	total_size int;
	tab1_size int;
BEGIN
	v_schema_list := replace(p_schema_list,'''', '''''')::text;
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;

	v_date := to_char(p_date, 'yyyy-mm-dd');

	RAISE NOTICE 'Registering validation...';
	EXECUTE 'SELECT * FROM register_postgres_validation(''' || v_schema_list || '''::text)';
	RAISE NOTICE 'Validation registered in postgres_validation table';

	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT rpad('-- OBJECTS COUNTING FOR POSTGRES VALIDATION ', 133, '-');
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '-- GENERAL';
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '-- Global objects to the cluster';
	RETURN NEXT '--' || lpad('-', 131, '-');
	
	EXECUTE 'select max(length(name_ref)), max(length(total::text)) from postgres_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''Global''' into object_size, total_size;
	IF object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Object ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF object_size < length(' Object ') THEN
			object_size := length(' Object ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Object ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select name_ref, coalesce(total,0) as total from postgres_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''Global'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';
	
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '-- PRIVILEGES';
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '-- Role privileges';
	RETURN NEXT '--' || lpad('-', 131, '-');	
	EXECUTE 'select max(length(name_ref)), max(length(total::text)) from postgres_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''Role privileges''' into object_size, total_size;
	IF object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Role ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF object_size < length(' Role ') THEN
			object_size := length(' Role ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Role ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select name_ref, coalesce(total,0) as total from postgres_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''Role privileges'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';
	
	RETURN NEXT '-- User privileges';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(name_ref)), max(length(total::text)) from postgres_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''User privileges''' into object_size, total_size;
	IF object_size is null and total_size is null THEN
		RETURN NEXT rpad(' User ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF object_size < length(' User ') THEN
			object_size := length(' User ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' User ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select name_ref, coalesce(total,0) as total from postgres_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''User privileges'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';
	
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '-- SCHEMAS';
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '-- Objects by schema';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Table'',''Ordinary view'',''Materialized view'',''Trigger'',''Synonym'',''Sequence'',''Index'',''Package'',''Package body'',''Function'',''Procedure'',''Constraint'')' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Object ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Object ') THEN
			object_size := length(' Object ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Object ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, coalesce(total,0) as total from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Table'',''Ordinary view'',''Materialized view'',''Trigger'',''Synonym'',''Sequence'',''Index'',''Package'',''Package body'',''Function'',''Procedure'',''Constraint'') order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Rows by schema';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(total::text)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Rows by schema''' into schema_size, total_size;
	IF schema_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, coalesce(total,0) as total from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Rows by schema'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Rows by table';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(name_ref)), max(length(total::text)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Rows by table''' into object_size, total_size;
	IF object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Table ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF object_size < length(' Table ') THEN
			object_size := length(' Table ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Table ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select name_ref, coalesce(total,0) as total from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Rows by table'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '-- TABLES';
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '-- Table and column comments';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Table comment'',''Column comment'')' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Comment ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Comment ') THEN
			object_size := length(' Comment ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Comment ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, coalesce(total,0) as total from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Table comment'',''Column comment'') order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Attributes by table';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Attributes by table''' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Table ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Table ') THEN
			object_size := length(' Table ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Table ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, coalesce(total,0) as total from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Attributes by table'' order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Attributes by index';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(table_attr_pkfkidx)), max(length(total::text)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Attributes by index''' into schema_size, object_size, tab1_size, total_size;
	IF schema_size is null and object_size is null and tab1_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Index ', 31, ' ') || '|' || rpad(' Table ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Index ') THEN
			object_size := length(' Index ');	
		END IF;
		IF tab1_size < length(' Table ') THEN
			tab1_size := length(' Table ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Index ', object_size+2, ' ') || '|' || rpad(' Table ', tab1_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', tab1_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, table_attr_pkfkidx, coalesce(total,0) as total from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Attributes by index'' order by 1,3,2,4'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '| ' || rpad(v_res.table_attr_pkfkidx, tab1_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
		RETURN NEXT '';
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Attributes by primary and foreign key';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description in (''Attributes by primary key'',''Attributes by foreign key'')' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Key ', 31, ' ') || '|' || rpad(' Table ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Key ') THEN
			object_size := length(' Key ');	
		END IF;
		IF tab1_size < length(' Table ') THEN
			tab1_size  := length(' Table ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Key ', object_size+2, ' ') || '|' || rpad(' Table ', tab1_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', tab1_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, table_attr_pkfkidx, coalesce(total,0) as total from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description in (''Attributes by primary key'',''Attributes by foreign key'') order by 1,3,2 desc'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '| ' || rpad(v_res.table_attr_pkfkidx, tab1_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
		RETURN NEXT '';
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Constraint types';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Primary key constraint'',''Foreign key constraint'',''Check constraint'',''Unique constraint'')' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Constraint ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Constraint ') THEN
			object_size := length(' Constraint ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Constraint ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, coalesce(total,0) as total from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Primary key constraint'',''Foreign key constraint'',''Check constraint'',''Unique constraint'') order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
		RETURN NEXT '';
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Synonyms list';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(description)) from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and total=-1' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Synonym ', 31, ' ') || '|' || rpad(' Reference ', 31, ' ') || '|' || rpad(' Link ', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Synonym ') THEN
			schema_size := length(' Synonym ');	
		END IF;
		IF object_size < length(' Reference ') THEN
			object_size := length(' Reference ');	
		END IF;
		IF total_size < length(' Link ') THEN
			total_size := length(' Link ');	
		END IF;
		RETURN NEXT rpad(' Synonym ', schema_size+2, ' ') || '|' || rpad(' Reference ', object_size+2, ' ') || '|' || rpad(' Link', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, description from postgres_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and total=-1 order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
		RETURN NEXT '';
	END IF;
	RETURN NEXT '';
	
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.generate_postgres_file('2019-08-07', '''check_orapg'',''public''', '/tmp', 'postgres_validation')
CREATE FUNCTION generate_postgres_file(p_date date, p_schema_list text, p_location_file text, p_name_output_file text) RETURNS boolean AS
$$
DECLARE
	v_query text;
	v_search_path text;
	v_schema_list text;
BEGIN
	v_schema_list := replace($2, '''', '''''')::text;
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;

	EXECUTE 'copy (SELECT * FROM postgres_file(''' || p_date ||''', '''|| v_schema_list || ''')) to ''' || p_location_file || '/' || p_name_output_file || '.txt''';

	RETURN true;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.oracle_file('2019-08-07','''check_orapg'',''public'''));
CREATE FUNCTION oracle_file(p_date date, p_schema_list text) RETURNS SETOF text AS
$$
DECLARE
	v_res record;
	v_schema_list text;
	v_date text;
	v_search_path text;
	schema_size int;
	object_size int;
	tab1_size int;
	total_size int;
BEGIN
	v_schema_list := replace(p_schema_list,'''', '''''')::text;
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;
	
	v_date := to_char(p_date, 'yyyy-mm-dd');
	
	RAISE NOTICE 'Registering validation...';
	EXECUTE 'SELECT * FROM register_oracle_validation(''' || v_schema_list || ''')';
	RAISE NOTICE 'Validation registered in oracle_validation table';

	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT rpad('-- OBJECTS COUNTING FOR ORACLE VALIDATION ', 133, '-');
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '-- GENERAL';
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '-- Global objects to the cluster';
	RETURN NEXT '--' || lpad('-', 131, '-');
	
	EXECUTE 'select max(length(name_ref)), max(length(total::text)) from oracle_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''Global''' into object_size, total_size;
	IF object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Object ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF object_size < length(' Object ') THEN
			object_size := length(' Object ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Object ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select name_ref, coalesce(total,0) as total from oracle_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''Global'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';
	
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '-- PRIVILEGES';
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '-- Role privileges';
	RETURN NEXT '--' || lpad('-', 131, '-');	
	EXECUTE 'select max(length(name_ref)), max(length(total::text)) from oracle_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''Role privileges''' into object_size, total_size;
	IF object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Role ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF object_size < length(' Role ') THEN
			object_size := length(' Role ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Role ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select name_ref, coalesce(total,0) as total from oracle_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''Role privileges'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';
	
	RETURN NEXT '-- User privileges';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(name_ref)), max(length(total::text)) from oracle_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''User privileges''' into object_size, total_size;
	IF object_size is null and total_size is null THEN
		RETURN NEXT rpad(' User ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF object_size < length(' User ') THEN
			object_size := length(' User ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' User ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select name_ref, coalesce(total,0) as total from oracle_validation where schema_ref =''-'' and to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description=''User privileges'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';
	
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '-- SCHEMAS';
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '-- Objects by schema';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Table'',''Ordinary view'',''Materialized view'',''Trigger'',''Synonym'',''Sequence'',''Index'',''Package'',''Package body'',''Function'',''Procedure'',''Constraint'')' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Object ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Object ') THEN
			object_size := length(' Object ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Object ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, coalesce(total,0) as total from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Table'',''Ordinary view'',''Materialized view'',''Trigger'',''Synonym'',''Sequence'',''Index'',''Package'',''Package body'',''Function'',''Procedure'',''Constraint'') order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Rows by schema';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(total::text)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Rows by schema''' into schema_size, total_size;
	IF schema_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, coalesce(total,0) as total from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Rows by schema'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Rows by table';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(name_ref)), max(length(total::text)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Rows by table''' into object_size, total_size;
	IF object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Table ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF object_size < length(' Table ') THEN
			object_size := length(' Table ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Table ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select name_ref, coalesce(total,0) as total from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Rows by table'' order by 1'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '-- TABLES';
	RETURN NEXT '--' || lpad('-', 131, '-');
	RETURN NEXT '';
	
	RETURN NEXT '-- Table and column comments';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Table comment'',''Column comment'')' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Comment ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Comment ') THEN
			object_size := length(' Comment ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Comment ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, coalesce(total,0) as total from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Table comment'',''Column comment'') order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Attributes by table';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Attributes by table''' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Table ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Table ') THEN
			object_size := length(' Table ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Table ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, coalesce(total,0) as total from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Attributes by table'' order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Attributes by index';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(table_attr_pkfkidx)), max(length(total::text)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Attributes by index''' into schema_size, object_size, tab1_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Index ', 31, ' ') || '|' || rpad(' Table ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Index ') THEN
			object_size := length(' Index ');	
		END IF;
		IF tab1_size < length(' Table ') THEN
			tab1_size := length(' Table ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Index ', object_size+2, ' ') || '|' || rpad(' Table ', tab1_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', tab1_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, table_attr_pkfkidx, coalesce(total,0) as total from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description = ''Attributes by index'' order by 1,3,2,4 desc'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '| ' || rpad(v_res.table_attr_pkfkidx, tab1_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
		RETURN NEXT '';
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Attributes by primary and foreign key';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(table_attr_pkfkidx)), max(length(total::text)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description in (''Attributes by primary key'',''Attributes by foreign key'')' into schema_size, object_size, tab1_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Key ', 31, ' ') || '|' || rpad(' Table ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Key ') THEN
			object_size := length(' Key ');	
		END IF;
		IF tab1_size < length(' Table ') THEN
			tab1_size := length(' Table ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Key ', object_size+2, ' ') || '|' || rpad(' Table ', tab1_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', tab1_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, table_attr_pkfkidx, coalesce(total,0) as total from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and description in (''Attributes by primary key'',''Attributes by foreign key'') order by 1,3,2 desc'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '| ' || rpad(v_res.table_attr_pkfkidx, tab1_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
		RETURN NEXT '';
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Constraint types';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(total::text)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Primary key constraint'',''Foreign key constraint'',''Check constraint'',''Unique constraint'')' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Schema ', 31, ' ') || '|' || rpad(' Constraint ', 31, ' ') || '|' || rpad(' Total', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Schema ') THEN
			schema_size := length(' Schema ');	
		END IF;
		IF object_size < length(' Constraint ') THEN
			object_size := length(' Constraint ');	
		END IF;
		IF total_size < length(' Total ') THEN
			total_size := length(' Total ');	
		END IF;
		RETURN NEXT rpad(' Schema ', schema_size+2, ' ') || '|' || rpad(' Constraint ', object_size+2, ' ') || '|' || rpad(' Total', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, coalesce(total,0) as total from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and name_ref in (''Primary key constraint'',''Foreign key constraint'',''Check constraint'',''Unique constraint'') order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
		RETURN NEXT '';
	END IF;
	RETURN NEXT '';

	RETURN NEXT '-- Synonyms list';
	RETURN NEXT '--' || lpad('-', 131, '-');
	EXECUTE 'select max(length(schema_ref)), max(length(name_ref)), max(length(description)) from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and total=-1' into schema_size, object_size, total_size;
	IF schema_size is null and object_size is null and total_size is null THEN
		RETURN NEXT rpad(' Synonym ', 31, ' ') || '|' || rpad(' Reference ', 31, ' ') || '|' || rpad(' Link ', 10, ' ');
		RETURN NEXT rpad('-', 31, '-') || '+' || rpad('-', 31, '-') || '+' || rpad('-', 10, '-');
		RETURN NEXT '';
	ELSE
		IF schema_size < length(' Synonym ') THEN
			schema_size := length(' Synonym ');	
		END IF;
		IF object_size < length(' Reference ') THEN
			object_size := length(' Reference ');	
		END IF;
		IF total_size < length(' Link ') THEN
			total_size := length(' Link ');	
		END IF;
		RETURN NEXT rpad(' Synonym ', schema_size+2, ' ') || '|' || rpad(' Reference ', object_size+2, ' ') || '|' || rpad(' Link', total_size+2, ' ');
		RETURN NEXT rpad('-', schema_size+2, '-') || '+' || rpad('-', object_size+2, '-') || '+' || rpad('-', total_size+2, '-');
		FOR v_res IN EXECUTE 'select schema_ref, name_ref, description from oracle_validation where to_char(date_ref, ''yyyy-mm-dd'')= ''' || v_date || ''' and total=-1 order by 1,2'
		LOOP
			RETURN NEXT (' ' || rpad(v_res.schema_ref, schema_size+1, ' ') || '| ' || rpad(v_res.name_ref, object_size+1, ' ') || '|' || lpad(v_res.total::text, total_size+1, ' '));
		END LOOP;
		RETURN NEXT '';
	END IF;
	RETURN NEXT '';
	
	RETURN;
END;
$$ LANGUAGE plpgsql;


-- SELECT * FROM check_orapg.generate_oracle_file('2019-08-08', '''yudita'',''public''', '/tmp', 'oracle_validation')
CREATE FUNCTION generate_oracle_file(p_date date, p_schema_list text, p_location_file text, p_name_output_file text) RETURNS boolean AS
$$
DECLARE
	v_query text;
	v_search_path text;
	v_schema_list text;
BEGIN
	v_schema_list := replace($2, '''', '''''')::text;
	SELECT '"' || string_agg(nspname,'", "' ORDER BY nspname) || '"' INTO v_search_path FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_temp_1', 'pg_toast_temp_1');
	EXECUTE 'set search_path to ' || v_search_path;

	EXECUTE 'copy (SELECT * FROM oracle_file(''' || p_date ||''', '''|| v_schema_list || ''')) to ''' || p_location_file || '/' || p_name_output_file || '.txt''';

	RETURN true;
END;
$$ LANGUAGE plpgsql;
