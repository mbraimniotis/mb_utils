CREATE OR REPLACE PACKAGE mb_utils IS

  /**
  * Author: mbraimni
  * Purpose: Various utilities
  * DDl required: create table mb_source as select * from all_source where 1=2;
  * Created: 29-NOV-2019
  * Modified:
  */
  --Unwrap object source code
  PROCEDURE pr_unwrap(p_source_name IN VARCHAR2, p_source_type IN VARCHAR2);

  --Unwrap object source code(OVERLOADED)
  PROCEDURE pr_unwrap(p_source_name IN VARCHAR2,
                      p_source_type IN VARCHAR2,
                      p_owner       IN VARCHAR2);

  --Print source code in DBMS_OUTPUT
  PROCEDURE pr_print_source_to_output(p_source_name IN VARCHAR2,
                                      p_source_type IN VARCHAR2,
                                      p_owner       IN VARCHAR2);

  --Print source code to file in mb_utl_dir
  PROCEDURE pr_print_source_to_file(p_source_name IN VARCHAR2,
                                    p_source_type IN VARCHAR2,
                                    p_owner       IN VARCHAR2,
                                    p_dir         IN VARCHAR2);

  --Create ctl file for sqlldr
  PROCEDURE pr_create_ctl_file(p_table_name IN VARCHAR2, p_dir IN VARCHAR2);

  --Wait scheduler job(s)
  PROCEDURE pr_wait_on_jobs(p_job_name  VARCHAR2,
                            p_sleep_sec INTEGER DEFAULT 60);

  --Execute sh script 
  PROCEDURE pr_exec_sh(p_script_name IN VARCHAR2,
                       p_path        IN VARCHAR2,
                       p_os_pass     IN VARCHAR2);

  --Kill session
  PROCEDURE pr_kill_session(p_sid IN VARCHAR2);

  --Kill all active sessions
  PROCEDURE pr_kill_active_sessions;

  --Kill scheduler running jobs
  PROCEDURE pr_kill_scheduler_running_jobs;

  --Encodes a BLOB into a Base64 CLOB
  FUNCTION fn_base64encode(p_blob IN BLOB) RETURN CLOB;

  --Decodes a Base64 CLOB into a BLOB
  FUNCTION fn_base64decode(p_clob CLOB) RETURN BLOB;

  --Converts a BLOB to a CLOB.
  FUNCTION fn_blob_to_clob(p_data IN BLOB) RETURN CLOB;

  --Writes the contents of a BLOB to a file
  PROCEDURE pr_blob_to_file(p_blob     IN OUT NOCOPY BLOB,
                         p_dir      IN VARCHAR2,
                         p_filename IN VARCHAR2);

  --Loads the contents of a file into a BLOB
  PROCEDURE pr_file_to_blob(p_blob     IN OUT NOCOPY BLOB,
                         p_dir      IN VARCHAR2,
                         p_filename IN VARCHAR2);

  --Converts a CLOB to a BLOB
  FUNCTION fn_clob_to_blob(p_data IN CLOB) RETURN BLOB;

  --Writes the contents of a CLOB to a file
  PROCEDURE pr_clob_to_file(p_clob     IN OUT NOCOPY CLOB,
                         p_dir      IN VARCHAR2,
                         p_filename IN VARCHAR2);

  --Loads the contents of a file into a CLOB
  PROCEDURE pr_file_to_clob(p_clob     IN OUT NOCOPY CLOB,
                         p_dir      IN VARCHAR2,
                         p_filename IN VARCHAR2);

  --Rebuild unusable index
  PROCEDURE pr_rebuild_unusable_idx(p_idx_name        VARCHAR2,
                                    p_parallel_degree NUMBER DEFAULT 1);

  --Rebuild unusable table indexes
  PROCEDURE pr_rebuild_tbl_unusable_idxs(p_tbl_name        VARCHAR2,
                                         p_parallel_degree NUMBER DEFAULT 1);

  --Generate a CSV from a query
  PROCEDURE pr_generate(p_dir   IN VARCHAR2,
                     p_file  IN VARCHAR2,
                     p_query IN VARCHAR2);

  --Generate a CSV from a REF CURSOR
  PROCEDURE pr_generate_rc(p_dir       IN VARCHAR2,
                        p_file      IN VARCHAR2,
                        p_refcursor IN OUT SYS_REFCURSOR);

  --Displays to output a CSV from a query
  PROCEDURE pr_output(p_query IN VARCHAR2);

  --Displays to output a CSV from a REF CURSOR
  PROCEDURE pr_output_rc(p_refcursor IN OUT SYS_REFCURSOR);

  --Alter separator from default
  PROCEDURE pr_set_separator(p_sep IN VARCHAR2);

  --Alter quotes from default
  PROCEDURE pr_set_quotes(p_add_quotes IN BOOLEAN := TRUE,
                       p_quote_char IN VARCHAR2 := '"');

END mb_utils;
/
