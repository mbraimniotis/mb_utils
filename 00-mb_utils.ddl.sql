create table mb_source as select * from all_source where 1=2;

CREATE directory mb_utl_dir AS '/u01/app/oracle/applogs/work_area/mb_utl_dir';
GRANT READ ON DIRECTORY mb_utl_dir TO PUBLIC;
GRANT WRITE ON DIRECTORY mb_utl_dir TO PUBLIC;