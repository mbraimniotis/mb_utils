CREATE OR REPLACE PACKAGE BODY mb_utils IS

  /**
  * Author: mbraimni
  * Purpose: Various utilities
  * Created: 29-NOV-2019
  * Modified:
  */
  -- Global variables for csv utils
  g_out_type VARCHAR2(1) := 'F';

  g_sep VARCHAR2(5) := ',';

  g_add_quotes BOOLEAN := TRUE;

  g_quote_char VARCHAR2(1) := '"';

  --Unwrap object source code
  PROCEDURE pr_do_unwrap(p_source_name IN VARCHAR2,
                         p_source_type IN VARCHAR2) IS
    v_text        VARCHAR2(4000);
    v_line        INTEGER := 0;
    tmp           BLOB;
    tmp2          BLOB;
    tmp3          BLOB;
    b64_len       INTEGER;
    str           VARCHAR2(2000);
    charmap       RAW(256) := hextoraw(REPLACE('3D 65 85 B3 18 DB E2 87 F1 52 AB 63 4B B5 A0 5F' ||
                                               '7D 68 7B 9B 24 C2 28 67 8A DE A4 26 1E 03 EB 17' ||
                                               '6F 34 3E 7A 3F D2 A9 6A 0F E9 35 56 1F B1 4D 10' ||
                                               '78 D9 75 F6 BC 41 04 81 61 06 F9 AD D6 D5 29 7E' ||
                                               '86 9E 79 E5 05 BA 84 CC 6E 27 8E B0 5D A8 F3 9F' ||
                                               'D0 A2 71 B8 58 DD 2C 38 99 4C 48 07 55 E4 53 8C' ||
                                               '46 B6 2D A5 AF 32 22 40 DC 50 C3 A1 25 8B 9C 16' ||
                                               '60 5C CF FD 0C 98 1C D4 37 6D 3C 3A 30 E8 6C 31' ||
                                               '47 F5 33 DA 43 C8 E3 5E 19 94 EC E6 A3 95 14 E0' ||
                                               '9D 64 FA 59 15 C5 2F CA BB 0B DF F2 97 BF 0A 76' ||
                                               'B4 49 44 5A 1D F0 00 96 21 80 7F 1A 82 39 4F C1' ||
                                               'A7 D7 0D D1 D8 FF 13 93 70 EE 5B EF BE 09 B9 77' ||
                                               '72 E7 B2 54 B7 2A C7 73 90 66 20 0E 51 ED F8 7C' ||
                                               '8F 2E F4 12 C6 2B 83 CD AC CB 3B C4 4E C0 69 36' ||
                                               '62 02 AE 88 FC AA 42 08 A6 45 57 D3 9A BD E1 23' ||
                                               '8D 92 4A 11 89 74 6B 91 FB FE C9 01 EA 1B F7 CE',
                                               ' ',
                                               ''));
    v_offset      INTEGER;
    v_buffer_size BINARY_INTEGER := 4800;
    v_buffer_raw  RAW(4800);
    t_out         BLOB;
    t_tmp         BLOB;
    t_buffer      RAW(1);
    t_hdl         BINARY_INTEGER;
    t_s1          PLS_INTEGER; -- s1 part of adler32 checksum
    t_last_chr    PLS_INTEGER;
  BEGIN
    dbms_output.enable(NULL);
    dbms_lob.createtemporary(tmp, TRUE);
    dbms_lob.createtemporary(tmp2, TRUE);
    dbms_lob.createtemporary(tmp3, TRUE);
    dbms_lob.createtemporary(t_out, TRUE);
    dbms_lob.createtemporary(t_tmp, TRUE);
    --  type, owner and object name (package, package body, procedure or function) to unwrap 
    FOR c IN (SELECT line, text
              FROM   user_source
              WHERE  NAME = p_source_name
              AND    TYPE = p_source_type
              ORDER  BY line)
    LOOP
      IF c.line = 1
      THEN
        b64_len := to_number(regexp_substr(regexp_substr(c.text,
                                                         '^[0-9a-f]+ [0-9a-f]+$',
                                                         1,
                                                         1,
                                                         'm'),
                                           '[0-9a-f]+',
                                           1,
                                           2),
                             'XXXXXXXXXX');
        dbms_lob.append(tmp,
                        utl_raw.cast_to_raw(REPLACE(substr(c.text,
                                                           regexp_instr(c.text,
                                                                        '^[0-9a-f]+ [0-9a-f]+$',
                                                                        1,
                                                                        1,
                                                                        1,
                                                                        'm')),
                                                    chr(10),
                                                    '')));
      ELSE
        dbms_lob.append(tmp,
                        utl_raw.cast_to_raw(REPLACE(c.text, chr(10), '')));
      END IF;
    END LOOP;
    -- dbms_output.put_line(dbms_lob.getlength(tmp));
    -- dbms_lob.trim(tmp,b64_len);
    -- base64 unpack
    -- tmp := utl_encode.base64_decode(tmp);
    v_offset := 1;
    FOR i IN 1 .. ceil(dbms_lob.getlength(tmp) / v_buffer_size)
    LOOP
      dbms_lob.read(tmp, v_buffer_size, v_offset, v_buffer_raw);
      v_buffer_raw := utl_encode.base64_decode(v_buffer_raw);
      dbms_lob.writeappend(tmp2,
                           utl_raw.length(v_buffer_raw),
                           v_buffer_raw);
      v_offset := v_offset + v_buffer_size;
    END LOOP;
    -- remove first 20 bytes
    dbms_lob.copy(tmp3, tmp2, dbms_lob.getlength(tmp) - 20, 1, 21);
    -- recode by table charmap
    FOR i IN 1 .. dbms_lob.getlength(tmp3)
    LOOP
      dbms_lob.write(tmp3,
                     1,
                     i,
                     utl_raw.substr(charmap,
                                    utl_raw.cast_to_binary_integer(dbms_lob.substr(tmp3,
                                                                                   1,
                                                                                   i)) + 1,
                                    1));
    END LOOP;
    -- zlib unpack
    t_tmp := hextoraw('1F8B0800000000000003'); -- gzip header
    dbms_lob.copy(t_tmp, tmp3, dbms_lob.getlength(tmp3) - 2 - 4, 11, 3);
    dbms_lob.append(t_tmp, hextoraw('0000000000000000')); -- add a fake trailer
    t_hdl := utl_compress.lz_uncompress_open(t_tmp);
    t_s1  := 1;
    LOOP
      BEGIN
        utl_compress.lz_uncompress_extract(t_hdl, t_buffer);
      EXCEPTION
        WHEN OTHERS THEN
          EXIT;
      END;
      dbms_lob.append(t_out, t_buffer);
      t_s1 := MOD(t_s1 + to_number(rawtohex(t_buffer), 'xx'), 65521);
    END LOOP;
    t_last_chr := to_number(dbms_lob.substr(tmp3,
                                            2,
                                            dbms_lob.getlength(tmp3) - 1),
                            '0XXX') - t_s1;
    IF t_last_chr < 0
    THEN
      t_last_chr := t_last_chr + 65521;
    END IF;
    dbms_lob.append(t_out, hextoraw(to_char(t_last_chr, 'fm0X')));
    IF utl_compress.isopen(t_hdl)
    THEN
      utl_compress.lz_uncompress_close(t_hdl);
    END IF;
    str := '';
    FOR i IN 1 .. dbms_lob.getlength(t_out)
    LOOP
      IF utl_raw.cast_to_varchar2(dbms_lob.substr(t_out, 1, i)) = chr(10)
      THEN
        v_line := v_line + 1;
        INSERT INTO mb_source
          (owner, NAME, TYPE, line, text)
        VALUES
          (USER, p_source_name, p_source_type, v_line, str);
        --dbms_output.put_line(str);
        str := '';
      ELSE
        str := str ||
               utl_raw.cast_to_varchar2(dbms_lob.substr(t_out, 1, i));
      END IF;
      COMMIT;
    END LOOP;
    v_line := v_line + 1;
    INSERT INTO mb_source
      (owner, NAME, TYPE, line, text)
    VALUES
      (USER, p_source_name, p_source_type, v_line, str);
    COMMIT;
    --dbms_output.put_line(str);
    dbms_lob.freetemporary(t_tmp);
    dbms_lob.freetemporary(t_out);
    dbms_lob.freetemporary(tmp3);
    dbms_lob.freetemporary(tmp2);
    dbms_lob.freetemporary(tmp);
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line(p_source_name || ' ~ ' || SQLERRM);
      dbms_lob.freetemporary(t_tmp);
      dbms_lob.freetemporary(t_out);
      dbms_lob.freetemporary(tmp3);
      dbms_lob.freetemporary(tmp2);
      dbms_lob.freetemporary(tmp);
  END pr_do_unwrap;

  --Unwrap object source code(OVERLOADED)
  PROCEDURE pr_do_unwrap(p_source_name IN VARCHAR2,
                         p_source_type IN VARCHAR2,
                         p_owner       IN VARCHAR2) IS
    v_text        VARCHAR2(4000);
    v_line        INTEGER := 0;
    tmp           BLOB;
    tmp2          BLOB;
    tmp3          BLOB;
    b64_len       INTEGER;
    str           VARCHAR2(2000);
    charmap       RAW(256) := hextoraw(REPLACE('3D 65 85 B3 18 DB E2 87 F1 52 AB 63 4B B5 A0 5F' ||
                                               '7D 68 7B 9B 24 C2 28 67 8A DE A4 26 1E 03 EB 17' ||
                                               '6F 34 3E 7A 3F D2 A9 6A 0F E9 35 56 1F B1 4D 10' ||
                                               '78 D9 75 F6 BC 41 04 81 61 06 F9 AD D6 D5 29 7E' ||
                                               '86 9E 79 E5 05 BA 84 CC 6E 27 8E B0 5D A8 F3 9F' ||
                                               'D0 A2 71 B8 58 DD 2C 38 99 4C 48 07 55 E4 53 8C' ||
                                               '46 B6 2D A5 AF 32 22 40 DC 50 C3 A1 25 8B 9C 16' ||
                                               '60 5C CF FD 0C 98 1C D4 37 6D 3C 3A 30 E8 6C 31' ||
                                               '47 F5 33 DA 43 C8 E3 5E 19 94 EC E6 A3 95 14 E0' ||
                                               '9D 64 FA 59 15 C5 2F CA BB 0B DF F2 97 BF 0A 76' ||
                                               'B4 49 44 5A 1D F0 00 96 21 80 7F 1A 82 39 4F C1' ||
                                               'A7 D7 0D D1 D8 FF 13 93 70 EE 5B EF BE 09 B9 77' ||
                                               '72 E7 B2 54 B7 2A C7 73 90 66 20 0E 51 ED F8 7C' ||
                                               '8F 2E F4 12 C6 2B 83 CD AC CB 3B C4 4E C0 69 36' ||
                                               '62 02 AE 88 FC AA 42 08 A6 45 57 D3 9A BD E1 23' ||
                                               '8D 92 4A 11 89 74 6B 91 FB FE C9 01 EA 1B F7 CE',
                                               ' ',
                                               ''));
    v_offset      INTEGER;
    v_buffer_size BINARY_INTEGER := 4800;
    v_buffer_raw  RAW(4800);
    t_out         BLOB;
    t_tmp         BLOB;
    t_buffer      RAW(1);
    t_hdl         BINARY_INTEGER;
    t_s1          PLS_INTEGER; -- s1 part of adler32 checksum
    t_last_chr    PLS_INTEGER;
  BEGIN
    dbms_output.enable(NULL);
    dbms_lob.createtemporary(tmp, TRUE);
    dbms_lob.createtemporary(tmp2, TRUE);
    dbms_lob.createtemporary(tmp3, TRUE);
    dbms_lob.createtemporary(t_out, TRUE);
    dbms_lob.createtemporary(t_tmp, TRUE);
    --  type, owner and object name (package, package body, procedure or function) to unwrap 
    FOR c IN (SELECT line, text
              FROM   all_source
              WHERE  NAME = p_source_name
              AND    TYPE = p_source_type
              AND    owner = p_owner
              ORDER  BY line)
    LOOP
      IF c.line = 1
      THEN
        b64_len := to_number(regexp_substr(regexp_substr(c.text,
                                                         '^[0-9a-f]+ [0-9a-f]+$',
                                                         1,
                                                         1,
                                                         'm'),
                                           '[0-9a-f]+',
                                           1,
                                           2),
                             'XXXXXXXXXX');
        dbms_lob.append(tmp,
                        utl_raw.cast_to_raw(REPLACE(substr(c.text,
                                                           regexp_instr(c.text,
                                                                        '^[0-9a-f]+ [0-9a-f]+$',
                                                                        1,
                                                                        1,
                                                                        1,
                                                                        'm')),
                                                    chr(10),
                                                    '')));
      ELSE
        dbms_lob.append(tmp,
                        utl_raw.cast_to_raw(REPLACE(c.text, chr(10), '')));
      END IF;
    END LOOP;
    -- dbms_output.put_line(dbms_lob.getlength(tmp));
    -- dbms_lob.trim(tmp,b64_len);
    -- base64 unpack
    -- tmp := utl_encode.base64_decode(tmp);
    v_offset := 1;
    FOR i IN 1 .. ceil(dbms_lob.getlength(tmp) / v_buffer_size)
    LOOP
      dbms_lob.read(tmp, v_buffer_size, v_offset, v_buffer_raw);
      v_buffer_raw := utl_encode.base64_decode(v_buffer_raw);
      dbms_lob.writeappend(tmp2,
                           utl_raw.length(v_buffer_raw),
                           v_buffer_raw);
      v_offset := v_offset + v_buffer_size;
    END LOOP;
    -- remove first 20 bytes
    dbms_lob.copy(tmp3, tmp2, dbms_lob.getlength(tmp) - 20, 1, 21);
    -- recode by table charmap
    FOR i IN 1 .. dbms_lob.getlength(tmp3)
    LOOP
      dbms_lob.write(tmp3,
                     1,
                     i,
                     utl_raw.substr(charmap,
                                    utl_raw.cast_to_binary_integer(dbms_lob.substr(tmp3,
                                                                                   1,
                                                                                   i)) + 1,
                                    1));
    END LOOP;
    -- zlib unpack
    t_tmp := hextoraw('1F8B0800000000000003'); -- gzip header
    dbms_lob.copy(t_tmp, tmp3, dbms_lob.getlength(tmp3) - 2 - 4, 11, 3);
    dbms_lob.append(t_tmp, hextoraw('0000000000000000')); -- add a fake trailer
    t_hdl := utl_compress.lz_uncompress_open(t_tmp);
    t_s1  := 1;
    LOOP
      BEGIN
        utl_compress.lz_uncompress_extract(t_hdl, t_buffer);
      EXCEPTION
        WHEN OTHERS THEN
          EXIT;
      END;
      dbms_lob.append(t_out, t_buffer);
      t_s1 := MOD(t_s1 + to_number(rawtohex(t_buffer), 'xx'), 65521);
    END LOOP;
    t_last_chr := to_number(dbms_lob.substr(tmp3,
                                            2,
                                            dbms_lob.getlength(tmp3) - 1),
                            '0XXX') - t_s1;
    IF t_last_chr < 0
    THEN
      t_last_chr := t_last_chr + 65521;
    END IF;
    dbms_lob.append(t_out, hextoraw(to_char(t_last_chr, 'fm0X')));
    IF utl_compress.isopen(t_hdl)
    THEN
      utl_compress.lz_uncompress_close(t_hdl);
    END IF;
    str := '';
    FOR i IN 1 .. dbms_lob.getlength(t_out)
    LOOP
      IF utl_raw.cast_to_varchar2(dbms_lob.substr(t_out, 1, i)) = chr(10)
      THEN
        v_line := v_line + 1;
        INSERT INTO mb_source
          (owner, NAME, TYPE, line, text)
        VALUES
          (p_owner, p_source_name, p_source_type, v_line, str);
        --dbms_output.put_line(str);
        str := '';
      ELSE
        str := str ||
               utl_raw.cast_to_varchar2(dbms_lob.substr(t_out, 1, i));
      END IF;
      COMMIT;
    END LOOP;
    v_line := v_line + 1;
    INSERT INTO mb_source
      (owner, NAME, TYPE, line, text)
    VALUES
      (p_owner, p_source_name, p_source_type, v_line, str);
    COMMIT;
    --dbms_output.put_line(str);
    dbms_lob.freetemporary(t_tmp);
    dbms_lob.freetemporary(t_out);
    dbms_lob.freetemporary(tmp3);
    dbms_lob.freetemporary(tmp2);
    dbms_lob.freetemporary(tmp);
  EXCEPTION
    WHEN OTHERS THEN
      dbms_output.put_line(p_source_name || ' ~ ' || SQLERRM);
      dbms_lob.freetemporary(t_tmp);
      dbms_lob.freetemporary(t_out);
      dbms_lob.freetemporary(tmp3);
      dbms_lob.freetemporary(tmp2);
      dbms_lob.freetemporary(tmp);
  END pr_do_unwrap;

  PROCEDURE pr_unwrap(p_source_name IN VARCHAR2, p_source_type IN VARCHAR2) IS
    l_cnt   INTEGER;
    l_owner VARCHAR2(30) := USER;
  BEGIN
    SELECT COUNT(1)
    INTO   l_cnt
    FROM   user_source
    WHERE  NAME = upper(p_source_name)
    AND    TYPE = upper(p_source_type)
    AND    text LIKE '%wrapped%'
    AND    line = 1;
    IF l_cnt > 0
    THEN
      SELECT COUNT(1)
      INTO   l_cnt
      FROM   mb_source
      WHERE  NAME = upper(p_source_name)
      AND    TYPE = upper(p_source_type)
      AND    owner = upper(l_owner)
      AND    rownum < 2;
      IF l_cnt < 1
      THEN
        pr_do_unwrap(upper(p_source_name), upper(p_source_type));
        dbms_output.put_line(l_owner || '.' || p_source_name ||
                             ' unwrapped and inserted into MB_SOURCE.');
      ELSE
        dbms_output.put_line(p_source_name ||
                             ' is already unwrapped in MB_SOURCE.');
      END IF;
    ELSE
      dbms_output.put_line(p_source_name ||
                           ' is not a wrapped source. This will be transferred as is into MB_SOURCE.');
      INSERT INTO mb_source
        SELECT *
        FROM   all_source
        WHERE  NAME = upper(p_source_name)
        AND    TYPE = upper(p_source_type)
        AND    owner = upper(l_owner);
      COMMIT;
    END IF;
  END pr_unwrap;

  PROCEDURE pr_unwrap(p_source_name IN VARCHAR2,
                      p_source_type IN VARCHAR2,
                      p_owner       IN VARCHAR2) IS
    l_cnt INTEGER;
  BEGIN
    SELECT COUNT(1)
    INTO   l_cnt
    FROM   all_source
    WHERE  NAME = upper(p_source_name)
    AND    TYPE = upper(p_source_type)
    AND    owner = upper(p_owner)
    AND    text LIKE '%wrapped%'
    AND    line = 1;
    IF l_cnt > 0
    THEN
      SELECT COUNT(1)
      INTO   l_cnt
      FROM   mb_source
      WHERE  NAME = upper(p_source_name)
      AND    TYPE = upper(p_source_type)
      AND    owner = upper(p_owner)
      AND    rownum < 2;
      IF l_cnt < 1
      THEN
        pr_do_unwrap(upper(p_source_name),
                     upper(p_source_type),
                     upper(p_owner));
        dbms_output.put_line(p_owner || '.' || p_source_name ||
                             ' unwrapped and inserted into MB_SOURCE.');
      ELSE
        dbms_output.put_line(p_source_name ||
                             ' is already unwrapped in MB_SOURCE.');
      END IF;
    ELSE
      dbms_output.put_line(p_source_name ||
                           ' is not a wrapped source. This will be transferred as is into MB_SOURCE.');
      INSERT INTO mb_source
        SELECT *
        FROM   all_source
        WHERE  NAME = upper(p_source_name)
        AND    TYPE = upper(p_source_type)
        AND    owner = upper(p_owner);
      COMMIT;
    END IF;
  END pr_unwrap;

  PROCEDURE pr_print_source_to_output(p_source_name IN VARCHAR2,
                                      p_source_type IN VARCHAR2,
                                      p_owner       IN VARCHAR2) IS
    l_cnt INTEGER;
  BEGIN
    SELECT COUNT(1)
    INTO   l_cnt
    FROM   mb_source
    WHERE  NAME = p_source_name
    AND    TYPE = p_source_type
    AND    owner = p_owner
    AND    rownum < 2;
    IF l_cnt < 1
    THEN
      dbms_output.disable;
      pr_unwrap(p_source_name, p_source_type, p_owner);
    END IF;
    dbms_output.enable(1000000);
    dbms_output.put_line('CREATE OR REPLACE ');
    FOR rec IN (SELECT text
                FROM   mb_source
                WHERE  NAME = p_source_name
                AND    TYPE = p_source_type
                AND    owner = p_owner
                AND    text IS NOT NULL
                ORDER  BY line ASC)
    LOOP
      dbms_output.put_line(rec.text);
    END LOOP;
  END pr_print_source_to_output;

  PROCEDURE pr_print_source_to_file(p_source_name IN VARCHAR2,
                                    p_source_type IN VARCHAR2,
                                    p_owner       IN VARCHAR2,
                                    p_dir         IN VARCHAR2) IS
    l_cnt       INTEGER;
    l_file_name VARCHAR2(30);
    l_file_ext  VARCHAR2(30);
    l_file      utl_file.file_type;
  BEGIN
    SELECT COUNT(1)
    INTO   l_cnt
    FROM   mb_source
    WHERE  NAME = p_source_name
    AND    TYPE = p_source_type
    AND    owner = p_owner
    AND    rownum < 2;
    IF l_cnt < 1
    THEN
      pr_unwrap(p_source_name, p_source_type, p_owner);
    END IF;
    SELECT decode(p_source_type,
                  'PACKAGE',
                  '.spc.sql',
                  'PACKAGE BODY',
                  '.plb.sql',
                  'PROCEDURE',
                  '.prc.sql',
                  'FUNCTION',
                  '.fnc.sql',
                  'TRIGGER',
                  '.trg.sql',
                  'LIBRARY',
                  '.lib.sql',
                  'JAVA SOURCE',
                  '.java',
                  '.SQL')
    INTO   l_file_ext
    FROM   dual;
    l_file_name := p_source_name || l_file_ext;
    l_file      := utl_file.fopen(p_dir, l_file_name, 'W');
    utl_file.put_line(l_file, 'CREATE OR REPLACE ');
    FOR rec IN (SELECT text
                FROM   mb_source
                WHERE  NAME = p_source_name
                AND    TYPE = p_source_type
                AND    owner = p_owner
                AND    text IS NOT NULL
                ORDER  BY line ASC)
    LOOP
      utl_file.put_line(l_file, rec.text);
    END LOOP;
  END pr_print_source_to_file;

  PROCEDURE pr_create_ctl_file(p_table_name IN VARCHAR2, p_dir IN VARCHAR2) AS
    l_first_column VARCHAR2(100);
    l_last_column  VARCHAR2(100);
    f              utl_file.file_type;
  BEGIN
    f := utl_file.fopen(p_dir, p_table_name || '.ctl', 'W');
    utl_file.put_line(f, 'OPTIONS ( ERRORS=100000, ROWS=1000)');
    utl_file.put_line(f, 'LOAD DATA ');
    utl_file.put_line(f, 'CHARACTERSET  ''UTF8''');
    utl_file.put_line(f, 'BYTEORDERMARK NOCHECK');
    utl_file.put_line(f, 'INFILE ''' || p_table_name || '.csv''');
    utl_file.put_line(f, 'BADFILE ''' || p_table_name || '.bad''');
    utl_file.put_line(f, 'DISCARDFILE ''' || p_table_name || '.dsc''');
    utl_file.put_line(f, '');
    utl_file.put_line(f, 'INTO TABLE ' || p_table_name);
    utl_file.put_line(f, 'APPEND');
    utl_file.put_line(f, 'FIELDS TERMINATED BY ";"');
    utl_file.put_line(f, 'ENCLOSED BY "''"');
    utl_file.put_line(f, 'TRAILING NULLCOLS');
    SELECT column_name
    INTO   l_last_column
    FROM   all_tab_cols
    WHERE  owner = 'FCBOV'
    AND    table_name = p_table_name
    AND    column_id = (SELECT MAX(column_id)
                        FROM   all_tab_cols
                        WHERE  owner = 'FCBOV'
                        AND    table_name = p_table_name);
    SELECT column_name
    INTO   l_first_column
    FROM   all_tab_cols
    WHERE  owner = 'FCBOV'
    AND    table_name = p_table_name
    AND    column_id = (SELECT MIN(column_id)
                        FROM   all_tab_cols
                        WHERE  owner = 'FCBOV'
                        AND    table_name = p_table_name);
    FOR cols IN (SELECT decode(column_name,
                               l_first_column,
                               '(' || column_name || ',',
                               l_last_column,
                               column_name || ')',
                               column_name || ',') colname
                 FROM   all_tab_cols
                 WHERE  owner = 'FCBOV'
                 AND    table_name = p_table_name
                 ORDER  BY column_id ASC)
    LOOP
      utl_file.put_line(f, cols.colname);
    END LOOP;
    utl_file.fclose(f);
  END pr_create_ctl_file;

  PROCEDURE pr_wait_on_jobs(p_job_name  IN VARCHAR2,
                            p_sleep_sec INTEGER DEFAULT 60) IS
    l_job_name  VARCHAR2(200);
    l_sleep_sec INTEGER;
    l_cnt_jobs  INTEGER;
  BEGIN
    IF TRIM(p_job_name) IS NULL
    THEN
      RETURN;
    END IF;
    l_sleep_sec := greatest(10, nvl(p_sleep_sec, 0));
    l_job_name  := upper(TRIM(p_job_name)) || '%';
    dbms_lock.sleep(p_sleep_sec);
    SELECT 1
    INTO   l_cnt_jobs
    FROM   all_scheduler_jobs
    WHERE  job_name LIKE l_job_name
    AND    state IN ('RUNNING', 'SCHEDULED')
    AND    rownum = 1;
    pr_wait_on_jobs(p_job_name, p_sleep_sec);
  EXCEPTION
    WHEN no_data_found THEN
      NULL; -- will come out only if no job like this indeed scheduled or running
    WHEN OTHERS THEN
      pr_wait_on_jobs(p_job_name, p_sleep_sec);
  END pr_wait_on_jobs;

  PROCEDURE pr_exec_sh(p_script_name IN VARCHAR2,
                       p_path        IN VARCHAR2,
                       p_os_pass     IN VARCHAR2) AS
    l_script  VARCHAR2(32767);
    l_os_pass VARCHAR2(30) := p_os_pass;
  BEGIN
    dbms_credential.create_credential(credential_name => 'mb_utl_pass',
                                      username        => 'oracle',
                                      password        => l_os_pass);
    l_script := '#!/bin/bash' || chr(10) || 'cd ' || p_path || chr(10) ||
                p_script_name;
    dbms_output.put_line('SH_SCRIPT=' || l_script);
    dbms_scheduler.create_job(job_name        => 'PR_EXEC_SH',
                              job_type        => 'EXTERNAL_SCRIPT',
                              job_action      => l_script,
                              credential_name => 'mb_utl_pass',
                              enabled         => TRUE);
    pr_wait_on_jobs('PR_EXEC_SH');
    dbms_credential.drop_credential('mb_utl_pass');
  END pr_exec_sh;

  PROCEDURE pr_kill_scheduler_running_jobs AS
  BEGIN
    FOR rec IN (SELECT 'begin dbms_scheduler.stop_job (job_name=>' || '''' ||
                       job_name || '''' || ', force=>true); end;' || chr(13) ||
                       chr(10) || '/' AS cmd
                FROM   dba_scheduler_running_jobs)
    LOOP
      dbms_output.put_line(rec.cmd);
      --execute immediate rec.cmd;
    END LOOP;
  END pr_kill_scheduler_running_jobs;

  PROCEDURE pr_kill_active_sessions AS
  BEGIN
    FOR rec IN (SELECT 'alter system disconnect session ''' || sid || ',' ||
                       serial# || ',' || '@' || inst_id || ''' immediate;' AS cmd
                FROM   gv$session
                WHERE  status = 'ACTIVE')
    LOOP
      dbms_output.put_line(rec.cmd);
      --execute immediate rec.cmd;
    END LOOP;
  END pr_kill_active_sessions;

  PROCEDURE pr_kill_session(p_sid IN VARCHAR2) AS
  BEGIN
    FOR rec IN (SELECT 'alter system disconnect session ''' || sid || ',' ||
                       serial# || ',' || '@' || inst_id || ''' immediate;' AS cmd
                FROM   gv$session
                WHERE  status = 'ACTIVE'
                AND    sid = p_sid)
    LOOP
      dbms_output.put_line(rec.cmd);
      --execute immediat rec.cmd;
    END LOOP;
  END pr_kill_session;

  FUNCTION fn_base64encode(p_blob IN BLOB) RETURN CLOB IS
    l_clob CLOB;
    l_step PLS_INTEGER := 12000; -- make sure you set a multiple of 3 not higher than 24573
  BEGIN
    FOR i IN 0 .. trunc((dbms_lob.getlength(p_blob) - 1) / l_step)
    LOOP
      l_clob := l_clob ||
                utl_raw.cast_to_varchar2(utl_encode.base64_encode(dbms_lob.substr(p_blob,
                                                                                  l_step,
                                                                                  i *
                                                                                  l_step + 1)));
    END LOOP;
    RETURN l_clob;
  END;

  FUNCTION fn_base64decode(p_clob CLOB) RETURN BLOB IS
    l_blob   BLOB;
    l_raw    RAW(32767);
    l_amt    NUMBER := 7700;
    l_offset NUMBER := 1;
    l_temp   VARCHAR2(32767);
  BEGIN
    BEGIN
      dbms_lob.createtemporary(l_blob, FALSE, dbms_lob.call);
      LOOP
        dbms_lob.read(p_clob, l_amt, l_offset, l_temp);
        l_offset := l_offset + l_amt;
        l_raw    := utl_encode.base64_decode(utl_raw.cast_to_raw(l_temp));
        dbms_lob.append(l_blob, to_blob(l_raw));
      END LOOP;
    EXCEPTION
      WHEN no_data_found THEN
        NULL;
    END;
    RETURN l_blob;
  END;

  FUNCTION fn_blob_to_clob(p_data IN BLOB) RETURN CLOB AS
    l_clob         CLOB;
    l_dest_offset  PLS_INTEGER := 1;
    l_src_offset   PLS_INTEGER := 1;
    l_lang_context PLS_INTEGER := dbms_lob.default_lang_ctx;
    l_warning      PLS_INTEGER;
  BEGIN
    dbms_lob.createtemporary(lob_loc => l_clob, cache => TRUE);
    dbms_lob.converttoclob(dest_lob     => l_clob,
                           src_blob     => p_data,
                           amount       => dbms_lob.lobmaxsize,
                           dest_offset  => l_dest_offset,
                           src_offset   => l_src_offset,
                           blob_csid    => dbms_lob.default_csid,
                           lang_context => l_lang_context,
                           warning      => l_warning);
    RETURN l_clob;
  END;

  PROCEDURE pr_blob_to_file(p_blob     IN OUT NOCOPY BLOB,
                            p_dir      IN VARCHAR2,
                            p_filename IN VARCHAR2) AS
    l_file     utl_file.file_type;
    l_buffer   RAW(32767);
    l_amount   BINARY_INTEGER := 32767;
    l_pos      INTEGER := 1;
    l_blob_len INTEGER;
  BEGIN
    l_blob_len := dbms_lob.getlength(p_blob);
    -- Open the destination file.
    l_file := utl_file.fopen(p_dir, p_filename, 'wb', 32767);
    -- Read chunks of the BLOB and write them to the file until complete.
    WHILE l_pos <= l_blob_len
    LOOP
      dbms_lob.read(p_blob, l_amount, l_pos, l_buffer);
      utl_file.put_raw(l_file, l_buffer, TRUE);
      l_pos := l_pos + l_amount;
    END LOOP;
    -- Close the file.
    utl_file.fclose(l_file);
  EXCEPTION
    WHEN OTHERS THEN
      -- Close the file if something goes wrong.
      IF utl_file.is_open(l_file)
      THEN
        utl_file.fclose(l_file);
      END IF;
      RAISE;
  END pr_blob_to_file;

  PROCEDURE pr_file_to_blob(p_blob     IN OUT NOCOPY BLOB,
                            p_dir      IN VARCHAR2,
                            p_filename IN VARCHAR2) AS
    l_bfile       BFILE;
    l_dest_offset INTEGER := 1;
    l_src_offset  INTEGER := 1;
  BEGIN
    l_bfile := bfilename(p_dir, p_filename);
    dbms_lob.fileopen(l_bfile, dbms_lob.file_readonly);
    dbms_lob.trim(p_blob, 0);
    IF dbms_lob.getlength(l_bfile) > 0
    THEN
      dbms_lob.loadblobfromfile(dest_lob    => p_blob,
                                src_bfile   => l_bfile,
                                amount      => dbms_lob.lobmaxsize,
                                dest_offset => l_dest_offset,
                                src_offset  => l_src_offset);
    END IF;
    dbms_lob.fileclose(l_bfile);
  END pr_file_to_blob;

  FUNCTION fn_clob_to_blob(p_data IN CLOB) RETURN BLOB AS
    l_blob         BLOB;
    l_dest_offset  PLS_INTEGER := 1;
    l_src_offset   PLS_INTEGER := 1;
    l_lang_context PLS_INTEGER := dbms_lob.default_lang_ctx;
    l_warning      PLS_INTEGER := dbms_lob.warn_inconvertible_char;
  BEGIN
    dbms_lob.createtemporary(lob_loc => l_blob, cache => TRUE);
    dbms_lob.converttoblob(dest_lob     => l_blob,
                           src_clob     => p_data,
                           amount       => dbms_lob.lobmaxsize,
                           dest_offset  => l_dest_offset,
                           src_offset   => l_src_offset,
                           blob_csid    => dbms_lob.default_csid,
                           lang_context => l_lang_context,
                           warning      => l_warning);
    RETURN l_blob;
  END;

  PROCEDURE pr_clob_to_file(p_clob     IN OUT NOCOPY CLOB,
                            p_dir      IN VARCHAR2,
                            p_filename IN VARCHAR2) AS
    l_file   utl_file.file_type;
    l_buffer VARCHAR2(32767);
    l_amount BINARY_INTEGER := 32767;
    l_pos    INTEGER := 1;
  BEGIN
    l_file := utl_file.fopen(p_dir, p_filename, 'w', 32767);
    LOOP
      dbms_lob.read(p_clob, l_amount, l_pos, l_buffer);
      utl_file.put(l_file, l_buffer);
      l_pos := l_pos + l_amount;
    END LOOP;
  EXCEPTION
    WHEN no_data_found THEN
      -- Expected end.
      IF utl_file.is_open(l_file)
      THEN
        utl_file.fclose(l_file);
      END IF;
    WHEN OTHERS THEN
      IF utl_file.is_open(l_file)
      THEN
        utl_file.fclose(l_file);
      END IF;
      RAISE;
  END pr_clob_to_file;

  PROCEDURE pr_file_to_clob(p_clob     IN OUT NOCOPY CLOB,
                            p_dir      IN VARCHAR2,
                            p_filename IN VARCHAR2) AS
    l_bfile        BFILE;
    l_dest_offset  INTEGER := 1;
    l_src_offset   INTEGER := 1;
    l_bfile_csid   NUMBER := 0;
    l_lang_context INTEGER := 0;
    l_warning      INTEGER := 0;
  BEGIN
    l_bfile := bfilename(p_dir, p_filename);
    dbms_lob.fileopen(l_bfile, dbms_lob.file_readonly);
    dbms_lob.trim(p_clob, 0);
    dbms_lob.loadclobfromfile(dest_lob     => p_clob,
                              src_bfile    => l_bfile,
                              amount       => dbms_lob.lobmaxsize,
                              dest_offset  => l_dest_offset,
                              src_offset   => l_src_offset,
                              bfile_csid   => l_bfile_csid,
                              lang_context => l_lang_context,
                              warning      => l_warning);
    dbms_lob.fileclose(l_bfile);
  END pr_file_to_clob;

  PROCEDURE pr_rebuild_unusable_idx(p_idx_name        VARCHAR2,
                                    p_parallel_degree NUMBER DEFAULT 1) AS
    v_stmt            VARCHAR2(500);
    v_refcursor       SYS_REFCURSOR;
    v_parallel_degree NUMBER := p_parallel_degree;
  BEGIN
    SELECT 'alter index ' || index_name || ' logging noparallel'
    INTO   v_stmt
    FROM   user_indexes u
    WHERE  status = 'UNUSABLE'
    AND    index_name = p_idx_name;
    IF p_parallel_degree = 0
    THEN
      dbms_output.put_line('Parallel degree cannot be zero. This will be cosnidered as 1.');
      v_parallel_degree := 1;
    END IF;
    IF v_parallel_degree = 1
    THEN
      FOR rec IN (SELECT v_sql
                  FROM   (SELECT 'alter index ' || index_name ||
                                 ' rebuild nologging' v_sql
                          FROM   user_indexes u
                          WHERE  status = 'UNUSABLE'
                          AND    index_name = p_idx_name
                          ORDER  BY 1 ASC)
                  UNION ALL
                  SELECT 'alter index ' || u.index_name ||
                         ' rebuild partition ' || u.partition_name
                  FROM   user_ind_partitions u
                  WHERE  u.status = 'UNUSABLE'
                  AND    u.index_name = p_idx_name
                  UNION ALL
                  SELECT 'alter index ' || u.index_name ||
                         ' rebuild subpartition ' || u.subpartition_name
                  FROM   user_ind_subpartitions u
                  WHERE  u.status = 'UNUSABLE'
                  AND    u.index_name = p_idx_name)
      LOOP
        -- dbms_output.put_line(rec.v_sql || ';');
        EXECUTE IMMEDIATE rec.v_sql;
      END LOOP;
    ELSE
      FOR rec IN (SELECT v_sql
                  FROM   (SELECT 'alter index ' || index_name ||
                                 ' rebuild nologging parallel ' ||
                                 v_parallel_degree v_sql
                          FROM   user_indexes u
                          WHERE  status = 'UNUSABLE'
                          AND    index_name = p_idx_name
                          ORDER  BY 1 ASC)
                  UNION ALL
                  SELECT 'alter index ' || u.index_name ||
                         ' rebuild partition ' || u.partition_name ||
                         ' parallel ' || v_parallel_degree
                  FROM   user_ind_partitions u
                  WHERE  u.status = 'UNUSABLE'
                  AND    u.index_name = p_idx_name
                  UNION ALL
                  SELECT 'alter index ' || u.index_name ||
                         ' rebuild subpartition ' || u.subpartition_name ||
                         ' parallel ' || v_parallel_degree
                  FROM   user_ind_subpartitions u
                  WHERE  u.status = 'UNUSABLE'
                  AND    u.index_name = p_idx_name)
      LOOP
        -- dbms_output.put_line(rec.v_sql || ';');
        EXECUTE IMMEDIATE rec.v_sql;
      END LOOP;
    END IF;
    EXECUTE IMMEDIATE v_stmt;
  EXCEPTION
    WHEN OTHERS THEN
      CLOSE v_refcursor;
  END pr_rebuild_unusable_idx;

  PROCEDURE pr_rebuild_tbl_unusable_idxs(p_tbl_name        VARCHAR2,
                                         p_parallel_degree NUMBER DEFAULT 1) AS
    v_refcursor       SYS_REFCURSOR;
    v_stmt            VARCHAR2(500);
    v_parallel_degree NUMBER := p_parallel_degree;
  BEGIN
    OPEN v_refcursor FOR
      SELECT 'alter index ' || index_name || ' logging noparallel'
      FROM   user_indexes u
      WHERE  status = 'UNUSABLE'
      AND    table_name = p_tbl_name;
    IF p_parallel_degree = 0
    THEN
      dbms_output.put_line('Parallel degree cannot be zero. This will be cosnidered as 1.');
      v_parallel_degree := 1;
    END IF;
    IF v_parallel_degree = 1
    THEN
      FOR rec IN (SELECT v_sql
                  FROM   (SELECT 'alter index ' || index_name ||
                                 ' rebuild nologging' v_sql
                          FROM   user_indexes u
                          WHERE  status = 'UNUSABLE'
                          AND    table_name = p_tbl_name
                          ORDER  BY 1 ASC)
                  UNION ALL
                  SELECT 'alter index ' || u.index_name ||
                         ' rebuild partition ' || u.partition_name
                  FROM   user_ind_partitions u, user_indexes i
                  WHERE  u.status = 'UNUSABLE'
                  AND    u.index_name = i.index_name
                  AND    i.table_name = p_tbl_name
                  UNION ALL
                  SELECT 'alter index ' || u.index_name ||
                         ' rebuild subpartition ' || u.subpartition_name
                  FROM   user_ind_subpartitions u, user_indexes i
                  WHERE  u.status = 'UNUSABLE'
                  AND    u.index_name = i.index_name
                  AND    i.table_name = p_tbl_name)
      LOOP
        -- dbms_output.put_line(rec.v_sql || ';');
        EXECUTE IMMEDIATE rec.v_sql;
      END LOOP;
    ELSE
      FOR rec IN (SELECT v_sql
                  FROM   (SELECT 'alter index ' || index_name ||
                                 ' rebuild nologging parallel ' ||
                                 v_parallel_degree v_sql
                          FROM   user_indexes u
                          WHERE  status = 'UNUSABLE'
                          AND    table_name = p_tbl_name
                          ORDER  BY 1 ASC)
                  UNION ALL
                  SELECT 'alter index ' || u.index_name ||
                         ' rebuild partition ' || u.partition_name ||
                         ' parallel ' || v_parallel_degree
                  FROM   user_ind_partitions u, user_indexes i
                  WHERE  u.status = 'UNUSABLE'
                  AND    u.index_name = i.index_name
                  AND    i.table_name = p_tbl_name
                  UNION ALL
                  SELECT 'alter index ' || u.index_name ||
                         ' rebuild subpartition ' || u.subpartition_name ||
                         ' parallel ' || v_parallel_degree
                  FROM   user_ind_subpartitions u, user_indexes i
                  WHERE  u.status = 'UNUSABLE'
                  AND    u.index_name = i.index_name
                  AND    i.table_name = p_tbl_name)
      LOOP
        -- dbms_output.put_line(rec.v_sql || ';');
        EXECUTE IMMEDIATE rec.v_sql;
      END LOOP;
    END IF;
    LOOP
      FETCH v_refcursor
        INTO v_stmt;
      EXECUTE IMMEDIATE v_stmt;
      EXIT WHEN v_refcursor%NOTFOUND;
    END LOOP;
    -- Close cursor:
    CLOSE v_refcursor;
  EXCEPTION
    WHEN OTHERS THEN
      CLOSE v_refcursor;
  END pr_rebuild_tbl_unusable_idxs;

  -- Prototype for hidden procedures.
  PROCEDURE pr_generate_all(p_dir       IN VARCHAR2,
                            p_file      IN VARCHAR2,
                            p_query     IN VARCHAR2,
                            p_refcursor IN OUT SYS_REFCURSOR);

  PROCEDURE pr_put(p_file IN utl_file.file_type, p_text IN VARCHAR2);

  PROCEDURE pr_new_line(p_file IN utl_file.file_type);

  -- Stub to generate a CSV from a query.
  PROCEDURE pr_generate(p_dir   IN VARCHAR2,
                        p_file  IN VARCHAR2,
                        p_query IN VARCHAR2) AS
    l_cursor SYS_REFCURSOR;
  BEGIN
    g_out_type := 'F';
    pr_generate_all(p_dir       => p_dir,
                    p_file      => p_file,
                    p_query     => p_query,
                    p_refcursor => l_cursor);
  END pr_generate;

  -- Stub to generate a CVS from a REF CURSOR.
  PROCEDURE pr_generate_rc(p_dir       IN VARCHAR2,
                           p_file      IN VARCHAR2,
                           p_refcursor IN OUT SYS_REFCURSOR) AS
  BEGIN
    g_out_type := 'F';
    pr_generate_all(p_dir       => p_dir,
                    p_file      => p_file,
                    p_query     => NULL,
                    p_refcursor => p_refcursor);
  END pr_generate_rc;

  -- Stub to output a CSV from a query.
  PROCEDURE pr_output(p_query IN VARCHAR2) AS
    l_cursor SYS_REFCURSOR;
  BEGIN
    g_out_type := 'D';
    pr_generate_all(p_dir       => NULL,
                    p_file      => NULL,
                    p_query     => p_query,
                    p_refcursor => l_cursor);
  END pr_output;

  -- Stub to output a CVS from a REF CURSOR.
  PROCEDURE pr_output_rc(p_refcursor IN OUT SYS_REFCURSOR) AS
  BEGIN
    g_out_type := 'D';
    pr_generate_all(p_dir       => NULL,
                    p_file      => NULL,
                    p_query     => NULL,
                    p_refcursor => p_refcursor);
  END pr_output_rc;

  -- Do the actual work.
  PROCEDURE pr_generate_all(p_dir       IN VARCHAR2,
                            p_file      IN VARCHAR2,
                            p_query     IN VARCHAR2,
                            p_refcursor IN OUT SYS_REFCURSOR) AS
    l_cursor   PLS_INTEGER;
    l_rows     PLS_INTEGER;
    l_col_cnt  PLS_INTEGER;
    l_desc_tab dbms_sql.desc_tab;
    l_buffer   VARCHAR2(32767);
    l_is_str   BOOLEAN;
    l_file     utl_file.file_type;
  BEGIN
    IF p_query IS NOT NULL
    THEN
      l_cursor := dbms_sql.open_cursor;
      dbms_sql.parse(l_cursor, p_query, dbms_sql.native);
    ELSIF p_refcursor%ISOPEN
    THEN
      l_cursor := dbms_sql.to_cursor_number(p_refcursor);
    ELSE
      raise_application_error(-20000,
                              'You must specify a query or a REF CURSOR.');
    END IF;
    dbms_sql.describe_columns(l_cursor, l_col_cnt, l_desc_tab);
    FOR i IN 1 .. l_col_cnt
    LOOP
      dbms_sql.define_column(l_cursor, i, l_buffer, 32767);
    END LOOP;
    IF p_query IS NOT NULL
    THEN
      l_rows := dbms_sql.execute(l_cursor);
    END IF;
    IF g_out_type = 'F'
    THEN
      l_file := utl_file.fopen(p_dir, p_file, 'w', 32767);
    END IF;
    -- Output the column names.
    FOR i IN 1 .. l_col_cnt
    LOOP
      IF i > 1
      THEN
        pr_put(l_file, g_sep);
      END IF;
      pr_put(l_file, l_desc_tab(i).col_name);
    END LOOP;
    pr_new_line(l_file);
    -- Output the data.
    LOOP
      EXIT WHEN dbms_sql.fetch_rows(l_cursor) = 0;
      FOR i IN 1 .. l_col_cnt
      LOOP
        IF i > 1
        THEN
          pr_put(l_file, g_sep);
        END IF;
        -- Check if this is a string column.
        l_is_str := FALSE;
        IF l_desc_tab(i).col_type IN (dbms_types.typecode_varchar,
                         dbms_types.typecode_varchar2,
                         dbms_types.typecode_char,
                         dbms_types.typecode_clob,
                         dbms_types.typecode_nvarchar2,
                         dbms_types.typecode_nchar,
                         dbms_types.typecode_nclob)
        THEN
          l_is_str := TRUE;
        END IF;
        dbms_sql.column_value(l_cursor, i, l_buffer);
        -- Optionally add quotes for strings.
        IF g_add_quotes
           AND l_is_str
        THEN
          pr_put(l_file, g_quote_char);
          pr_put(l_file, l_buffer);
          pr_put(l_file, g_quote_char);
        ELSE
          pr_put(l_file, l_buffer);
        END IF;
      END LOOP;
      pr_new_line(l_file);
    END LOOP;
    IF utl_file.is_open(l_file)
    THEN
      utl_file.fclose(l_file);
    END IF;
    dbms_sql.close_cursor(l_cursor);
  EXCEPTION
    WHEN OTHERS THEN
      IF utl_file.is_open(l_file)
      THEN
        utl_file.fclose(l_file);
      END IF;
      IF dbms_sql.is_open(l_cursor)
      THEN
        dbms_sql.close_cursor(l_cursor);
      END IF;
      dbms_output.put_line('ERROR: ' ||
                           dbms_utility.format_error_backtrace);
      RAISE;
  END pr_generate_all;

  -- Alter separator from default.
  PROCEDURE pr_set_separator(p_sep IN VARCHAR2) AS
  BEGIN
    g_sep := p_sep;
  END pr_set_separator;

  -- Alter separator from default.
  PROCEDURE pr_set_quotes(p_add_quotes IN BOOLEAN := TRUE,
                          p_quote_char IN VARCHAR2 := '"') AS
  BEGIN
    g_add_quotes := nvl(p_add_quotes, TRUE);
    g_quote_char := nvl(substr(p_quote_char, 1, 1), '"');
  END pr_set_quotes;

  -- Handle put to file or screen.
  PROCEDURE pr_put(p_file IN utl_file.file_type, p_text IN VARCHAR2) AS
  BEGIN
    IF g_out_type = 'F'
    THEN
      utl_file.put(p_file, p_text);
    ELSE
      dbms_output.put(p_text);
    END IF;
  END pr_put;

  -- Handle newline to file or screen.
  PROCEDURE pr_new_line(p_file IN utl_file.file_type) AS
  BEGIN
    IF g_out_type = 'F'
    THEN
      utl_file.new_line(p_file);
    ELSE
      dbms_output.new_line;
    END IF;
  END pr_new_line;

  PROCEDURE pr_get_ddl_to_file(p_source_name IN VARCHAR2,
                               p_source_type IN VARCHAR2,
                               p_owner       IN VARCHAR2,
                               p_dir         IN VARCHAR2) IS
    l_clob        CLOB;
    l_source_type VARCHAR2(30) := upper(p_source_type);
    l_source_name VARCHAR2(30) := upper(p_source_name);
    l_owner       VARCHAR2(30) := upper(p_owner);
    l_file_name   VARCHAR2(30);
    l_file_ext    VARCHAR2(30);
  BEGIN
    dbms_metadata.set_transform_param(dbms_metadata.session_transform,
                                      'SQLTERMINATOR',
                                      TRUE);
    dbms_metadata.set_transform_param(dbms_metadata.session_transform,
                                      'PRETTY',
                                      TRUE);
    dbms_lob.createtemporary(l_clob, TRUE);
    
    SELECT CASE 
           WHEN OBJECT_TYPE = 'PACKAGE' THEN DBMS_METADATA.get_ddl ('PACKAGE_SPEC', OBJECT_NAME, OWNER)
           WHEN OBJECT_TYPE = 'PACKAGE BODY' THEN DBMS_METADATA.get_ddl ('PACKAGE_BODY', OBJECT_NAME, OWNER)
           WHEN OBJECT_TYPE = 'MATERIALIZED VIEW' THEN DBMS_METADATA.get_ddl ('MATERIALIZED_VIEW', OBJECT_name, owner)
           WHEN OBJECT_TYPE = 'JOB' THEN DBMS_METADATA.get_ddl ('PROCOBJ', OBJECT_name, owner)
           ELSE DBMS_METADATA.get_ddl (OBJECT_TYPE, OBJECT_NAME, OWNER)
           END
    INTO l_clob  
    FROM   all_OBJECTS
    WHERE  owner      = l_owner
    AND    OBJECT_name = l_source_name
    AND OBJECT_TYPE = l_source_type;
    --l_clob := dbms_metadata.get_ddl(l_source_type, l_source_name, l_owner);
    SELECT decode(p_source_type,
                  'PACKAGE',
                  '.spc.sql',
                  'PACKAGE BODY',
                  '.plb.sql',
                  'PROCEDURE',
                  '.prc.sql',
                  'FUNCTION',
                  '.fnc.sql',
                  'TRIGGER',
                  '.trg.sql',
                  'LIBRARY',
                  '.lib.sql',
                  'JAVA SOURCE',
                  '.java',
                  '.SQL')
    INTO   l_file_ext
    FROM   dual;
    l_file_name := lower(l_source_name) || l_file_ext;
    pr_clob_to_file(l_clob, p_dir, l_file_name);
    dbms_lob.freetemporary(l_clob);
  EXCEPTION
    WHEN OTHERS THEN
      dbms_lob.freetemporary(l_clob);
  END pr_get_ddl_to_file;
  
END mb_utils;
/
