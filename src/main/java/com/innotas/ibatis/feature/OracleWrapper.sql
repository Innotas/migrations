--
--    Copyright 2010-2017 the original author or authors.
--
--    Licensed under the Apache License, Version 2.0 (the "License");
--    you may not use this file except in compliance with the License.
--    You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
--    Unless required by applicable law or agreed to in writing, software
--    distributed under the License is distributed on an "AS IS" BASIS,
--    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--    See the License for the specific language governing permissions and
--    limitations under the License.
--

  declare
    v_large_sql  CLOB;
    v_num        NUMBER := 0;
    v_upperbound NUMBER;
    v_sql        DBMS_SQL.VARCHAR2S;
    v_cur        INTEGER;
    v_ret        NUMBER;
  begin
    v_large_sql := ?;
    v_upperbound := CEIL(DBMS_LOB.GETLENGTH(v_large_sql)/256);
    FOR i IN 1..v_upperbound
    LOOP
      v_sql(i) := DBMS_LOB.SUBSTR(v_large_sql
                                 ,256 -- amount
                                 ,((i-1)*256)+1 -- offset
                                 );
    END LOOP;
    v_cur := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cur, v_sql, 1, v_upperbound, FALSE, DBMS_SQL.NATIVE);
    v_ret := DBMS_SQL.EXECUTE(v_cur);
    DBMS_SQL.CLOSE_CURSOR(v_cur);
  EXCEPTION   
    when others then
      dbms_sql.close_cursor(v_cur);
    raise;
  END;
