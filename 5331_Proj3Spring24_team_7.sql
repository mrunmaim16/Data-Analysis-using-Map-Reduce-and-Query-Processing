SQL> -- Retrieve the top 5 movies with comedy and romance genres released between 2001 and 2010,
SQL> -- sorted by rating, with at least 150,000 votes.
SQL> SET LINESIZE 200
SQL> SET PAGESIZE 200
SQL> SET HEADING ON
SQL> SET VERIFY OFF
SQL> SET FEEDBACK OFF
SQL> COLUMN MOVIE_NAME FORMAT A50
SQL> COLUMN RATING FORMAT 999.99
SQL> COLUMN VOTES FORMAT 99999999
SQL> SELECT DISTINCT tb.PRIMARYTITLE AS movie_name, tr.AVERAGERATING AS rating, tr.NUMVOTES as votes
  2  FROM imdb00.TITLE_BASICS tb
  3  JOIN imdb00.TITLE_RATINGS tr ON tb.TCONST = tr.TCONST
  4  JOIN imdb00.TITLE_PRINCIPALS tp ON tb.TCONST = tp.TCONST
  5  JOIN imdb00.NAME_BASICS nb ON tp.NCONST = nb.NCONST
  6  WHERE tb.GENRES LIKE '%Comedy%' AND tb.GENRES LIKE '%Romance%'
  7  AND tb.TITLETYPE LIKE 'movie'
  8  AND tb.STARTYEAR BETWEEN '2001' AND '2010'
  9  AND tr.NUMVOTES >= 150000
 10  ORDER BY tr.AVERAGERATING DESC
 11  FETCH FIRST 5 ROWS ONLY;

MOVIE_NAME                                          RATING     VOTES                                                                                                                                    
-------------------------------------------------- ------- ---------                                                                                                                                    
Amelie                                                8.30    748048                                                                                                                                    
500 Days of Summer                                    7.70    507857                                                                                                                                    
Love Actually                                         7.60    476576                                                                                                                                    
Sideways                                              7.50    190672                                                                                                                                    
The Terminal                                          7.40    452046                                                                                                                                    

SQL> EXPLAIN PLAN FOR
  2  SELECT DISTINCT tb.PRIMARYTITLE AS movie_name, tr.AVERAGERATING AS rating, tr.NUMVOTES as votes
  3  FROM imdb00.TITLE_BASICS tb
  4  JOIN imdb00.TITLE_RATINGS tr ON tb.TCONST = tr.TCONST
  5  JOIN imdb00.TITLE_PRINCIPALS tp ON tb.TCONST = tp.TCONST
  6  JOIN imdb00.NAME_BASICS nb ON tp.NCONST = nb.NCONST
  7  WHERE tb.GENRES LIKE '%Comedy%' AND tb.GENRES LIKE '%Romance%'
  8  AND tb.TITLETYPE LIKE 'movie'
  9  AND tb.STARTYEAR BETWEEN '2001' AND '2010'
 10  AND tr.NUMVOTES >= 150000
 11  ORDER BY tr.AVERAGERATING DESC
 12  FETCH FIRST 5 ROWS ONLY;
SQL> SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);

PLAN_TABLE_OUTPUT                                                                                                                                                                                       
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Plan hash value: 457476648                                                                                                                                                                              
                                                                                                                                                                                                        
------------------------------------------------------------------------------------------------------                                                                                                  
| Id  | Operation                         | Name             | Rows  | Bytes | Cost (%CPU)| Time     |                                                                                                  
------------------------------------------------------------------------------------------------------                                                                                                  
|   0 | SELECT STATEMENT                  |                  |     5 |  5270 |   135K  (1)| 00:00:06 |                                                                                                  
|*  1 |  VIEW                             |                  |     5 |  5270 |   135K  (1)| 00:00:06 |                                                                                                  
|*  2 |   WINDOW SORT PUSHED RANK         |                  |   276 |   280K|   135K  (1)| 00:00:06 |                                                                                                  
|   3 |    VIEW                           |                  |   276 |   280K|   135K  (1)| 00:00:06 |                                                                                                  
|   4 |     HASH UNIQUE                   |                  |   276 | 34776 |   135K  (1)| 00:00:06 |                                                                                                  
|*  5 |      HASH JOIN SEMI               |                  |   276 | 34776 |   135K  (1)| 00:00:06 |                                                                                                  
|   6 |       NESTED LOOPS                |                  |   276 | 31740 |  3845   (1)| 00:00:01 |                                                                                                  
|   7 |        NESTED LOOPS               |                  |  1380 | 31740 |  3845   (1)| 00:00:01 |                                                                                                  
|*  8 |         TABLE ACCESS FULL         | TITLE_RATINGS    |  1380 | 23460 |  1084   (2)| 00:00:01 |                                                                                                  
|*  9 |         INDEX UNIQUE SCAN         | SYS_C00547784    |     1 |       |     1   (0)| 00:00:01 |                                                                                                  
|* 10 |        TABLE ACCESS BY INDEX ROWID| TITLE_BASICS     |     1 |    98 |     2   (0)| 00:00:01 |                                                                                                  
|  11 |       TABLE ACCESS FULL           | TITLE_PRINCIPALS |    51M|   538M|   131K  (1)| 00:00:06 |                                                                                                  
------------------------------------------------------------------------------------------------------                                                                                                  
                                                                                                                                                                                                        
Predicate Information (identified by operation id):                                                                                                                                                     
---------------------------------------------------                                                                                                                                                     
                                                                                                                                                                                                        
   1 - filter("from$_subquery$_009"."rowlimit_$$_rownumber"<=5)                                                                                                                                         
   2 - filter(ROW_NUMBER() OVER ( ORDER BY INTERNAL_FUNCTION("from$_subquery$_008"."RATING")                                                                                                            
              DESC )<=5)                                                                                                                                                                                
   5 - access("TB"."TCONST"="TP"."TCONST")                                                                                                                                                              
   8 - filter("TR"."NUMVOTES">=150000)                                                                                                                                                                  
   9 - access("TB"."TCONST"="TR"."TCONST")                                                                                                                                                              
  10 - filter("TB"."TITLETYPE"=U'movie' AND "TB"."STARTYEAR"<='2010' AND "TB"."GENRES" LIKE                                                                                                             
              U'%Comedy%' AND "TB"."GENRES" LIKE U'%Romance%' AND "TB"."STARTYEAR">='2001' AND                                                                                                          
              "TB"."GENRES" IS NOT NULL AND "TB"."GENRES" IS NOT NULL)                                                                                                                                  
SQL> spool off;
