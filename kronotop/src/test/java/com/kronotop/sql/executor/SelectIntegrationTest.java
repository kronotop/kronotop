package com.kronotop.sql.executor;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/*
INSERT INTO users (age, username) VALUES(35, 'buraksezer')

After Optimization:
19:EnumerableTableModify(table=[[public, users]], operation=[INSERT], flattened=[false]): rowcount = 1.0, cumulative cost = 3.0
  18:EnumerableProject(id=[''], age=[$0], username=[$1]): rowcount = 1.0, cumulative cost = 2.0
    17:EnumerableValues(tuples=[[{ 35, 'buraksezer' }]]): rowcount = 1.0, cumulative cost = 1.0
 */

/*
SELECT avg(age) FROM users

After Optimization:
36:EnumerableProject(EXPR$0=[CAST(/(CASE(=($1, 0), null:INTEGER, $0), $1)):INTEGER]): rowcount = 1.0, cumulative cost = 3.25
  35:EnumerableAggregate(group=[{}], agg#0=[$SUM0($0)], agg#1=[COUNT($0)]): rowcount = 1.0, cumulative cost = 2.25
    34:EnumerableProject(age=[$1]): rowcount = 1.0, cumulative cost = 1.0
      30:EnumerableTableScan(table=[[public, users]]): rowcount = 1.0, cumulative cost = 0.0
 */

/*
SELECT age, username FROM users ORDER BY age LIMIT 100

After Optimization:
39:EnumerableLimit(fetch=[100]): rowcount = 1.0, cumulative cost = 3.0
  38:EnumerableSort(sort0=[$0], dir0=[ASC]): rowcount = 1.0, cumulative cost = 2.0
    37:EnumerableProject(age=[$1], username=[$2]): rowcount = 1.0, cumulative cost = 1.0
      21:EnumerableTableScan(table=[[public, users]]): rowcount = 1.0, cumulative cost = 0.0
 */
public class SelectIntegrationTest extends BasePlanIntegrationTest {
    @Disabled("too early to run this")
    @Test
    public void test_SELECT() {
        executeSQLQuery("CREATE TABLE users (age INTEGER, username VARCHAR)");
        awaitSchemaMetadataForTable(DEFAULT_SCHEMA, "users");

        //executeSQLQuery("SELECT username as isim FROM users limit 10");
        executeSQLQuery("INSERT INTO users (age, username) VALUES(35, 'buraksezer')");
        //executeSQLQuery("SELECT avg(age) FROM users");
        //executeSQLQuery("SELECT username from users where age >= 20 order by age limit 100");
        executeSQLQuery("SELECT username FROM users WHERE age = 35");
    }
}
