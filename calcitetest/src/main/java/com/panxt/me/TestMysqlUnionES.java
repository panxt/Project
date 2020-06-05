package com.panxt.me;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.Test;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.panxt.mysql.TestMysql1.ShowQueryResult;

public class TestMysqlUnionES {


    @Test
    public void test() throws Throwable {

        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();

        /* start ES */
        ObjectMapper mapper = new ObjectMapper();
//        ElasticsearchSchemaFactory schemaFactory = new ElasticsearchSchemaFactory();
        Map<String, Object> mapping = ImmutableMap.of("id", "Integer", "name",
                "text");

        RestClient client = RestClient.builder(new HttpHost("linux-1", 9200, "http")).build();
        ElasticsearchSchema elasticsearchSchema = new ElasticsearchSchema(client, mapper,"stu");

//        Schema stu = schemaFactory.create(rootSchema, "stu", mapping);
        rootSchema.add("ES", elasticsearchSchema);
        /* end ES  */

        /* start MySql */
        Class.forName("com.mysql.jdbc.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://localhost/test?characterEncoding=utf8&&useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("941112");
        Schema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");
        rootSchema.add("test", schema);
        /* end MySql */

//        String sql = "select name from ES.stu as a join test.stu as b on a.id = b.id";
//        String viewSql = String.format(Locale.ROOT, "select CAST(_MAP['id'] AS integer) AS \"id\",CAST(_MAP['name'] AS varchar(20)) AS \"name\" from \"ES\"" +
//                ".\"stu\"", "stu");
        String viewSql = "select cast(_MAP['id'] AS integer) AS \"id\", " +
                "cast(_MAP['name'] AS varchar(20)) AS \"name\"" +
                " from \"ES\".\"stu\"";
        ViewTableMacro macro = ViewTable.viewMacro(rootSchema, viewSql,
                Collections.singletonList("ES"), Arrays.asList( "ES","view"), false);
        rootSchema.add("stu", macro);

//        String sql = "select * from test.stu";
//        String sql1 = "select CAST (_MAP['id'] as varchar ) as id from ES.stu";

        // TODO 支持联合查询
//        String sql = "select * from stu as a join test.stu as b on a.ID = b.id";

        // a.ID id,a.NAME a_name,b.name b_name,b.age b_age
        //TODO 支持order by
//        String sql = "select * from stu as a join test.stu as b on a.ID = b.id order by b.id asc ";

//        TODO 支持分组
        String sql = "select id,count(*) from stu group by  id";

        //TODO
        String sql1 = "select a.id from stu as a inner join test.stu as b on a.id = b.id group by a.id";

        CalciteConnection unwrap = connection.unwrap(CalciteConnection.class);
        PreparedStatement preparedStatement = unwrap.prepareStatement(sql1);

        ResultSet resultSet = preparedStatement.executeQuery();

        ShowQueryResult(resultSet);
/*        while (resultSet.next()) {
//            String map = resultSet.getString("_MAP");
            String name = resultSet.getString("name");
            int id = resultSet.getInt("id");
            String ID = resultSet.getString("NAME");
            System.out.println(ID);
            System.out.println("id = " + id + " name = " + name);
        }*/
        resultSet.close();

        preparedStatement.close();
        calciteConnection.close();
        connection.close();
    }

    /**
     * 只测试模板
     *
     * @throws SQLException
     */
    @Test
    public void testView() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        RestClient client = RestClient.builder(new HttpHost("linux-1", 9200)).build();
        ElasticsearchSchema elasticsearchSchema = new ElasticsearchSchema(client, new ObjectMapper(), "stu");
        rootSchema.add("elastic", elasticsearchSchema);

        String viewSql = "select CAST(_MAP['id'] AS integer) AS id,CAST(_MAP['name'] AS varchar(20)) AS name from " +
                "\"elastic\".\"stu\"";
        ViewTableMacro macro = ViewTable.viewMacro(rootSchema, viewSql,
                Collections.singletonList("elastic"), Arrays.asList("elastic", "view"), false);
        rootSchema.add("stu", macro);

//        CalciteConnection calciteConnection1 = connection.unwrap(CalciteConnection.class);
        String sql = "select ID ,NAME from stu";
        Statement calciteConnectionStatement = calciteConnection.createStatement();
        ResultSet resultSet = calciteConnectionStatement.executeQuery(sql);

        while (resultSet.next()) {
//            String map = resultSet.getString("_MAP");
            String name = resultSet.getString("NAME");
            int id = resultSet.getInt("ID");
            System.out.println("id = " + id + " name = " + name);
        }

    }

}
