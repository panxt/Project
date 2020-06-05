package com.panxt.es.sql_on_es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static com.panxt.mysql.TestMysql1.ShowQueryResult;

public class SqlOnES {
    /* 编写calcite连接ES服务器,使用SQL进行操作的测试类 */

    /**
     * 1.基于代码进行配置
     * @throws SQLException
     */
    @Test
    public void test() throws SQLException {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        final RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build();
        ElasticsearchSchema elasticsearchSchema = new ElasticsearchSchema(client, new ObjectMapper(), "zips");
        rootSchema.add("ES", elasticsearchSchema);
        String sql = "select * from ES.zips";
//        String sql = "select ES.stu.id,ES.stu.name from ES.stu";
        PreparedStatement preparedStatement = calciteConnection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

//        String sql ="insert into ES.stu value(id,name ) values(3,'yuzi');";
//        calciteConnection.prepareStatement(sql);

        ShowQueryResult(resultSet);
        preparedStatement.close();
        calciteConnection.close();
        connection.close();
    }

    /**
     * 2.基于文件进行配置
     * @throws SQLException
     */
    @Test
    public void test1() throws SQLException {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:model=D:\\workspace_learn\\calcitetest\\src\\main\\resources\\stu1.json", info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        Statement statement = calciteConnection.createStatement();

//        String sql ="insert into elasticsearch.stu ('id','name') values(3,'yuzi') ";
//        boolean execute = statement.execute(sql);
//        System.out.println(execute);

        ResultSet resultSet = statement.executeQuery("select * from elasticsearch.stu");
//        ResultSet resultSet = statement.executeQuery("select * from elasticsearch.stu group by id");
        ShowQueryResult(resultSet);
        resultSet.close();
        statement.close();
        connection.close();
    }


}
