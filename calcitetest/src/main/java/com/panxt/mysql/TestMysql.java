package com.panxt.mysql;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;
import java.util.Properties;

/**
 * 将mysql数据库配置为calcte的数据源
 */
public class TestMysql {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.setProperty("remarks", "true");
        info.setProperty("parserFactory", "org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        System.out.println(calciteConnection.getProperties());
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        Class.forName("com.mysql.jdbc.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://localhost/test?characterEncoding=utf8&&useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("941112");
        Schema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");
        rootSchema.add("test", schema);
        Statement statement = calciteConnection.createStatement();

        //statement.execute("insert into test.stu values(4,'bb',40)");
        ResultSet resultSet = statement.executeQuery("select * from test.stu");
        while (resultSet.next()) {
            String name = resultSet.getString("name");
            System.out.println(name);
        }
//    statement.close();
        statement.close();
        connection.close();
    }
}
