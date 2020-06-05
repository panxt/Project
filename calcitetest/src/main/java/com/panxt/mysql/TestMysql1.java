package com.panxt.mysql;

import com.panxt.mysql.model.Department;
import com.panxt.mysql.model.Employee;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class TestMysql1 {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
//        TestInMemDatasource();
        TestJDBCDatasource(null);
//        TestJDBCDatasourceByJsonCof();
    }

    /**
     * 通过配置json文件配置mysql数据源
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void TestJDBCDatasourceByJsonCof() throws ClassNotFoundException, SQLException {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection =
                // 读取上面的配置文件
                DriverManager.getConnection("jdbc:calcite:model=D:\\workspace_learn\\calcitetest\\src\\main\\resources\\stu.json", info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from test.stu limit 10");
        ShowQueryResult(resultSet);
        resultSet.close();
        statement.close();
        connection.close();
    }

    /**
     * 设置连接mysql为数据源
     *
     * @param sql
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void TestJDBCDatasource (String sql)  throws ClassNotFoundException, SQLException{
        if (sql == null) {
            sql = "select * from test.stu limit 10";
        }
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        Class.forName("com.mysql.jdbc.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://localhost/test?characterEncoding=utf8&&useSSL=false");
        dataSource.setUsername("root");
        dataSource.setPassword("941112");
        Schema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");
        rootSchema.add("test", schema);
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        ShowQueryResult(resultSet);
        resultSet.close();
        statement.close();
        connection.close();
    }

    /**
     * 测试内存数据
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static void TestInMemDatasource () throws ClassNotFoundException, SQLException {
        HrSchema hs = createInmemSchema();
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        Schema schema = new ReflectiveSchema(hs);
        rootSchema.add("hr", schema);
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery(
                "select d.deptno, min(e.empid)\n"
                        + "from hr.emps as e\n"
                        + "join hr.depts as d\n"
                        + "  on e.deptno = d.deptno\n"
                        + "group by d.deptno\n"
                        + "having count(*) > 1");

        ShowQueryResult(resultSet);
        resultSet.close();
        statement.close();
        connection.close();
    }

    public static void ShowQueryResult(ResultSet resultSet) throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                buf.append(i > 1 ? "; " : "")
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getObject(i));
            }
            System.out.println(buf.toString());
            buf.setLength(0);
        }
    }


    public static HrSchema createInmemSchema () {
        HrSchema hs = new HrSchema();
        hs.depts[0] = new Department(1);
        hs.depts[1] = new Department(2);
        hs.emps[0] = new Employee(11,2);
        hs.emps[1] = new Employee(22,2);
        return hs;
    }
    public static class HrSchema {
        public final Employee[] emps = new Employee[2];
        public final Department[] depts = new Department[2];
    }
}
