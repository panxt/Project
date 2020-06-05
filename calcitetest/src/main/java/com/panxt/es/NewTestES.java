package com.panxt.es;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

import static com.panxt.mysql.TestMysql1.ShowQueryResult;

public class NewTestES {

    public static final EmbeddedElasticsearchPolicy NODE = EmbeddedElasticsearchPolicy.create();

    private static final String ZIPS = "zips";


    @BeforeClass
    public static void setup() throws Throwable {
        final Map<String, String> mapping = ImmutableMap.of("city", "keyword", "state",
                "keyword", "pop", "long");
        NODE.before();
        NODE.createIndex(ZIPS, mapping);

        // load records from file
        final List<ObjectNode> bulk = new ArrayList<>();
        Resources.readLines(TestES.class.getResource("/zips-mini.json"),
                StandardCharsets.UTF_8, new LineProcessor<Void>() {
                    @Override public boolean processLine(String line) throws IOException {
                        line = line.replaceAll("_id", "id"); // _id is a reserved attribute in ES
                        bulk.add((ObjectNode) NODE.mapper().readTree(line));
                        return true;
                    }
                    @Override public Void getResult() {
                        return null;
                    }
                });
        if (bulk.isEmpty()) {
            throw new IllegalStateException("No records to index. Empty file ?");
        }
        NODE.insertBulk(ZIPS, bulk);
    }

    /**
     * ES连接
     *
     * @return
     * @throws SQLException
     */
    public Connection createConnection() throws Throwable {
        final Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
        final SchemaPlus root = connection.unwrap(CalciteConnection.class).getRootSchema();

        root.add("elastic", new ElasticsearchSchema(NODE.restClient(), NODE.mapper(), ZIPS));
        // add calcite view programmatically
        final String viewSql = "select cast(_MAP['city'] AS varchar(20)) AS \"city\", "
                + " cast(_MAP['loc'][0] AS float) AS \"longitude\",\n"
                + " cast(_MAP['loc'][1] AS float) AS \"latitude\",\n"
                + " cast(_MAP['pop'] AS integer) AS \"pop\", "
                + " cast(_MAP['state'] AS varchar(2)) AS \"state\", "
                + " cast(_MAP['id'] AS varchar(5)) AS \"id\" "
                + "from \"elastic\".\"zips\"";

        ViewTableMacro macro = ViewTable.viewMacro(root, viewSql,
                Collections.singletonList("elastic"), Arrays.asList("elastic", "view"), false);
        root.add("zips", macro);
        return connection;
    }

    /**
     * TODO calcite查询ES-测试
     *
     * @throws SQLException
     */
    @Test
    public void querySqlOnES() throws Throwable {
        boolean materializationsEnabled = false;

        Connection connection = createConnection();
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

        /* properties */
//        Properties properties = calciteConnection.getProperties();
//        properties.setProperty(
//                CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
//                Boolean.toString(materializationsEnabled));
//        properties.setProperty(
//                CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
//                Boolean.toString(materializationsEnabled));
//        if (!properties.containsKey(CalciteConnectionProperty.TIME_ZONE.camelName())) {
//            properties.setProperty(
//                    CalciteConnectionProperty.TIME_ZONE.camelName(),
//                    DateTimeUtils.UTC_ZONE.getID());
//        }

        /*  */
        Statement statement = connection.createStatement();
//        String sql = "select * from zips where city = 'BROOKLYN'";
//        String sql = "select city,pop,state from zips where city = 'BROOKLYN'";
        String sql = "select city,count(*) `count` from zips group by city ";
        ResultSet resultSet = statement.executeQuery(sql);

        PreparedStatement preparedStatement = calciteConnection.prepareStatement(sql);
        ResultSet resultSet1 = preparedStatement.executeQuery();

        ShowQueryResult(resultSet1);

//        while (resultSet1.next()) {
//            String city = resultSet1.getString("city");
//            int pop = resultSet1.getInt("pop");
//            System.out.println("city = " + city + " pop = " + pop);
//        }


    }




}
