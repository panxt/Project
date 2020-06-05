package com.panxt.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import com.panxt.util.Utils;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * 将官方例子再自己ES节点上
 */
public class TestES {

    RestClient client = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();
    String index = "zips";
    ObjectMapper mapper = new ObjectMapper();

    /**
     * 插入索引
     */
    @Test
    public void putIndexES() throws IOException {
        Map<String, String> mapping = ImmutableMap.of("city", "keyword", "state",
                "keyword", "pop", "long");
        ObjectNode mappings = mapper.createObjectNode();

        ObjectNode properties = mappings.with("mappings").with("properties");
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            Utils.applyMapping(properties, entry.getKey(), entry.getValue());
        }
        // create index and mapping
        final HttpEntity entity = new StringEntity(mapper.writeValueAsString(mappings),
                ContentType.APPLICATION_JSON);
        final Request r = new Request("PUT", "/" + index);
        r.setEntity(entity);
        client.performRequest(r);

    }

    /**
     * 将官方测试数据插入到自己es节点上
     */
    @Test
    public void insertBuilkES() throws IOException {

        final List<ObjectNode> documents = new ArrayList<>();
        Resources.readLines(TestES.class.getResource("/zips-mini.json"),
                StandardCharsets.UTF_8, new LineProcessor<Void>() {
                    @Override
                    public boolean processLine(String line) throws IOException {
                        line = line.replaceAll("_id", "id"); // _id is a reserved attribute in ES
                        documents.add((ObjectNode) mapper.readTree(line));
                        return true;
                    }
                    @Override
                    public Void getResult() {
                        return null;
                    }
                });

        List<String> bulk = new ArrayList<>(documents.size() * 2);
        for (ObjectNode doc : documents) {
            bulk.add(String.format(Locale.ROOT, "{\"index\": {\"_index\":\"%s\"}}", index));
            bulk.add(mapper.writeValueAsString(doc));
        }

        final StringEntity entity = new StringEntity(String.join("\n", bulk) + "\n",
                ContentType.APPLICATION_JSON);

        final Request r = new Request("POST", "/_bulk?refresh");
        r.setEntity(entity);
        client.performRequest(r);
    }


    /**
     * @return
     */
    public Connection getConn() {
        Connection connection = null;
        SchemaPlus root = null;
        try {
            connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
            root = connection.unwrap(CalciteConnection.class).getRootSchema();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        root.add("elastic", new ElasticsearchSchema(client, mapper, index));

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
     * 查询
     *
     * @param sql
     * @return
     * @throws SQLException
     */
    public ResultSet query(String sql) {
        Connection conn = getConn();
        ResultSet resultSet = null;
        try {
            resultSet = conn.prepareStatement(sql).executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }


    @Test
    public void testGroupBy() throws SQLException {
        ResultSet query = query("select state, city\n"
                + "from zips\n"
                + "group by state, city\n"
                + "order by city limit 10");

        Utils.ShowQueryResult(query);
    }

    @Test
    public void view() {
        //city=BROOKLYN; longitude=-73.956985; latitude=40.646694; pop=111396; state=NY; id=11226
        ResultSet query = query("select * from zips where city = 'BROOKLYN'");
        Utils.ShowQueryResult(query);
    }


    @Test
    public void emptyResult() {
        ResultSet query = query("select * from zips limit 0");
        Utils.ShowQueryResult(query);

        ResultSet query1 = query("select * from elastic.zips where _MAP['Foo'] = '_MISSING_'");
        Utils.ShowQueryResult(query1);
    }


    @Test
    public void basic() {

        ResultSet query1 = query("select * from elastic.zips");
        Utils.ShowQueryResult(query1);

        ResultSet query = query("select * from elastic.zips where _MAP['city'] = 'BROOKLYN'");
        Utils.ShowQueryResult(query);

        ResultSet query2 = query("select * from elastic.zips where"
                + " _MAP['city'] in ('BROOKLYN', 'WASHINGTON')");
        Utils.ShowQueryResult(query2);

        // lower-case
        ResultSet query3 = query("select * from elastic.zips where "
                + "_MAP['city'] in ('brooklyn', 'Brooklyn', 'BROOK') ");
        Utils.ShowQueryResult(query3);

        // missing field
        ResultSet query4 = query("select * from elastic.zips where _MAP['CITY'] = 'BROOKLYN'");
        Utils.ShowQueryResult(query4);

        // limit 0
        ResultSet query5 = query("select * from elastic.zips limit 0");
        Utils.ShowQueryResult(query5);
    }

    @Test
    public void testSort() {

        ResultSet query = query("select * from zips order by state");
        Utils.ShowQueryResult(query);
    }

    @Test
    public void testSortLimit() {
        ResultSet query = query("select state, pop from zips\n"
                + "order by state, pop offset 2 rows fetch next 3 rows only");
        Utils.ShowQueryResult(query);
    }

    @Test
    public void agg1() {

        ResultSet query = query("select count(*) from zips");
        Utils.ShowQueryResult(query);

        ResultSet query1 = query("select min(pop), max(pop) from zips");
        Utils.ShowQueryResult(query1);

        ResultSet query2 = query("select count(*), max(pop), min(pop), sum(pop), avg(pop) from zips");
        Utils.ShowQueryResult(query2);


    }

    @Test
    public void groupBy() {

        ResultSet query = query("select distinct state\n"
                + "from zips\n"
                + "limit 6");
        Utils.ShowQueryResult(query);

        ResultSet query1 = query("select state, city\n"
                + "from zips\n"
                + "group by state, city\n"
                + "order by city limit 10");
        Utils.ShowQueryResult(query1);


        ResultSet query2 = query("select min(pop), state\n"
                + "from zips\n"
                + "group by state\n"
                + "order by state limit 3");
        Utils.ShowQueryResult(query2);

        ResultSet query3 = query("select count(city), state\n"
                + "from zips\n"
                + "group by state\n"
                + "order by state limit 3");
        Utils.ShowQueryResult(query3);

        ResultSet query4 = query("select min(pop), max(pop), state\n"
                + "from zips\n"
                + "group by state\n"
                + "order by state desc limit 3");
        Utils.ShowQueryResult(query4);

    }
    @Test
    public void approximateCount() {

        ResultSet query = query("select state, approx_count_distinct(city), approx_count_distinct(pop) from zips"
                + " group by state order by state limit 3");
        Utils.ShowQueryResult(query);
    }

    @Test
    public void testOver() {

//        ResultSet query = query("select * from zips");
        ResultSet query = query("select city,row_number() over(partition by state order by id desc) from zips");
//        ResultSet query = query("select city from zips where state like 'M%' ");
        Utils.ShowQueryResult(query);
    }


}
