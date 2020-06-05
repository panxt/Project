package com.panxt.util;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.PrimitiveIterator;

public class Utils {


    /**
     * 打印resSet
     *
     * @param resultSet
     * @throws SQLException
     */
    public static void ShowQueryResult(ResultSet resultSet) {
        final StringBuilder buf = new StringBuilder();
        try {
            while (resultSet.next()) {
                int n = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= n; i++) {
                    buf.append(i > 1 ? "; " : "")
                            .append(resultSet.getMetaData().getColumnLabel(i))
                            .append("=")
                            .append(resultSet.getObject(i));
                }
//                System.out.println(resultSet.hashCode() + " : "  + buf.toString());
                System.out.println( buf.toString());
                buf.setLength(0);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    public static void applyMapping(ObjectNode parent, String key, String type) {
        final int index = key.indexOf('.');
        if (index > -1) {
            String prefix = key.substring(0, index);
            String suffix = key.substring(index + 1, key.length());
            applyMapping(parent.with(prefix).with("properties"), suffix, type);
        } else {
            parent.with(key).put("type", type);
        }
    }
}
