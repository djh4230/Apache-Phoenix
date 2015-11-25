/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class InsertParserTest {
    private void parseQuery(String sql) throws IOException, SQLException {
        SQLParser parser = new SQLParser(new StringReader(sql));
        BindableStatement stmt = null;
        stmt = parser.parseStatement();
        /*if (stmt.getOperation() != Operation.QUERY) {
            return;
        }*/
        String newSQL = stmt.toString();
        System.out.println(newSQL);
        SQLParser newParser = new SQLParser(new StringReader(newSQL));
        BindableStatement newStmt = null;
        try {
            newStmt = newParser.parseStatement();
        } catch (SQLException e) {
            fail("Unable to parse new:\n" + newSQL);
        }
        assertEquals("Expected equality:\n" + sql + "\n" + newSQL, stmt, newStmt);
    }
    
    /*@Test
    public void testParsePreQuery0() throws Exception {
        String sql = ((
            "select a from b\n" +
            "where ((ind.name = 'X')" +
            "and rownum <= (1000 + 1000))\n"
            ));
        parseQuery(sql);
    }*/
    @Test
    public void testSetParameter_Insert() throws Exception {
        Connection connection = getConnection("jdbc:phoenix:192.168.161.9:2181:/hbase");

        PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO \"TEST3\"(id,\"name\",\"sex\") " + "VALUES (?,?,?)");

        stmt.setString(1, "15");
        stmt.setString(2, "djh");
        stmt.setString(3, "m");

        stmt.execute();
        connection.commit();
       // PreparedStatement stmt1=connection.prepareStatement("select * from \"channellog\" where PK=14 and \"local\"='success'");
        //ResultSet result= stmt1.executeQuery();
        /*if(result.next()){
            throw new SQLException("primary key has exists","phoenix",201);
        }*/


    }
    public static Connection getConnection(String url) {
        Connection conn = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        if (conn == null) {
            try {
                conn = DriverManager.getConnection(url);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }
   /* @Test
    public void testParseInsert() throws Exception {
        String sql = "insert into a values('a','b')";
        parseQuery(sql);
    }*/
}
