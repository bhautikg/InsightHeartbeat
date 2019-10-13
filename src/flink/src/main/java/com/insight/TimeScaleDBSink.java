/*
 * Define a sink of Flink to TimeScale Database
 *
 * @author Bhautik Gandhi
 */
package com.insight;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.*;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/*
 * Define a sink of Flink to TimeScale Database
 * Accepts String[] array, but not a tuple2 object
 */

public class TimeScaleDBSink extends RichSinkFunction<String[]> {

    private static final long serialVersionUID = 1L;

    private Connection connection;
    private PreparedStatement preparedStatement;
    /**
     * open will be executed once before invoke method
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        String USERNAME = "ecg" ;
        String PASSWORD = "pwd";
        String DRIVERNAME = "org.postgresql.Driver";
        String DBURL = "jdbc:postgresql://ec2-34-220-61-87.us-west-2.compute.amazonaws.com:5432/ecg";
        Class.forName(DRIVERNAME);
        // get the access to the database.
        connection = DriverManager.getConnection(DBURL,USERNAME,PASSWORD);
        String sql = "INSERT INTO signal_samples(signame, time, ecg, abnormal) VALUES " +
                "(?, TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.MS'), ?::float[], ?::varchar)";
        preparedStatement = connection.prepareStatement(sql);
        super.open(parameters);
    }

    /**
     * invoke() analyzed one element and insert it to the databbase
     * @param row
     * @throws Exception
     */
    //@Override
    public  void invoke(String[] row, SinkFunction.Context context) throws Exception{
        if(row.length == 4) {
            try {
                //inserts signal name
                preparedStatement.setString(1, row[0]);
                //inserts timestamp
                preparedStatement.setString(2, row[1]);
                // abnormal event True or fals (but varchar for display reasons)
                preparedStatement.setString(4, row[3]);

                //covert ECG raw signal from String to String Array to Double Array
                String str = row[2];
                str = StringUtils.substringBetween(str, "[", "]");

                String[] strArr = str.split(",");
                Double[] arr = new Double[strArr.length];
                for (int i = 0; i<strArr.length; i++)
                    arr[i] = Double.valueOf(strArr[i]);
                // postgress driver can only insert an array if its an Object class, primitive type array doesnt work
                Array ecg = connection.createArrayOf("FLOAT", arr);
                preparedStatement.setArray(3, ecg);
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            return;
        }

    }

    /**
     * close() is the mothod of tear down，it is executed to turn off the connection
     */
    @Override
    public void close() throws Exception {
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }
}