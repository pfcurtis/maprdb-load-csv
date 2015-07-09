//
// LoadCSV -- Hard coded example of loading a CSV file directly into MapR DB (HBase 0.98 API)
//
package com.mapr;

import java.io.IOException;
import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.GregorianCalendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.csv.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LoadCSV {
    /*
    "YEAR","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","UNIQUE_CARRIER","ORIGIN","ORIGIN_CITY_NAME","ORIGIN_STATE_NM","DEST","DEST_CITY_NAME","DEST_STATE_NM","CRS_DEP_TIME","DEP_TIME","DEP_DELAY","CRS_ARR_TIME","ARR_TIME","ARR_DELAY","ACTUAL_ELAPSED_TIME","AIR_TIME"
    */



    private static String TABLE_NAME = "/user/mapr/flight_data";
    private static String TABLE_CF = "cf1";
    private static String CSV_FILE = "flights.csv";
    private static String[] columns = {"YEAR","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","UNIQUE_CARRIER","ORIGIN","ORIGIN_CITY_NAME","ORIGIN_STATE_NM","DEST","DEST_CITY_NAME","DEST_STATE_NM","CRS_DEP_TIME","DEP_TIME","DEP_DELAY","CRS_ARR_TIME","ARR_TIME","ARR_DELAY","ACTUAL_ELAPSED_TIME","AIR_TIME"};
    /*  private static String[] columns = {"YEAR","MONTH","FL_DATE","CARRIER","FL_NUM","ORIGIN","ORIGIN_CITY_NAME",
                                           "ORIGIN_STATE_ABR","DEST","DEST_CITY_NAME","DEST_STATE_ABR","DEP_TIME",
                                           "DEP_DELAY_NEW","ARR_TIME","ARR_DELAY_NEW","CANCELLED",
                                           "DIVERTED"
                                          }; */

    public static void main(String[] args) {
        Log log = LogFactory.getLog(LoadCSV.class);
        FileReader in = null;
        HTable table = null;
        Iterable<CSVRecord>parser  = null;
        int count = 0;

        try {
            Configuration config = HBaseConfiguration.create();
            table = new HTable(config, TABLE_NAME);
        } catch (Exception e1) {
            e1.printStackTrace(System.out);
            System.exit(1);
        }

        log.debug("Reading Data");
        try {
            in = new FileReader(CSV_FILE);
            parser  = CSVFormat.RFC4180.withHeader().parse(in);
        } catch (Exception e2) {
            e2.printStackTrace(System.out);
            System.exit(1);
        }

        for (CSVRecord record : parser) {

// Create a key value that's unique
            String row_key = record.get("UNIQUE_CARRIER") + "-" + record.get("FL_NUM") + "-" + record.get("YEAR")+
                record.get("MONTH")+record.get("DAY_OF_MONTH");
            Put put = new Put(Bytes.toBytes(row_key));

            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("ARR_TIME"), Bytes.toBytes(record.get("ARR_TIME")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("CRS_ARR_TIME"), Bytes.toBytes(record.get("CRS_ARR_TIME")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("CRS_DEP_TIME"), Bytes.toBytes(record.get("CRS_DEP_TIME")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("DEP_TIME"), Bytes.toBytes(record.get("DEP_TIME")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("DEST"), Bytes.toBytes(record.get("DEST")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("DEST_CITY_NAME"), Bytes.toBytes(record.get("DEST_CITY_NAME")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("DEST_STATE_NM"), Bytes.toBytes(record.get("DEST_STATE_NM")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("ORIGIN"), Bytes.toBytes(record.get("ORIGIN")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("ORIGIN_CITY_NAME"), Bytes.toBytes(record.get("ORIGIN_CITY_NAME")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("ORIGIN_STATE_NM"), Bytes.toBytes(record.get("ORIGIN_STATE_NM")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("UNIQUE_CARRIER"), Bytes.toBytes(record.get("UNIQUE_CARRIER")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("FL_NUM"), Bytes.toBytes(record.get("FL_NUM")));
            put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("FL_DATE"), Bytes.toBytes(record.get("FL_DATE")));

            try {
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("ACTUAL_ELAPSED_TIME"),
                        Bytes.toBytes(Float.parseFloat(record.get("ACTUAL_ELAPSED_TIME"))));
            } catch (Exception e11) {}

            try {
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("AIR_TIME"),
                        Bytes.toBytes(Float.parseFloat(record.get("AIR_TIME"))));
            } catch (Exception e12) {}
            try {
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("ARR_DELAY"),
                        Bytes.toBytes(Float.parseFloat(record.get("ARR_DELAY"))));
            } catch (Exception e13) {}
            try {
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("DAY_OF_MONTH"),
                        Bytes.toBytes(Long.parseLong(record.get("DAY_OF_MONTH"))));
            } catch (Exception e14) {}
            try {
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("DAY_OF_WEEK"),
                        Bytes.toBytes(Long.parseLong(record.get("DAY_OF_WEEK"))));
            } catch (Exception e15) {}
            try {
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("DEP_DELAY"),
                        Bytes.toBytes(Float.parseFloat(record.get("DEP_DELAY"))));
            } catch (Exception e16) {}
            try {
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("MONTH"),
                        Bytes.toBytes(Long.parseLong(record.get("MONTH"))));
            } catch (Exception e17) {}
            try {
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("YEAR"),
                        Bytes.toBytes(Long.parseLong(record.get("YEAR"))));
            } catch (Exception e18) {}

// Try to construct a time field based on departure time
            try {

                GregorianCalendar c = new GregorianCalendar(
                    Integer.parseInt(record.get("YEAR")),
                    Integer.parseInt(record.get("MONTH")),
                    Integer.parseInt(record.get("DAY_OF_MONTH")),
                    Integer.parseInt(record.get("CRS_ARR_TIME").substring(0,2)),
                    Integer.parseInt(record.get("CRS_ARR_TIME").substring(2,4)));

                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes("ARRIVAL_TIME"),
                        Bytes.toBytes((long)(c.getTimeInMillis() / 1000.0)));

                table.put(put);
                log.info("Record: " + row_key);
                Thread.sleep(100);
            } catch (Exception e3) {
                log.info("Record: " + row_key + " failed.");
                e3.printStackTrace(System.out);
                System.exit(1);
            }
        }
        try {
            table.close();
            in.close();
        } catch (Exception e4) {
            e4.printStackTrace(System.out);
        }
    }

}
