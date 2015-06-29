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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.csv.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LoadCSV {

    private static String TABLE_NAME = "/user/mapr/flightdata";
    private static String TABLE_CF = "cf1";
    private static String CSV_FILE = "364949592_T_ONTIME.csv";
    private static String[] columns = {"YEAR","MONTH","FL_DATE","CARRIER","FL_NUM","ORIGIN","ORIGIN_CITY_NAME",
                                       "ORIGIN_STATE_ABR","DEST","DEST_CITY_NAME","DEST_STATE_ABR","DEP_TIME",
                                       "DEP_DELAY_NEW","ARR_TIME","ARR_DELAY_NEW","CANCELLED",
                                       "DIVERTED"
                                      };

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
            String carrier = record.get("CARRIER");
            String flight_num = record.get("FL_NUM");
            String flight_date = record.get("FL_DATE");

// Create a key value that's unique
            Put put = new Put(Bytes.toBytes(carrier + "-" + flight_num + "-" + flight_date));
            log.debug("Key: " + carrier + "-" + flight_num + "-" + flight_date);

            for (String columnName : columns) {
                log.debug("Add columns: "+TABLE_CF+":"+columnName+" = "+record.get(columnName));
                put.add(Bytes.toBytes(TABLE_CF), Bytes.toBytes(columnName), Bytes.toBytes(record.get(columnName)));
            }
            try {
                table.put(put);
                log.info("Record: " + carrier + "-" + flight_num + "-" + flight_date);
                Thread.sleep(1000);
            } catch (Exception e3) {
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
