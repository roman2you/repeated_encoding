package com.icongtai.zebra.encoding.test;

import com.google.common.base.Charsets;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.config.Column;
import com.icongtai.zebra.encoding.format.ColumnType;
import com.icongtai.zebra.encoding.format.DataRecord;
import com.icongtai.zebra.encoding.values.DataReaderV1;
import com.icongtai.zebra.encoding.values.DataWriterV1;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * Created by robin on 21/10/16.
 */
public class DataWriterTest {

    @Test
    public void testWriter() throws IOException, ParseException {
        DataWriterV1 dataWriterV1 = new DataWriterV1();
        dataWriterV1.select(Column.latitude).select(Column.longitude).select(Column.speed).select(Column.bearing);
        dataWriterV1.select(Column.horizontal_accuracy);
        List<String> lines = FileUtils.readLines(new File("/Users/robin/Documents/gps_219.csv"), Charsets.UTF_8);
        Iterator<String> iterator = lines.iterator();
        for(int i = 0 ; i < 600 ; i++) {
            String content = iterator.next();
            String[] contents = content.split(",");
            Map<Column, Object> map = new HashMap<>();
            map.put(Column.latitude, Double.valueOf(contents[1]));
            map.put(Column.longitude, Double.valueOf(contents[2]));
            float speed = Float.valueOf(contents[3]);
            map.put(Column.speed, speed < 0 ? null : speed);
            float bearing =  Float.valueOf(contents[4]);
            map.put(Column.bearing, bearing < 0 ? null : bearing);
            Float f = Float.parseFloat(contents[5]);
            map.put(Column.horizontal_accuracy, f.intValue());
            dataWriterV1.writeRecord(map);
        }
        long time = System.currentTimeMillis();
        dataWriterV1.flush(false);
        System.out.println(">>>>>>>>>>>>time:" + (System.currentTimeMillis() - time));


        BytesInput r = dataWriterV1.getMessage();
        System.out.println(r.size());

        DataReaderV1 dataReaderV1 = new DataReaderV1(r.toByteArray());
        dataReaderV1.initMessage();
        dataReaderV1.readData(Column.latitude);
        dataReaderV1.readData(Column.longitude);
        dataReaderV1.readData(Column.speed);
        dataReaderV1.readData(Column.bearing);
        dataReaderV1.readData(Column.horizontal_accuracy);

        DataRecord record  = dataReaderV1.getRecord();
        for(;record.hasNext();) {
            DataRecord record1 = record.next();
            System.out.print(record1.getValue(Column.latitude, ColumnType.GPS_LL));
            System.out.print(",");
            System.out.print(record1.getValue(Column.longitude, ColumnType.GPS_LL));
            System.out.print(",");
            System.out.print(record1.getValue(Column.speed, ColumnType.GPS_SPEED));
            System.out.print(",");
            System.out.print(record1.getValue(Column.bearing, ColumnType.GPS_BEARING));
            System.out.print(",");
            System.out.print(record1.getValue(Column.horizontal_accuracy, ColumnType.UNSIGNED_INT));
            System.out.println("");
        }
    }

    @Test
    public void testWriter4CSV() throws IOException, ParseException {
        DataWriterV1 dataWriterV1 = new DataWriterV1();
        dataWriterV1.select(Column.latitude).select(Column.longitude).select(Column.tag).select(Column.timestamp);
        List<String> lines = FileUtils.readLines(new File("/Users/robin/Downloads/gps.csv"), Charsets.UTF_8);
        Iterator<String> iterator = lines.iterator();
        for(int i = 0 ; i < 294 ; i++) {
            String content = iterator.next();
            String[] contents = content.split(",");
            Map<Column, Object> map = new HashMap<>();
            map.put(Column.latitude, Double.valueOf(contents[0]));
            map.put(Column.longitude, Double.valueOf(contents[1]));
            map.put(Column.tag, Integer.parseInt(contents[2]));
            map.put(Column.timestamp, Long.parseLong(contents[3]));
            dataWriterV1.writeRecord(map);
        }
        long time = System.currentTimeMillis();
        dataWriterV1.flush(false);
        System.out.println(">>>>>>>>>>>>time:" + (System.currentTimeMillis() - time));


        BytesInput r = dataWriterV1.getMessage();
        System.out.println(r.size());

        DataReaderV1 dataReaderV1 = new DataReaderV1(r.toByteArray());
        dataReaderV1.initMessage();
        dataReaderV1.readData(Column.latitude);
        dataReaderV1.readData(Column.longitude);
        dataReaderV1.readData(Column.tag);
        dataReaderV1.readData(Column.timestamp);

        DataRecord record  = dataReaderV1.getRecord();
        for(;record.hasNext();) {
            DataRecord record1 = record.next();
            System.out.print(record1.getValue(Column.latitude, ColumnType.GPS_LL));
            System.out.print(",");
            System.out.print(record1.getValue(Column.longitude, ColumnType.GPS_LL));
            System.out.print(",");
            System.out.print(record1.getValue(Column.tag, ColumnType.UNSIGNED_INT));
            System.out.print(",");
            System.out.print(record1.getValue(Column.timestamp, ColumnType.UNSIGNED_LONG));
            System.out.println("");
        }
    }

}
