package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.impl.ext.GpsSpeedValuesReader;
import com.icongtai.zebra.encoding.impl.ext.GpsSpeedValuesWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Created by robin on 17/10/16.
 */
public class GpsSpeedTest  {

    @Test
    public void test() throws IOException {
        GpsSpeedValuesWriter writer = new GpsSpeedValuesWriter(32);
        for(int i = 0; i < 500; i++) {
            if(i > 50 && i < 80) {
                writer.writeInteger(0);
            } else {
                writer.writeInteger(new Random().nextInt(1300));
            }
        }

        byte[] bytes = writer.getBytes().toByteArray();

        System.out.println(bytes.length);

        GpsSpeedValuesReader reader = new GpsSpeedValuesReader();
        reader.init(500, bytes, 0);
        for(int i = 0; i < 500; i++) {
            System.out.println(reader.readInteger());
        }

    }
}
