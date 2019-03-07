package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.impl.ext.GpsBearingValuesReader;
import com.icongtai.zebra.encoding.impl.ext.GpsBearingValuesWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Created by robin on 17/10/16.
 */
public class GpsBearingTest {

    @Test
    public void test() throws IOException {
        GpsBearingValuesWriter writer = new GpsBearingValuesWriter(32);
        for(int i = 0; i < 50; i++) {
            if(i > 10 && i < 30) {
                System.out.print(0);
                System.out.print(",");
                writer.writeInteger(0);
            } else {
                int v = new Random().nextInt(3600);
                System.out.print(v);
                System.out.print(",");
                writer.writeInteger(v);
            }
        }

        byte[] bytes = writer.getBytes().toByteArray();

        System.out.println(bytes.length);

        GpsBearingValuesReader reader = new GpsBearingValuesReader();
        reader.init(50, bytes, 0);
        for(int i = 0; i < 50; i++) {
            System.out.print(reader.readInteger());
            System.out.print(",");
        }

    }
}
