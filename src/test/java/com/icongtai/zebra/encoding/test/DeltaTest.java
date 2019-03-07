package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.delta.DeltaIntBitPackingValuesWriter;
import com.icongtai.zebra.encoding.impl.delta.DeltaLongBitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.delta.DeltaLongBitPackingValuesWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Created by robin on 14/10/16.
 */
public class DeltaTest {

    @Test
    public void testDeltaInt() throws IOException {
        DeltaIntBitPackingValuesWriter writer = new DeltaIntBitPackingValuesWriter(64, 1, 32);
        for(int i = 1; i < 1024; ) {
            System.out.println(i);
            writer.writeInteger(i);
            i += new Random().nextInt(10);
        }
        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);

        DeltaIntBitPackingValuesReader reader = new DeltaIntBitPackingValuesReader();
        reader.init(129, bytes, 0);
        for(int i = 0; i < reader.getTotalValueCount(); i++) {
            System.out.println(reader.readInteger());
        }
    }

    @Test
    public void testDeltaLong() throws IOException {
        DeltaLongBitPackingValuesWriter writer = new DeltaLongBitPackingValuesWriter(64, 1, 32);
        long time = System.currentTimeMillis();
        for(int i = 0; i < 311; i++) {
            time += 1000;
            writer.writeLong(time);
            System.out.println(time);
        }
        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);

        DeltaLongBitPackingValuesReader reader = new DeltaLongBitPackingValuesReader();
        reader.init(129, bytes, 0);
        for(int i = 0; i < reader.getTotalValueCount(); i++) {
            System.out.println(reader.readLong());
        }
    }
}

