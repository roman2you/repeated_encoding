package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesReader;
import com.icongtai.zebra.encoding.impl.rle.RunLengthBitPackingHybridValuesWriter;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by robin on 13/10/16.
 */
public class RleTest {

    @Test
    public void testRLE() throws IOException {
        RunLengthBitPackingHybridValuesWriter writer = new RunLengthBitPackingHybridValuesWriter(50, 0, 32);
        int skip = 0;
        for(int i = 1; i < 58; ) {
            writer.writeInteger(i);
            if(++skip % 10 == 0) {
                i += 10;
            }
        }

        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);
        RunLengthBitPackingHybridValuesReader reader = new RunLengthBitPackingHybridValuesReader(false);
        reader.init(57, bytes, 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readInteger());
        }
    }

    @Test
    public void testRLE1() throws IOException {
        RunLengthBitPackingHybridValuesWriter writer = new RunLengthBitPackingHybridValuesWriter(51, 0, 32);
        for(int i = 1; i < 58; i++) {
            if(i > 49) {
                writer.writeInteger(51);
            } else if(i > 10) {
                writer.writeInteger(i);
            } else {
                writer.writeInteger(5);
            }
        }

        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);
        RunLengthBitPackingHybridValuesReader reader = new RunLengthBitPackingHybridValuesReader(false);
        reader.init(57, bytes, 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readInteger());
        }
    }

    @Test
    public void testRLE2() throws IOException {
        RunLengthBitPackingHybridValuesWriter writer = new RunLengthBitPackingHybridValuesWriter(51, -10, 32);
        for(int i = 1; i < 58; i++) {
            if(i > 49) {
                writer.writeInteger(51);
            } else if(i > 10) {
                writer.writeInteger(i - 20);
            } else {
                writer.writeInteger(5);
            }
        }

        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);
        RunLengthBitPackingHybridValuesReader reader = new RunLengthBitPackingHybridValuesReader(false);
        reader.init(57, bytes, 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readInteger());
        }
    }


    @Test
    public void testRLE3() throws IOException {
        RunLengthBitPackingHybridValuesWriter writer = new RunLengthBitPackingHybridValuesWriter(50L, 0L, 32);
        int skip = 0;
        for(int i = 1; i < 58; ) {
            writer.writeLong(i);
            if(++skip % 10 == 0) {
                i += 10;
            }
        }

        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);
        RunLengthBitPackingHybridValuesReader reader = new RunLengthBitPackingHybridValuesReader(true);
        reader.init(57, bytes, 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readLong());
        }
    }

    @Test
    public void testRLE4() throws IOException {
        RunLengthBitPackingHybridValuesWriter writer = new RunLengthBitPackingHybridValuesWriter(50L, -10L, 32);
        for(long i = 1; i < 58; i++) {
            if(i > 49) {
                writer.writeLong(51);
            } else if(i > 10) {
                writer.writeLong(i - 20);
            } else {
                writer.writeLong(5);
            }
        }

        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);
        RunLengthBitPackingHybridValuesReader reader = new RunLengthBitPackingHybridValuesReader(true);
        reader.init(57, bytes, 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readLong());
        }
    }

}
