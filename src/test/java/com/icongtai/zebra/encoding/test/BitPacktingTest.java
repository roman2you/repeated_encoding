package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.bitpacking.Packer;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.impl.bitpacking.BitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.bitpacking.BitPackingValuesWriter;
import com.icongtai.zebra.encoding.impl.bitpacking.ByteBitPackingValuesReader;
import com.icongtai.zebra.encoding.impl.bitpacking.ByteBitPackingValuesWriter;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by robin on 13/10/16.
 */
public class BitPacktingTest {

    @Test
    public void testByteBitPackingTest() throws IOException {
        ByteBitPackingValuesWriter writer = new ByteBitPackingValuesWriter(57, Packer.LITTLE_ENDIAN);
        for(int i = 1; i < 58 ; i++) {
            writer.writeInteger(i);
        }
        System.out.println(writer.getAllocatedSize());
        System.out.println(writer.getBufferedSize());
        BytesInput bytesInput = writer.getBytes();
        System.out.println(bytesInput.size());
        System.out.println(bytesInput.toByteArray().length);

        ByteBitPackingValuesReader reader = new ByteBitPackingValuesReader(57, Packer.LITTLE_ENDIAN);
        reader.init(57, bytesInput.toByteArray(), 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readInteger());
        }

    }


    @Test
    public void testBitPackingTest() throws IOException {
        BitPackingValuesWriter writer = new BitPackingValuesWriter(57, 16);
        for(int i = 1; i < 58 ; i++) {
            writer.writeInteger(i);
        }
        System.out.println(writer.getAllocatedSize());
        System.out.println(writer.getBufferedSize());
        BytesInput bytesInput = writer.getBytes();
        System.out.println(bytesInput.size());
        System.out.println(bytesInput.toByteArray().length);

        BitPackingValuesReader reader = new BitPackingValuesReader(57);
        reader.init(57, bytesInput.toByteArray(), 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readInteger());
        }

    }
}
