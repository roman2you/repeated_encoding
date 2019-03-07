package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.impl.plain.BinaryPlainValuesReader;
import com.icongtai.zebra.encoding.impl.plain.PlainValuesReader;
import com.icongtai.zebra.encoding.impl.plain.PlainValuesWriter;
import org.junit.Test;

/**
 */
public class PlainTest {

    @Test
    public void testPlainInt() throws Exception {
        PlainValuesWriter plainValuesWriter = new PlainValuesWriter(57);
        for(int i = 0; i < 57; i++) {
            plainValuesWriter.writeInteger(i + 1);
        }

        PlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
        reader.init(57, plainValuesWriter.getBytes().toByteArray(), 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readInteger());
        }
    }

    @Test
    public void testPlainBinary() throws Exception {
        PlainValuesWriter plainValuesWriter = new PlainValuesWriter(57);
        for(int i = 0; i < 57; i++) {
            plainValuesWriter.writeBytes(Binary.fromString("test" + i));
        }

        BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
        reader.init(57, plainValuesWriter.getBytes().toByteArray(), 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readBytes().toStringUsingUTF8());
        }
    }
}
