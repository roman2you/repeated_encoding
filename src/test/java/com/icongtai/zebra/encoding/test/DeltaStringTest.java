package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.impl.deltastrings.DeltaByteArrayReader;
import com.icongtai.zebra.encoding.impl.deltastrings.DeltaByteArrayWriter;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by robin on 14/10/16.
 */
public class DeltaStringTest {

    @Test
    public void testDeltaString()  throws IOException {
        DeltaByteArrayWriter deltaByteArrayWriter = new DeltaByteArrayWriter(128);
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123456"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123478"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123488"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123497"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123512"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123522"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123537"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123567"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123579"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123591"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.123691"));
        deltaByteArrayWriter.writeBytes(Binary.fromString("32.124291"));

        byte[] bytes = deltaByteArrayWriter.getBytes().toByteArray();
        System.out.println(bytes.length);

        DeltaByteArrayReader reader = new DeltaByteArrayReader();
        reader.init(12, bytes, 0);
        for(int i = 0; i < 12; i++) {
            System.out.println(reader.readBytes().toStringUsingUTF8());
        }
    }
}
