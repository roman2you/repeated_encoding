package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.impl.varplain.VarPlainValuesReader;
import com.icongtai.zebra.encoding.impl.varplain.VarPlainValuesWriter;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by robin on 13/10/16.
 */
public class VarPlainTest {

    @Test
    public void testVarIntPlain() throws IOException {
        VarPlainValuesWriter writer = new VarPlainValuesWriter(16, true);
        for(int i = 100; i < 157; i++) {
            writer.writeInteger(i);
        }

        byte[] encodes = writer.getBytes().toByteArray();
        System.out.println(encodes.length);

        VarPlainValuesReader.VarIntegerPlainValuesReader reader = new VarPlainValuesReader.VarIntegerPlainValuesReader(true);
        reader.init(57, encodes, 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readInteger());
        }
    }

    @Test
    public void testVarLongPlain() throws IOException {
        VarPlainValuesWriter writer = new VarPlainValuesWriter(16, true);
        for(long i = Integer.MAX_VALUE; i < Integer.MAX_VALUE + 57; i++) {
            writer.writeLong(i);
        }

        byte[] encodes = writer.getBytes().toByteArray();
        System.out.println(encodes.length);

        VarPlainValuesReader.VarLongPlainValuesReader reader = new VarPlainValuesReader.VarLongPlainValuesReader(true);
        reader.init(57, encodes, 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readLong());
        }
    }


}
