package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.ValuesWriter;
import com.icongtai.zebra.encoding.bytes.BytesInput;
import com.icongtai.zebra.encoding.impl.boundedint.BoundedIntValuesFactory;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by robin on 13/10/16.
 */
public class BoundIntTest {

    @Test
    public void testBoundIntTest() throws IOException {
        ValuesWriter writer = BoundedIntValuesFactory.getBoundedWriter(57, 32);
        for(int i = 1; i < 58; i++) {
            writer.writeInteger(57);
        }

        BytesInput bytesInput = writer.getBytes();
        byte[] bytes = bytesInput.toByteArray();

        System.out.println(bytes.length);

        ValuesReader reader = BoundedIntValuesFactory.getBoundedReader(57);
        reader.init(57, bytes, 0);
        for(int i = 0; i < 57; i++) {
            System.out.println(reader.readInteger());
        }
    }
}
