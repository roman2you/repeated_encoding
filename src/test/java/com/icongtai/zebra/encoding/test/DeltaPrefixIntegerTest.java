package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.impl.deltaprefixnumber.DeltaPrefixIntegerReader;
import com.icongtai.zebra.encoding.impl.deltaprefixnumber.DeltaPrefixIntegerWriter;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by robin on 16/10/16.
 */
public class DeltaPrefixIntegerTest {

    @Test
    public void testInt() throws IOException {
        DeltaPrefixIntegerWriter writer = new DeltaPrefixIntegerWriter(32000, 28000, 5, 128);
        writer.writeInteger(28123456);
        for(int i = 1; i < 901;i++) {
            if(i > 600) {
                writer.writeInteger(31000000 + (i * 300));
            } else if(i > 300) {
                writer.writeInteger(30000000 + (i * 300));
            } else {
                writer.writeInteger(29000000 + (i * 300));
            }

        }
        writer.writeInteger(31123456);

        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);

        DeltaPrefixIntegerReader reader = new DeltaPrefixIntegerReader(false, 5);
        reader.init(902, bytes, 0);

        for(int i = 0; i < 902; i++) {
            System.out.println(reader.readInteger());
        }

    }
}
