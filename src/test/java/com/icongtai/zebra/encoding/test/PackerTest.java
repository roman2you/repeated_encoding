package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.bitpacking.BytePacker;
import com.icongtai.zebra.encoding.bitpacking.Packer;
import com.icongtai.zebra.encoding.bytes.BytesUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Created by robin on 12/10/16.
 */
public class PackerTest {

    @Test
    public void testBytePacker() {
        BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(3);
        BytePacker packer2 = Packer.BIG_ENDIAN.newBytePacker(3);
        int[] input = new int[]{0,1,2,3,4,5,6,7};
        byte[] output = new byte[3];
        byte[] output2 = new byte[3];
        packer.pack8Values(input, 0, output, 0);
        packer2.pack8Values(input, 0, output2, 0);
        System.out.println(output[0]);
        System.out.println(output[1]);
        System.out.println(output[2]);
        System.out.println(output2[0]);
        System.out.println(output2[1]);
        System.out.println(output2[2]);

        int[] input1 = new int[8];
        int[] input2 = new int[8];
        packer.unpack8Values(output, 0, input1, 0 );
        packer2.unpack8Values(output2, 0, input2, 0);
        System.out.print(input2[0]);
        System.out.print(input2[1]);
        System.out.print(input2[2]);
        System.out.print(input2[3]);
        System.out.print(input2[4]);
        System.out.print(input2[5]);
        System.out.print(input2[6]);
        System.out.print(input2[7]);
        System.out.print(input1[7]);
    }

    @Test
    public void testVarInt() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(5);
        BytesUtils.writeUnsignedVarInt(Integer.MAX_VALUE, stream);
        System.out.println(stream.toByteArray()[0]);
        System.out.println(stream.toByteArray()[1]);
        System.out.println(stream.toByteArray()[2]);
        System.out.println(stream.toByteArray()[3]);
        System.out.println(stream.toByteArray()[4]);

        ByteArrayInputStream bs = new ByteArrayInputStream(stream.toByteArray());
        int val = BytesUtils.readUnsignedVarInt(bs);
        System.out.println(val);
    }

    @Test
    public void testVarLong() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(5);
        BytesUtils.writeUnsignedVarLong(Integer.MAX_VALUE + 100L, stream);
        System.out.println(stream.toByteArray()[0]);
        System.out.println(stream.toByteArray()[1]);
        System.out.println(stream.toByteArray()[2]);
        System.out.println(stream.toByteArray()[3]);
        System.out.println(stream.toByteArray()[4]);

        ByteArrayInputStream bs = new ByteArrayInputStream(stream.toByteArray());
        long val = BytesUtils.readUnsignedVarLong(bs);
        System.out.println(val);
    }

}
