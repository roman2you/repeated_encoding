package com.icongtai.zebra.encoding.test;

import com.icongtai.zebra.encoding.format.ColumnType;
import com.icongtai.zebra.encoding.format.DictionaryPage;
import com.icongtai.zebra.encoding.format.EncodeType;
import com.icongtai.zebra.encoding.impl.dictionary.DictionaryValuesReader;
import com.icongtai.zebra.encoding.impl.dictionary.DictionaryValuesWriter;
import com.icongtai.zebra.encoding.impl.dictionary.PlainValuesDictionary;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Created by robin on 18/10/16.
 */
public class DictionaryTest  {

    @Test
    public void testDictionaryPage() throws IOException {

        DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter writer = new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(1024, EncodeType.plan_dictionary, EncodeType.plain);
        for(int i = 0 ; i < 500; i++) {
            writer.writeInteger(new Random().nextInt(10) + 1);
        }


        byte[] bytes = writer.getBytes().toByteArray();
        System.out.println(bytes.length);

        DictionaryPage page = writer.createDictionaryPage();

        DictionaryValuesReader reader = new DictionaryValuesReader(PlainValuesDictionary.initDictionary(ColumnType.UNSIGNED_INT, page));
        reader.init(500, bytes, 0);
        for(int i = 0; i < 500; i++) {
            System.out.println(reader.readInteger());
        }
    }
}
