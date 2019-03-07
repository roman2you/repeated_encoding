package com.icongtai.zebra.encoding.format;

import com.icongtai.zebra.encoding.bytes.BytesInput;

import java.io.IOException;

/**
 * Created by robin on 11/10/16.
 */
public class DictionaryPage {
    private EncodeType encoding;
    private BytesInput bytes;
    private int dictionarySize;

    /**
     * @param bytes the content of the page
     * @param dictionarySize the value count in the dictionary
     * @param encoding the encoding used
     */
    public DictionaryPage(BytesInput bytes, int dictionarySize, EncodeType encoding) {
        this.encoding = encoding;
        this.bytes = bytes;
        this.dictionarySize = dictionarySize;
    }


    public BytesInput getBytes() {
        return bytes;
    }

    public int getDictionarySize() {
        return dictionarySize;
    }

    public EncodeType getEncoding() {
        return encoding;
    }

    public DictionaryPage copy() throws IOException {
        return new DictionaryPage(BytesInput.copy(bytes), dictionarySize, encoding);
    }


    @Override
    public String toString() {
        return "Page [bytes.size=" + bytes.size() + ", entryCount=" + dictionarySize  + ", encoding=" + encoding + "]";
    }

}
