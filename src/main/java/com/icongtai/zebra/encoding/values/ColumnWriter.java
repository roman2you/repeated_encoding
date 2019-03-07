package com.icongtai.zebra.encoding.values;

import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.bytes.BytesInput;

/**
 * Created by robin on 17/10/16.
 */
public interface ColumnWriter {

    /**
     * writes the current value
     *
     * @param value
     *
     */
    void write(int value);

    /**
     * writes the current value
     *
     * @param value
     *
     */
    void write(long value);

    /**
     * writes the current value
     *
     * @param value
     *
     */
    void write(boolean value);

    /**
     * writes the current value
     *
     * @param value
     *
     */
    void write(Binary value);

    /**
     * writes the current value
     *
     * @param value
     *
     */
    void write(float value);

    /**
     * writes the current value
     *
     * @param value
     */
    void write(double value);

    /**
     * writes the current null value
     *
     * @param definitionLevel
     *
     */
    void writeNull(int definitionLevel);

    /**
     * get Result
     * @return
     */
    BytesInput getBytes();

}
