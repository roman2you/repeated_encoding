package com.icongtai.zebra.encoding.values;


import com.icongtai.zebra.encoding.ValuesReader;
import com.icongtai.zebra.encoding.bytes.Binary;
import com.icongtai.zebra.encoding.config.ColumnConfig;

/**
 *
 */
public interface ColumnReader {

    /**
     * @return the totalCount of values to be consumed
     */
    int getNextOffset();

    /**
     * @return the definition level for the current value
     */
    int getCurrentDefinitionLevel();


    /**
     * Skip the current value
     */
    void skip();

    /**
     * @return the current value
     */
    Integer getInteger();

    /**
     * @return the current value
     */
    boolean getBoolean();

    /**
     * @return the current value
     */
    Long getLong();

    /**
     * @return the current value
     */
    Binary getBinary();

    /**
     * @return the current value
     */
    Float getFloat();

    /**
     * @return the current value
     */
    Double getDouble();

    /**
     * @return Descriptor of the column.
     */
    ColumnConfig getColumnConfig();

    public ValuesReader getDataReader();

}
