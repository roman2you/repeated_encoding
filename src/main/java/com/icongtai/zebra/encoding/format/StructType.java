package com.icongtai.zebra.encoding.format;

/**
 * Created by robin on 18/10/16.
 */
public enum  StructType {
    /**
     * 可以包含NULL列,这样会多DefinitionLevel block
     */
    optional,
    /**
     * 值不能为NULL
     */
    required
}
