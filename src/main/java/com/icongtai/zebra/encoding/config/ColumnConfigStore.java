package com.icongtai.zebra.encoding.config;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.icongtai.zebra.encoding.exception.ZebraDecodingException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

/**
 */
public class ColumnConfigStore {

    private Map<String, ColumnConfig> configByName = Maps.newHashMap();

    private Map<Integer, ColumnConfig> configById = Maps.newHashMap();

    private static ColumnConfigStore store;

    private static Object monitor = new Object();

    private ColumnConfigStore() {
    }

    public void init() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream in = loader.getResourceAsStream("column_config.json");
        if (in == null) {
            in = this.getClass().getResourceAsStream("column_config.json");
        }

        if(in == null) {
            throw new ZebraDecodingException("not found resource column_config.json");
        }

        InputStreamReader input = new InputStreamReader(in, Charsets.UTF_8);
        int n = 0;
        int EOF = -1;
        char[] buffer = new char[512];
        StringBuilder sb = new StringBuilder();
        try {
            while (EOF != (n = input.read(buffer))) {
                sb.append(buffer, 0, n);
            }
        } catch (IOException e) {
        }
        List<ColumnConfig> columnConfigs = JSON.parseArray(sb.toString(), ColumnConfig.class);
        for (ColumnConfig columnConfig : columnConfigs) {
            configById.put(columnConfig.getId(), columnConfig);
            configByName.put(columnConfig.getName(), columnConfig);
        }
    }

    public ColumnConfig getById(int id) {
        return configById.get(id);
    }

    public ColumnConfig getByName(String name) {
        return configByName.get(name);
    }

    public static ColumnConfigStore getInstance() {
        if (store == null) {
            synchronized (monitor) {
                if (store == null) {
                    store = new ColumnConfigStore();
                    store.init();
                }
            }
        }
        return store;
    }

}
