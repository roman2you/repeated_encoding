package com.icongtai.zebra.encoding.config;

/**
 * Created by robin on 18/10/16.
 */
public enum Column {

    /**
     * 时间
     */
    timestamp,
    /**
     * 纬度
     */
    latitude,
    /**
     * 经度
     */
    longitude,
    /**
     * 速度
     */
    speed,
    /**
     * 方向
     */
    bearing,
    /**
     * 经纬度水平精度
     */
    horizontal_accuracy,
    /**
     * 海拔
     */
    altitude,
    /**
     * 海拔精度
     */
    vertical_accuracy,
    /**
     * 卫星数
     */
    satellite_count,
    /**
     * 是否为wifi
     */
    is_wifi,
    /**
     * gps标记列
     */
    tag;
}
