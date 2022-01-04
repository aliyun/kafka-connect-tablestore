package com.aliyun.tablestore.kafka.connect.enums;

/**
 * @Author lihn
 * @Date 2021/12/2 19:05
 */
public enum TablestoreMode {
    NORMAL,
    TIMESERIES;


    public static TablestoreMode getType(String mode){
        for(TablestoreMode enums:TablestoreMode.values()){
            if(enums.toString().equalsIgnoreCase(mode)){
                return enums;
            }
        }
        return null;
    }

}
