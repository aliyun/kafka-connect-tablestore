package com.aliyun.tablestore.kafka.connect.enums;

/**
 * @Author lihn
 * @Date 2021/12/21 15:37
 */
public enum SearchTimeMode {

    KAFKA,
    LOCAL;
    public static SearchTimeMode getType(String mode){
        for(SearchTimeMode enums:SearchTimeMode.values()){
            if(enums.toString().equalsIgnoreCase(mode)){
                return enums;
            }
        }
        return null;
    }

}
