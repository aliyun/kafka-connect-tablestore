

package com.aliyun.tablestore.kafka.connect.utils;

public class Version {
    public static String getVersion() {
        try {
            return Version.class.getPackage().getImplementationVersion();
        } catch(Exception ex){
            return "1.0.0.0";
        }
    }
}
