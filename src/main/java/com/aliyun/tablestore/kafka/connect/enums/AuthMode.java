package com.aliyun.tablestore.kafka.connect.enums;

/**
 * @Author lihn
 * @Date 2021/12/17 11:37
 */
public enum AuthMode {
     STS,
     AKSK;

     public static AuthMode getType(String mode){
          for(AuthMode enums:AuthMode.values()){
               if(enums.toString().equalsIgnoreCase(mode)){
                    return enums;
               }
          }
          return null;
     }

}
