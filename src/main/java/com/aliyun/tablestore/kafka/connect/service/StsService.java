package com.aliyun.tablestore.kafka.connect.service;

import com.aliyun.tablestore.kafka.connect.model.StsUserBo;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;

/**
 * @Author lihn
 * @Date 2021/10/13 10:11
 */
public class StsService {

    /**
     * we assume others by ownId
     */
    public static StsUserBo getAssumeRole(String ownId, String regionId, String stsEndpoint, String ak, String sk, String roleName) {

        StsUserBo stsUserBo = new StsUserBo();
        try {
            IClientProfile profile = DefaultProfile.getProfile(regionId, ak, sk);
            DefaultAcsClient client = new DefaultAcsClient(profile);

            AssumeRoleRequest request = new AssumeRoleRequest();

            request.setRoleSessionName("kafka-session-" + ownId);
            request.setMethod(MethodType.POST);
            request.setProtocol(ProtocolType.HTTPS);
            request.setEndpoint(stsEndpoint);

            request.setRoleArn(buildRoleArn(ownId, roleName));
            request.setActionName("AssumeRoleWithServiceIdentity");
            request.putQueryParameter("AssumeRoleFor", ownId);
            request.setDurationSeconds(12 * 60 * 60L);
//            request.setActionName("AssumeRole");
//            request.setDurationSeconds(30 * 60L);

            AssumeRoleResponse response = client.getAcsResponse(request);
            String userAk = response.getCredentials().getAccessKeyId();
            String userSk = response.getCredentials().getAccessKeySecret();
            String token = response.getCredentials().getSecurityToken();

            stsUserBo.setAk(userAk);
            stsUserBo.setSk(userSk);
            stsUserBo.setToken(token);
            stsUserBo.setOwnId(ownId);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return stsUserBo;
    }

    private static String buildRoleArn(String uid, String roleName) {
        return String.format("acs:ram::%s:role/%s", uid, roleName);
    }
}
