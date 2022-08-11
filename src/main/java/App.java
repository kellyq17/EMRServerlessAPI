import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.AWSEMRServerlessClientBuilder;
import com.amazonaws.services.emrserverless.model.ConfigurationOverrides;
import com.amazonaws.services.emrserverless.model.JobDriver;
import com.amazonaws.services.emrserverless.model.MonitoringConfiguration;
import com.amazonaws.services.emrserverless.model.S3MonitoringConfiguration;
import com.amazonaws.services.emrserverless.model.SparkSubmit;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import com.amazonaws.services.emrserverless.model.ListApplicationsRequest;
import com.amazonaws.services.emrserverless.model.ListApplicationsResult;
import com.amazonaws.services.emrserverless.model.StartApplicationRequest;
import com.amazonaws.services.emrserverless.model.GetJobRunRequest;
import com.amazonaws.services.emrserverless.model.ValidationException;
import com.amazonaws.services.emrserverless.model.ResourceNotFoundException;
import com.amazonaws.services.emrserverless.model.InternalServerException;
import com.amazonaws.services.emrserverless.model.ConflictException;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.Role;

import java.io.IOException;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class App {
    public static void main(String[] rawArgs) {
        AWSEMRServerless emrServerless = AWSEMRServerlessClientBuilder.defaultClient();

        System.out.println("Built Serverless client");

        String appId = "00f2e2rlgvd8fj09";
        System.out.println("appId initialized");

        List<String> appArgs = new ArrayList<>();
        appArgs.add("{\"@type\":\"JobConfig\",\"applicationArguments\":[\"DimensionServiceQualifier=Base.Gamma.HT\",\"shouldUpdateDimSegPermission=false\",\"partitionCount=5\"],\"inputDatasets\":{\"certServiceDatasets\":[{\"certificateName\":\"advertiser-segment-permissions-decorated\",\"certificationDate\":\"2022-07-11T15:10:49Z\",\"dataPath\":null,\"dataType\":\"ADVERTISER_SEGMENT_PERMISSION\",\"manifestPath\":\"s3://traffic.obsidiantargeting.data.gamma/cn/advertiser-segment-permissions-decorated/2022/07/11/1657552094820/1657552247.manifest\",\"namespace\":\"traffic-targeting-gamma\",\"region\":\"cn\",\"timeWindowEnd\":\"2022-07-11T00:00:00Z\",\"timeWindowSize\":\"PT24H\"},{\"certificateName\":\"advertiser-supergroup-segment-permissions-decorated\",\"certificationDate\":\"2022-07-11T15:10:53Z\",\"dataPath\":null,\"dataType\":\"ADVERTISER_SEGMENT_PERMISSION\",\"manifestPath\":\"s3://traffic.obsidiantargeting.data.gamma/cn/advertiser-supergroup-segment-permissions-decorated/2022/07/11/1657552094852/1657552249.manifest\",\"namespace\":\"traffic-targeting-gamma\",\"region\":\"cn\",\"timeWindowEnd\":\"2022-07-11T00:00:00Z\",\"timeWindowSize\":\"PT24H\"}],\"certServiceStreamDatasets\":[],\"pubServiceDatasets\":[],\"scheduleDatasets\":[]},\"jobName\":\"segment-permission-aggregation-cn\",\"metadata\":{},\"outputDatasets\":{\"certServiceDatasets\":[{\"certificateName\":\"advertiser-segment-permissions-agg\",\"certificationDate\":null,\"dataPath\":\"s3://traffic.obsidiantargeting.data.gamma/cn/advertiser-segment-permissions-agg/2022/07/11/1660003949158\",\"dataStoreRoot\":\"traffic.obsidiantargeting.data.gamma\",\"dataType\":\"ADVERTISER_AGGREGATED_SEGMENT_PERMISSION\",\"manifestPath\":\"s3://traffic.obsidiantargeting.data.gamma/cn/advertiser-segment-permissions-agg/2022/07/11/manifest-1660003949158\",\"namespace\":\"traffic-targeting-gamma\",\"numOutputFiles\":100,\"outputPathUniqueId\":\"1660003949158\",\"region\":\"cn\",\"schemaReference\":\"\",\"timeWindowEnd\":\"2022-07-11T00:00:00Z\",\"timeWindowSize\":\"PT24H\"},{\"certificateName\":\"advertiser-segment-permissions-agg-error\",\"certificationDate\":null,\"dataPath\":\"s3://traffic.obsidiantargeting.data.gamma/cn/advertiser-segment-permissions-agg-error/2022/07/11/1660003949179\",\"dataStoreRoot\":\"traffic.obsidiantargeting.data.gamma\",\"dataType\":\"ADVERTISER_AGGREGATED_SEGMENT_PERMISSION_UPDATE_ERROR\",\"manifestPath\":\"s3://traffic.obsidiantargeting.data.gamma/cn/advertiser-segment-permissions-agg-error/2022/07/11/manifest-1660003949179\",\"namespace\":\"traffic-targeting-gamma\",\"numOutputFiles\":5000,\"outputPathUniqueId\":\"1660003949179\",\"region\":\"cn\",\"schemaReference\":\"\",\"timeWindowEnd\":\"2022-07-11T00:00:00Z\",\"timeWindowSize\":\"PT24H\"}],\"certServiceStreamDatasets\":[],\"pubServiceDatasets\":[],\"scheduleDatasets\":[]},\"timeWindowEnd\":\"2022-07-11T00:00:00Z\"}");

        String sparkArgsString = "--conf spark.executor.memory=10g --conf spark.executor.cores=4 --conf spark.dynamicAllocation.enabled=false --class com.amazon.demandanalytics.targeting.app.advsegpermissions.AdvertiserSegmentPermissionAggregationApp";

        SparkSubmit sparkSubmit = new SparkSubmit()
                .withEntryPoint("s3://emr-serverless-obsidiantargeting-output/input-jars/MADSTrafficTargetingEMR-1.0-standalone-kelqiang-v5.jar")
                .withSparkSubmitParameters(sparkArgsString);
        System.out.println("sparkSubmit created");
//
        if (appArgs.size() > 0) {
            sparkSubmit.withEntryPointArguments(appArgs);
        }

        AmazonIdentityManagement iamClient = AmazonIdentityManagementClientBuilder.defaultClient();
        System.out.println("iamClient created");

        GetRoleRequest roleRequest = new GetRoleRequest().withRoleName("EMRServerlessRuntimeRole");
        Role roleInfo = iamClient.getRole(roleRequest).getRole();
        System.out.println("got roleInfo");
        String serverlessRoleArn = roleInfo.getArn();
        System.out.println("got roleArn");
        StartJobRunRequest jobRunRequest = new StartJobRunRequest()
                .withApplicationId(appId)
                .withExecutionRoleArn(serverlessRoleArn)
                .withJobDriver(new JobDriver()
                        .withSparkSubmit(sparkSubmit))
                .withConfigurationOverrides(new ConfigurationOverrides()
                        .withMonitoringConfiguration(new MonitoringConfiguration()
                                .withS3MonitoringConfiguration(new S3MonitoringConfiguration()
                                        .withLogUri("s3://emr-serverless-obsidiantargeting-output/logs/"))
                        )
                );
        System.out.println("created jobRunRequest");
        StartJobRunResult jobResponse = new StartJobRunResult();
        try {
            jobResponse = emrServerless.startJobRun(jobRunRequest);
        } catch (Throwable e) {
            System.out.println("Failed to start job run");
        }
        System.out.println("jobRun started");
        GetJobRunRequest jobRunInfo = new GetJobRunRequest()
                .withApplicationId(appId)
                .withJobRunId(jobResponse.getJobRunId());
        String jobState = emrServerless.getJobRun(jobRunInfo).getJobRun().getState();
        while (!(jobState.equals("SUCCESS"))){
            if (jobState.equals("FAILED")) {
                System.out.println("Unable to run job");
            }
            jobState = emrServerless.getJobRun(jobRunInfo).getJobRun().getState();
        }

        System.out.println("jobRun finished");
    }
}

