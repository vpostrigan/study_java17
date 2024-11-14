package aws_sdk_v2;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceState;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;

import java.util.List;
import java.util.stream.Collectors;

/**
 * https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/javav2/example_code/ec2/src/main/java/com/example/ec2/DescribeInstances.java
 * https://docs.aws.amazon.com/code-library/latest/ug/ec2_example_ec2_StartInstances_section.html
 */
public class EC2 {

    public static void main(String[] args) throws InterruptedException {
        AwsCredentialsProvider awsCreds = StaticCredentialsProvider.create(
                AwsBasicCredentials.create("AKIA5C...", "OaC5mfOrn..."));

        Ec2Client ec2 = Ec2Client.builder()
                .credentialsProvider(awsCreds)
                .region(Region.US_EAST_2)
                .build();

        log(ec2, "i-01ee554e440b...");

        startInstance(ec2, "i-01ee554e440b...");
        Thread.sleep(15000);
        log(ec2, "i-01ee554e440b3b096");

        stopInstance(ec2, "i-01ee554e440b...");
        Thread.sleep(15000);
        log(ec2, "i-01ee554e440b...");
    }

    public static void log(Ec2Client ec2, String instanceId) {
        for (Instance instance : describeEC2Instances(ec2, instanceId)) {
            InstanceState state = instance.state();
            System.out.println("\nInstance state " + state);
            System.out.println("Instance Id is " + instance.instanceId());
            System.out.println("Image id is " + instance.imageId());
            System.out.println("Instance type is " + instance.instanceType());
            System.out.println("Instance state name is " + instance.state().name());
            System.out.println("monitoring information is " + instance.monitoring().state());

            System.out.println("privateIpAddress is " + instance.privateIpAddress());
            System.out.println("publicIpAddress is " + instance.publicIpAddress());
        }
    }

    public static List<Instance> describeEC2Instances(Ec2Client ec2, String... instanceIds) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .instanceIds(instanceIds)
                .build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        return response.reservations().stream()
                .flatMap(reservation -> reservation.instances().stream())
                .collect(Collectors.toList());
    }

    public static void startInstance(Ec2Client ec2, String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(request);
        System.out.printf("Successfully started instance %s\n", instanceId);
    }

    public static void stopInstance(Ec2Client ec2, String instanceId) {
        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.stopInstances(request);
        System.out.printf("Successfully stopped instance %s\n", instanceId);
    }

}
