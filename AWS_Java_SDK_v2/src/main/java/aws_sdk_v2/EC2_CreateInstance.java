package aws_sdk_v2;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.InstanceNetworkInterfaceSpecification;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class EC2_CreateInstance {

    static List<Config> US_EAST_1 = new ArrayList<>(); // N Virginia
    static List<Config> US_EAST_2 = new ArrayList<>(); // Ohio
    static List<Config> US_WEST_1 = new ArrayList<>(); // California
    static List<Config> US_WEST_2 = new ArrayList<>(); // Oregon
    static List<Config> EU_WEST_1 = new ArrayList<>(); // Ireland
    static List<Config> EU_WEST_2 = new ArrayList<>(); // London
    static List<Config> AP_NORTHEAST_1 = new ArrayList<>(); // Tokyo
    static List<Config> EU_NORTH_1 = new ArrayList<>(); // Stockholm

    static {

        for (String[] c : new String[][]{
                {"Pr1_01_NEW", "subnet-b3..."},
                {"Pr1_02_NEW", "subnet-0f..."},
                {"Pr1_03_NEW", "subnet-88..."},
                {"Pr1_04_NEW", "subnet-54..."},
                {"Pr1_05_NEW", "subnet-0e..."},

                {"Pr1_06_NEW", "subnet-b3..."},
                {"Pr1_07_NEW", "subnet-0f..."},
                {"Pr1_08_NEW", "subnet-88..."},
                {"Pr1_09_NEW", "subnet-54..."},
                {"Pr1_10_NEW", "subnet-0e..."},

                {"Pr1_11_NEW", "subnet-b3..."},
                {"Pr1_12_NEW", "subnet-0f..."},
                {"Pr1_13_NEW", "subnet-88..."},
                {"Pr1_14_NEW", "subnet-54..."},
        }) {
            Config c1 = new Config();
            c1.region = Region.US_EAST_1;
            c1.amiId = "ami-06...";
            c1.instanceType = InstanceType.T4_G_NANO;
            c1.keyName = "keyName...";
            c1.network = InstanceNetworkInterfaceSpecification.builder()
                    .subnetId(c[1])
                    .associatePublicIpAddress(true)
                    .deviceIndex(0)
                    .groups("sg-d7...")
                    .build();
            c1.tags = new Tag[]{
                    Tag.builder().key("Name").value(c[0]).build(),
                    Tag.builder().key("Project").value("event-data").build()
            };
            US_EAST_1.add(c1);
        }

        for (String[] c : new String[][]{
                {"Pr1_01_NEW", "subnet-80..."},
                {"Pr1_02_NEW", "subnet-71..."},
                {"Pr1_03_NEW", "subnet-80..."},
                {"Pr1_04_NEW", "subnet-71..."},
                {"Pr1_05_NEW", "subnet-80..."},

                {"Pr1_06_NEW", "subnet-71..."},
                {"Pr1_07_NEW", "subnet-80..."},
                {"Pr1_08_NEW", "subnet-71..."},
                {"Pr1_09_NEW", "subnet-80..."},
                {"Pr1_10_NEW", "subnet-71..."},

                {"Pr10_01_NEW", "subnet-80..."},
                {"Pr10_02_NEW", "subnet-71..."},
                {"Pr10_03_NEW", "subnet-80..."},
                {"Pr10_04_NEW", "subnet-71..."},
                {"Pr10_05_NEW", "subnet-80..."},

                {"Pr10_06_NEW", "subnet-71..."},
                {"Pr10_07_NEW", "subnet-80..."},
                {"Pr10_08_NEW", "subnet-71..."},
                {"Pr10_09_NEW", "subnet-80..."},
                {"Pr10_10_NEW", "subnet-71..."},
        }) {
            Config c1 = new Config();
            c1.region = Region.US_EAST_2;
            c1.amiId = "ami-05...";
            c1.instanceType = InstanceType.T4_G_NANO;
            c1.keyName = "keyName...";
            c1.network = InstanceNetworkInterfaceSpecification.builder()
                    .subnetId(c[1])
                    .associatePublicIpAddress(true)
                    .deviceIndex(0)
                    .groups("sg-cd...")
                    .build();
            c1.tags = new Tag[]{
                    Tag.builder().key("Name").value(c[0]).build(),
                    Tag.builder().key("Project").value("event-data").build()
            };
            US_EAST_2.add(c1);
        }

        for (String[] c : new String[][]{
                {"Pr2_01_NEW", "subnet-e7..."},
                {"Pr2_02_NEW", "subnet-67..."},
                {"Pr2_03_NEW", "subnet-e7..."},
                {"Pr2_04_NEW", "subnet-67..."},
                {"Pr2_05_NEW", "subnet-e7..."}
        }) {
            Config c1 = new Config();
            c1.region = Region.US_WEST_1;
            c1.amiId = "ami-0c...";
            c1.instanceType = InstanceType.T4_G_NANO;
            c1.keyName = "keyName...";
            c1.network = InstanceNetworkInterfaceSpecification.builder()
                    .subnetId(c[1])
                    .associatePublicIpAddress(true)
                    .deviceIndex(0)
                    .groups("sg-09...")
                    .build();
            c1.tags = new Tag[]{
                    Tag.builder().key("Name").value(c[0]).build(),
                    Tag.builder().key("Project").value("event-data").build()
            };
            US_WEST_1.add(c1);
        }

        for (String[] c : new String[][]{
                {"Pr2_01_NEW", "subnet-84..."},
                {"Pr2_02_NEW", "subnet-0f..."},
                {"Pr2_03_NEW", "subnet-84..."},
                {"Pr2_04_NEW", "subnet-0d..."},
                {"Pr2_05_NEW", "subnet-0f..."}
        }) {
            Config c1 = new Config();
            c1.region = Region.US_WEST_2;
            c1.amiId = "ami-06...";
            c1.instanceType = InstanceType.T4_G_NANO;
            c1.keyName = "keyName...";
            c1.network = InstanceNetworkInterfaceSpecification.builder()
                    .subnetId(c[1])
                    .associatePublicIpAddress(true)
                    .deviceIndex(0)
                    .groups("sg-2e...")
                    .build();
            c1.tags = new Tag[]{
                    Tag.builder().key("Name").value(c[0]).build(),
                    Tag.builder().key("Project").value("event-data").build()
            };
            US_WEST_2.add(c1);
        }

        for (String[] c : new String[][]{
                {"Pr4_1_NEW", "subnet-0d..."},
                {"Pr4_2_NEW", "subnet-58..."},
                {"Pr4_3_NEW", "subnet-71..."},
                {"Pr4_4_NEW", "subnet-0d..."},
                {"Pr4_5_NEW", "subnet-58..."},
                {"Pr4_6_NEW", "subnet-71..."}
        }) {
            Config c1 = new Config();
            c1.region = Region.EU_WEST_1;
            c1.amiId = "ami-0b...";
            c1.instanceType = InstanceType.T4_G_NANO;
            c1.keyName = "keyName...";
            c1.network = InstanceNetworkInterfaceSpecification.builder()
                    .subnetId(c[1])
                    .associatePublicIpAddress(true)
                    .deviceIndex(0)
                    .groups("sg-02...")
                    .build();
            c1.tags = new Tag[]{
                    Tag.builder().key("Name").value(c[0]).build(),
                    Tag.builder().key("Project").value("event-data").build()
            };
            EU_WEST_1.add(c1);
        }

        for (String[] c : new String[][]{
                {"Pr4_1_NEW", "subnet-50..."},
                {"Pr4_2_NEW", "subnet-96..."},
                {"Pr4_3_NEW", "subnet-50..."},
                {"Pr4_4_NEW", "subnet-96..."}
        }) {
            Config c1 = new Config();
            c1.region = Region.EU_WEST_2;
            c1.amiId = "ami-09...";
            c1.instanceType = InstanceType.T4_G_NANO;
            c1.keyName = "keyName...";
            c1.network = InstanceNetworkInterfaceSpecification.builder()
                    .subnetId(c[1])
                    .associatePublicIpAddress(true)
                    .deviceIndex(0)
                    .groups("sg-5a...")
                    .build();
            c1.tags = new Tag[]{
                    Tag.builder().key("Name").value(c[0]).build(),
                    Tag.builder().key("Project").value("event-data").build()
            };
            EU_WEST_2.add(c1);
        }

        for (String[] c : new String[][]{
                {"Pr1_1_NEW", "subnet-09..."},
                {"Pr1_2_NEW", "subnet-70..."},
                {"Pr1_3_NEW", "subnet-09..."},
                {"Pr1_4_NEW", "subnet-70..."},

                {"Pr5_1_NEW", "subnet-09..."},
                {"Pr5_2_NEW", "subnet-70..."},
                {"Pr5_3_NEW", "subnet-09..."},
                {"Pr5_4_NEW", "subnet-70..."}
        }) {
            Config c1 = new Config();
            c1.region = Region.AP_NORTHEAST_1;
            c1.amiId = "ami-04...";
            c1.instanceType = InstanceType.T4_G_NANO;
            c1.keyName = "keyName...";
            c1.network = InstanceNetworkInterfaceSpecification.builder()
                    .subnetId(c[1])
                    .associatePublicIpAddress(true)
                    .deviceIndex(0)
                    .groups("sg-01...")
                    .build();
            c1.tags = new Tag[]{
                    Tag.builder().key("Name").value(c[0]).build(),
                    Tag.builder().key("Project").value("event-data").build()
            };
            AP_NORTHEAST_1.add(c1);
        }

        for (String[] c : new String[][]{
                {"Pr1_1_NEW", "subnet-24..."},
                {"Pr1_2_NEW", "subnet-5e..."},
                {"Pr1_3_NEW", "subnet-6a..."},
                {"Pr1_4_NEW", "subnet-24..."},
                {"Pr1_5_NEW", "subnet-5e..."},
                {"Pr1_6_NEW", "subnet-6a..."}
        }) {
            Config c1 = new Config();
            c1.region = Region.EU_NORTH_1;
            c1.amiId = "ami-0c...";
            c1.instanceType = InstanceType.T4_G_NANO;
            c1.keyName = "keyName...";
            c1.network = InstanceNetworkInterfaceSpecification.builder()
                    .subnetId(c[1])
                    .associatePublicIpAddress(true)
                    .deviceIndex(0)
                    .groups("sg-0e...")
                    .build();
            c1.tags = new Tag[]{
                    Tag.builder().key("Name").value(c[0]).build(),
                    Tag.builder().key("Project").value("event-data").build()
            };
            EU_NORTH_1.add(c1);
        }


    }

    public static void main(String[] args) {
        String awsAccessKey = "ASIA5C7FLJ...";
        String awsSecretKey = "An69VGdqFR...";
        String sessionToken = "IQoJb3JpZ2...";
        AwsSessionCredentials credentials =
                AwsSessionCredentials.builder()
                        .accessKeyId(awsAccessKey)
                        .secretAccessKey(awsSecretKey)
                        .sessionToken(sessionToken)
                        .build();

        AwsCredentialsProvider awsCreds = StaticCredentialsProvider.create(credentials);

        Stream.of(
                        US_EAST_1,
                        US_EAST_2,
                        US_WEST_1,
                        US_WEST_2,
                        EU_WEST_1,
                        EU_WEST_2,
                        AP_NORTHEAST_1,
                        EU_NORTH_1)
                .flatMap(s -> s.stream())
                .forEach(c -> {
                    Ec2Client ec2 = Ec2Client.builder()
                            .credentialsProvider(awsCreds)
                            .region(c.region)
                            .build();

                    String instanceId = createEC2Instance(ec2, c);
                    System.out.println("The Amazon EC2 Instance ID is " + instanceId);
                    ec2.close();
                });
    }

    static String createEC2Instance(Ec2Client ec2, Config c) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(c.amiId)
                .instanceType(c.instanceType)
                .maxCount(1)
                .minCount(1)
                .keyName(c.keyName)
                .ebsOptimized(true)
                .networkInterfaces(c.network)
                .build();

        // Use a waiter to wait until the instance is running.
        System.out.println("Going to start an EC2 instance using a waiter");
        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceIdVal = response.instances().get(0).instanceId();
        ec2.waiter().waitUntilInstanceRunning(r -> r.instanceIds(instanceIdVal));


        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceIdVal)
                .tags(c.tags)
                .build();
        try {
            ec2.createTags(tagRequest);
            System.out.printf("Successfully started EC2 Instance %s based on AMI %s", instanceIdVal, c.amiId);
            return instanceIdVal;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    static class Config {
        Region region;
        String amiId;
        InstanceType instanceType;
        String keyName;
        InstanceNetworkInterfaceSpecification network;
        Tag[] tags;
    }

}
