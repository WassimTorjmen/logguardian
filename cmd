Microsoft Windows [version 10.0.26200.8246]
(c) Microsoft Corporation. Tous droits réservés.

C:\Users\torjm>kubectl get nodes
No resources found

C:\Users\torjm>aws sts get-caller-identity
{
    "UserId": "AIDASFIXCPWSI4DFR7I5D",
    "Account": "148761640356",
    "Arn": "arn:aws:iam::148761640356:user/Wassim"
}


C:\Users\torjm>kubectl get nodes
No resources found

C:\Users\torjm>kubectl get nodes
No resources found

C:\Users\torjm>aws eks describe-cluster \

aws: [ERROR]: An error occurred (ParamValidation): the following arguments are required: --name

usage: aws [options] <command> <subcommand> [<subcommand> ...] [parameters]
To see help text, you can run:

  aws help
  aws <command> help
  aws <command> <subcommand> help


C:\Users\torjm>  --name logguardian \
'--name' n’est pas reconnu en tant que commande interne
ou externe, un programme exécutable ou un fichier de commandes.

C:\Users\torjm>  --region <ta-region> \
Le fichier spécifié est introuvable.

C:\Users\torjm>  --query 'cluster.status'
'--query' n’est pas reconnu en tant que commande interne
ou externe, un programme exécutable ou un fichier de commandes.

C:\Users\torjm>aws eks describe-cluster \

aws: [ERROR]: An error occurred (ParamValidation): the following arguments are required: --name

usage: aws [options] <command> <subcommand> [<subcommand> ...] [parameters]
To see help text, you can run:

  aws help
  aws <command> help
  aws <command> <subcommand> help


C:\Users\torjm>aws eks describe-cluster --name logguardian-nodes-lt1 --region eu-west-1 --query 'cluster.status'

aws: [ERROR]: An error occurred (ResourceNotFoundException) when calling the DescribeCluster operation: No cluster found for name: logguardian-nodes-lt1.

Additional error details:
clusterName: logguardian-nodes-lt1

C:\Users\torjm>aws eks describe-cluster --name logguardian --region eu-west-1 --query 'cluster.status'
"cluster.status"


C:\Users\torjm>aws eks describe-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes-lt1 --region eu-west-1
{
    "nodegroup": {
        "nodegroupName": "logguardian-nodes-lt1",
        "nodegroupArn": "arn:aws:eks:eu-west-1:148761640356:nodegroup/logguardian/logguardian-nodes-lt1/52cedb5f-5212-2571-fe76-5843059490f1",
        "clusterName": "logguardian",
        "version": "1.35",
        "releaseVersion": "1.35.3-20260415",
        "createdAt": "2026-04-22T21:50:41.780000+02:00",
        "modifiedAt": "2026-04-22T22:23:54.192000+02:00",
        "status": "CREATE_FAILED",
        "capacityType": "ON_DEMAND",
        "scalingConfig": {
            "minSize": 1,
            "maxSize": 3,
            "desiredSize": 2
        },
        "instanceTypes": [
            "t3.medium"
        ],
        "subnets": [
            "subnet-044076265d5c4e5a7",
            "subnet-0a27aa6f1017701ad"
        ],
        "amiType": "AL2023_x86_64_STANDARD",
        "nodeRole": "arn:aws:iam::148761640356:role/logguardian-eks-node-role",
        "labels": {},
        "resources": {
            "autoScalingGroups": [
                {
                    "name": "eks-logguardian-nodes-lt1-52cedb5f-5212-2571-fe76-5843059490f1"
                }
            ]
        },
        "diskSize": 20,
        "health": {
            "issues": [
                {
                    "code": "NodeCreationFailure",
                    "message": "Instances failed to join the kubernetes cluster",
                    "resourceIds": [
                        "i-060ddd61d217d9125",
                        "i-0b7e9973418f8ab03"
                    ]
                }
            ]
        },
        "updateConfig": {
            "maxUnavailable": 1,
            "updateStrategy": "DEFAULT"
        },
        "nodeRepairConfig": {
            "enabled": false
        },
        "tags": {}
    }
}


C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>aws eks describe-cluster --name logguardian --region eu-west-1   --query 'cluster.resourcesVpcConfig.{public:endpointPublicAccess,private:endpointPrivateAccess,subnets:subnetIds,clusterSG:clusterSecurityGroupId}'
"cluster.resourcesVpcConfig.{public:endpointPublicAccess,private:endpointPrivateAccess,subnets:subnetIds,clusterSG:clusterSecurityGroupId}"


C:\Users\torjm>aws eks describe-cluster --name logguardian --region eu-west-1   --query 'cluster.resourcesVpcConfig.{public:endpointPublicAccess,private:endpointPrivateAccess,subnets:subnetIds,clusterSG:clusterSecurityGroupId}'
"cluster.resourcesVpcConfig.{public:endpointPublicAccess,private:endpointPrivateAccess,subnets:subnetIds,clusterSG:clusterSecurityGroupId}"


C:\Users\torjm>aws eks describe-cluster --name logguardian --region eu-west-1 --query "cluster.resourcesVpcConfig.{public:endpointPublicAccess,private:endpointPrivateAccess,subnets:subnetIds,clusterSG:clusterSecurityGroupId}"
{
    "public": true,
    "private": true,
    "subnets": [
        "subnet-044076265d5c4e5a7",
        "subnet-0a27aa6f1017701ad",
        "subnet-0e33579915f8895b5",
        "subnet-047b19142f9c03f04"
    ],
    "clusterSG": "sg-0614294c663cc95e6"
}


C:\Users\torjm>aws eks describe-cluster --name logguardian --region eu-west-1 --query "cluster.resourcesVpcConfig.{public:endpointPublicAccess,private:endpointPrivateAccess,subnets:subnetIds,clusterSG:clusterSecurityGroupId}" --output json
{
    "public": true,
    "private": true,
    "subnets": [
        "subnet-044076265d5c4e5a7",
        "subnet-0a27aa6f1017701ad",
        "subnet-0e33579915f8895b5",
        "subnet-047b19142f9c03f04"
    ],
    "clusterSG": "sg-0614294c663cc95e6"
}


C:\Users\torjm>aws eks describe-cluster --name logguardian --region eu-west-1 --query "cluster.resourcesVpcConfig.vpcId" --output text
vpc-0c389465f6951eeaa


C:\Users\torjm>aws ec2 describe-vpc-attribute --vpc-id vpc-0c389465f6951eeaa --attribute enableDnsSupport
{
    "EnableDnsSupport": {
        "Value": true
    },
    "VpcId": "vpc-0c389465f6951eeaa"
}


C:\Users\torjm>aws ec2 describe-vpc-attribute --vpc-id vpc-0c389465f6951eeaa --attribute enableDnsHostnames
{
    "EnableDnsHostnames": {
        "Value": true
    },
    "VpcId": "vpc-0c389465f6951eeaa"
}


C:\Users\torjm>aws ec2 describe-route-tables --filters "Name=association.subnet-id,Values=subnet-044076265d5c4e5a7,subnet-0a27aa6f1017701ad" --query "RouteTables[].{RouteTableId:RouteTableId,Routes:Routes[*].{Dest:DestinationCidrBlock,NatGatewayId:NatGatewayId,GatewayId:GatewayId}}"
[
    {
        "RouteTableId": "rtb-04fa044b04979a4d1",
        "Routes": [
            {
                "Dest": "10.0.0.0/16",
                "NatGatewayId": null,
                "GatewayId": "local"
            },
            {
                "Dest": "0.0.0.0/0",
                "NatGatewayId": "nat-04ce5e5bd2c95d535",
                "GatewayId": null
            },
            {
                "Dest": null,
                "NatGatewayId": null,
                "GatewayId": "vpce-0b9293f1d365fe728"
            }
        ]
    },
    {
        "RouteTableId": "rtb-08882e11e2821ace8",
        "Routes": [
            {
                "Dest": "10.0.0.0/16",
                "NatGatewayId": null,
                "GatewayId": "local"
            },
            {
                "Dest": "0.0.0.0/0",
                "NatGatewayId": "nat-04ce5e5bd2c95d535",
                "GatewayId": null
            },
            {
                "Dest": null,
                "NatGatewayId": null,
                "GatewayId": "vpce-0b9293f1d365fe728"
            }
        ]
    }
]


C:\Users\torjm>
C:\Users\torjm>aws iam list-attached-role-policies --role-name logguardian-eks-node-role
{
    "AttachedPolicies": [
        {
            "PolicyName": "AmazonEKS_CNI_Policy",
            "PolicyArn": "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
        },
        {
            "PolicyName": "AmazonEC2ContainerRegistryReadOnly",
            "PolicyArn": "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
        },
        {
            "PolicyName": "AmazonEKSWorkerNodePolicy",
            "PolicyArn": "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
        }
    ]
}


C:\Users\torjm>aws eks update-cluster-config --name logguardian --region eu-west-1 --resources-vpc-config endpointPrivateAccess=true,endpointPublicAccess=true

aws: [ERROR]: An error occurred (InvalidParameterException) when calling the UpdateClusterConfig operation: Cluster is already at the desired configuration with endpointPrivateAccess: true , endpointPublicAccess: true, and Public Endpoint Restrictions: [0.0.0.0/0]

Additional error details:
clusterName: logguardian

C:\Users\torjm>aws ec2 describe-nat-gateways --nat-gateway-ids nat-04ce5e5bd2c95d535 --query "NatGateways[*].{State:State,SubnetId:SubnetId}" --output json
[
    {
        "State": "available",
        "SubnetId": "subnet-047b19142f9c03f04"
    }
]


C:\Users\torjm>aws eks describe-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes-lt1 --region eu-west-1 --query "nodegroup.launchTemplate" --output json
null


C:\Users\torjm>aws ec2 describe-instances --instance-ids i-060ddd61d217d9125 i-0b7e9973418f8ab03 --region eu-west-1 --query "Reservations[].Instances[].{Id:InstanceId,Subnet:SubnetId,SGs:SecurityGroups[*].GroupId,Profile:IamInstanceProfile.Arn,LT:LaunchTemplate}" --output json
[
    {
        "Id": "i-060ddd61d217d9125",
        "Subnet": "subnet-044076265d5c4e5a7",
        "SGs": [
            "sg-0614294c663cc95e6"
        ],
        "Profile": "arn:aws:iam::148761640356:instance-profile/eks-52cedb5f-5212-2571-fe76-5843059490f1",
        "LT": null
    },
    {
        "Id": "i-0b7e9973418f8ab03",
        "Subnet": "subnet-0a27aa6f1017701ad",
        "SGs": [
            "sg-0614294c663cc95e6"
        ],
        "Profile": "arn:aws:iam::148761640356:instance-profile/eks-52cedb5f-5212-2571-fe76-5843059490f1",
        "LT": null
    }
]


C:\Users\torjm>aws ec2 describe-security-groups --group-ids sg-0614294c663cc95e6 --region eu-west-1 --output json
{
    "SecurityGroups": [
        {
            "GroupId": "sg-0614294c663cc95e6",
            "IpPermissionsEgress": [
                {
                    "IpProtocol": "-1",
                    "UserIdGroupPairs": [
                        {
                            "Description": "Allows EFA traffic, which is not matched by CIDR rules.",
                            "UserId": "148761640356",
                            "GroupId": "sg-0614294c663cc95e6"
                        }
                    ],
                    "IpRanges": [
                        {
                            "CidrIp": "0.0.0.0/0"
                        }
                    ],
                    "Ipv6Ranges": [],
                    "PrefixListIds": []
                }
            ],
            "Tags": [
                {
                    "Key": "aws:eks:cluster-name",
                    "Value": "logguardian"
                },
                {
                    "Key": "kubernetes.io/cluster/logguardian",
                    "Value": "owned"
                },
                {
                    "Key": "Name",
                    "Value": "eks-cluster-sg-logguardian-145348821"
                }
            ],
            "VpcId": "vpc-0c389465f6951eeaa",
            "SecurityGroupArn": "arn:aws:ec2:eu-west-1:148761640356:security-group/sg-0614294c663cc95e6",
            "OwnerId": "148761640356",
            "GroupName": "eks-cluster-sg-logguardian-145348821",
            "Description": "EKS created security group applied to ENI that is attached to EKS Control Plane master nodes, as well as any managed workloads.",
            "IpPermissions": [
                {
                    "IpProtocol": "-1",
                    "UserIdGroupPairs": [
                        {
                            "Description": "Allows EFA traffic, which is not matched by CIDR rules.",
                            "UserId": "148761640356",
                            "GroupId": "sg-0614294c663cc95e6"
                        }
                    ],
                    "IpRanges": [],
                    "Ipv6Ranges": [],
                    "PrefixListIds": []
                }
            ]
        }
    ]
}


C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>aws iam get-role --role-name logguardian-eks-node-role --query "Role.AssumeRolePolicyDocument" --output json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}


C:\Users\torjm>aws ec2 describe-vpcs --vpc-ids vpc-0c389465f6951eeaa --region eu-west-1 --query "Vpcs[0].DhcpOptionsId" --output text
dopt-00e59acea36cf9631


C:\Users\torjm>aws ec2 describe-dhcp-options --dhcp-options-ids dopt-00e59acea36cf9631 --region eu-west-1 --output json
{
    "DhcpOptions": [
        {
            "OwnerId": "148761640356",
            "Tags": [],
            "DhcpOptionsId": "dopt-00e59acea36cf9631",
            "DhcpConfigurations": [
                {
                    "Key": "domain-name",
                    "Values": [
                        {
                            "Value": "eu-west-1.compute.internal"
                        }
                    ]
                },
                {
                    "Key": "domain-name-servers",
                    "Values": [
                        {
                            "Value": "AmazonProvidedDNS"
                        }
                    ]
                }
            ]
        }
    ]
}


C:\Users\torjm>aws ec2 get-console-output --instance-id i-060ddd61d217d9125 --region eu-west-1 --latest --output text
i-060ddd61d217d9125
  Booting `Amazon Linux (6.12.79-101.147.amzn2023.x86_64) 2023'

[    0.059591] RETBleed: WARNING: Spectre v2 mitigation leaves CPU vulnerable to RETBleed attacks, data leaks possible!
[    2.504264] systemd-journald[1197]: Received client request to flush runtime journal.
[    2.724519] RPC: Registered named UNIX socket transport module.
[    2.725269] RPC: Registered udp transport module.
[    2.725870] RPC: Registered tcp transport module.
[    2.726512] RPC: Registered tcp-with-tls transport module.
[    2.727245] RPC: Registered tcp NFSv4.1 backchannel transport module.
[    2.912673] input: Power Button as /devices/LNXSYSTM:00/LNXPWRBN:00/input/input0
[    2.919172] ena 0000:00:05.0: Elastic Network Adapter (ENA) v2.16.1g
[    2.936671] ena 0000:00:05.0: ENA device version: 0.10
[    2.937322] ena 0000:00:05.0: ENA controller version: 0.0.1 implementation version 1
[    2.959237] ACPI: button: Power Button [PWRF]
[    2.959895] input: Sleep Button as /devices/LNXSYSTM:00/LNXSLPBN:00/input/input1
[    2.968189] ACPI: button: Sleep Button [SLPF]
[    2.988224] i8042: PNP: PS/2 Controller [PNP0303:KBD,PNP0f13:MOU] at 0x60,0x64 irq 1,12
[    2.994820] i8042: Warning: Keylock active
[    2.997173] serio: i8042 KBD port at 0x60,0x64 irq 1
[    2.997704] serio: i8042 AUX port at 0x60,0x64 irq 12
[    3.020228] ena 0000:00:05.0: LLQ is not supported Fallback to host mode policy.
[    3.034247] ena 0000:00:05.0: Elastic Network Adapter (ENA) found at mem c0400000, mac addr 02:ca:87:6e:fe:91
[    3.053084] ena 0000:00:05.0 ens5: renamed from eth0
[    3.058569] nvme nvme0: using unchecked data buffer
[    3.214910] zram_generator::config[1806]: zram0: system has too much memory (3834MB), limit is 800MB, ignoring.
[    4.763503] nodeadm-internal[1862]: info boot-book/hook.go:58 Waiting for consistent network interfaces..
[    4.816927] nodeadm-internal[1862]: info system/networking.go:83 checking link states...
[    4.932882] nodeadm-internal[1862]: info system/networking.go:119 secondary link unmanaged {"linkName": "lo"}
[    4.934751] nodeadm-internal[1862]: info system/networking.go:112 link not yet configured {"linkName": "ens5", "linkState": "configuring"}
[    4.983151] nodeadm-internal[1862]: info system/networking.go:83 checking link states...
[    4.989731] nodeadm-internal[1862]: info system/networking.go:119 secondary link unmanaged {"linkName": "lo"}
[    4.991548] nodeadm-internal[1862]: info system/networking.go:109 link configured {"linkName": "ens5"}
[    4.993280] nodeadm-internal[1862]: info boot-book/hook.go:62 Completed boot hook!
[    5.241295] nodeadm[1879]: info init/init.go:55 Checking user is root..
[    5.242602] nodeadm[1879]: info init/init.go:65 Loading configuration.. {"configSource": ["imds://user-data"], "configCache": "/run/eks/nodeadm/config.json"}
[    5.244945] nodeadm[1879]: warn cli/config.go:25 failed to load cached config {"error": "open /run/eks/nodeadm/config.json: no such file or directory"}
[    5.258038] nodeadm[1879]: info init/init.go:75 Setting up nodeadm environment aspect...
[    5.259596] nodeadm[1879]: info init/init.go:84 Enriching configuration..
[    5.260877] nodeadm[1879]: info init/init.go:164 Fetching kubelet version..
[    5.262178] nodeadm[1879]: info kubelet/version.go:30 Reading kubelet version from file {"path": "/etc/eks/kubelet-version.txt"}
[    5.264155] nodeadm[1879]: info init/init.go:170 Fetched kubelet version {"version": "v1.35.3"}
[    5.265769] nodeadm[1879]: info init/init.go:171 Fetching instance details..
[    5.280383] nodeadm[1879]: SDK 2026/04/22 19:51:32 DEBUG attempting waiter request, attempt count: 1
[    5.499797] nodeadm[1879]: info init/init.go:201 Instance details populated {"details": {"id":"i-060ddd61d217d9125","region":"eu-west-1","type":"t3.medium","availabilityZone":"eu-west-1a","mac":"02:ca:87:6e:fe:91","privateDnsName":"ip-10-0-132-36.eu-west-1.compute.internal"}}
[    5.503993] nodeadm[1879]: info init/init.go:202 Fetching default options...
[    5.506274] nodeadm[1879]: info init/init.go:206 Default options populated {"defaults": {"sandboxImage":"localhost/kubernetes/pause"}}
[    5.508351] nodeadm[1879]: info init/init.go:89 Loaded configuration {"config": {"metadata":{},"spec":{"cluster":{"name":"logguardian","apiServerEndpoint":"https://1B3FC4C4EF0C07B7E44948A0E5644D55.gr7.eu-west-1.eks.amazonaws.com","certificateAuthority":"LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJWFN3YU9QQnJ1Ujh3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TmpBME1qQXlNVEl3TXpSYUZ3MHpOakEwTVRjeU1USTFNelJhTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUURsOHd5YzRuNjhZZmw2MXA3U1hsMGJ0L0ptRzRTa0dnbDBpTjNEQjE0QndWUnVKazRzaGFvTlJWOWwKeHZidjlsNGV1R3BCeXVJL1huODR5SDhUalhqa3FHTjBDb1MxVHp4MDBkbnJ3ZThxZWVDWUZ4Rngyc0FVdFhVSwpMSG9PNG1xUElYbzg4NFVraG45MisvSk5UdjZERTE3MXZMcnNYcUlGdGpiVElkdlhBK1FtL01lRUxuUWRNUHpFCngxYUpXNGRkdUhWN080QzZvVFVrdUR0d21TeFV2amI2Q0ZzbzdvbWZ4ZVVXMmxQRUlCRXpEM3l5aWdVbWk0UUYKak93NXcyVVlEY0YxUVlXMHoxQmpBalpubUlzaGlFZjgvOUZNUGR5cGxxaFI5ZjB2S21WUmEwVStiYVpLU05BQQpJeU1URj[2026-04-22T19:51:34.423871]hjUS9icVByUUF3S0FQdmVnRkRBNlFsQWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJUaml6ck5VT2R4KzZMYlNCQWJ1Ly9VbFRwUWh6QVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQzNGajJ5SzVXQwpuaUNMc0NDbG9SNUN4VXhmMG1KaVZDWWkrc3BpQnpmRGlzR0lHd2RKQjc1eHhKOHNJaVRpWGFpWHVwQjYxdG1FCkQ2OEtKSmkxeVdjeGpSNzY4Skc5cmx3a3k1YjN3bkphNHJvNlJDaXRXWC9aZkJTdWRKNGhsblk2K3dZTGY3SnQKa2hYaTNNK3R5d1FQT0JTUjAxcktOKzI0VVZ2ZDE5ZElLVUtHRG1kdDhGUFF5SFVHdTcwMXBmc0YxWTFjSTFaNApxVXlkVU16eDMzR1Rna2x1ZmFRNkc5L01ERnE2bHIxZHVOQXRIT2VLYWwzRTUraitWNURmRDhSYzU0YjcwVFlICnVHaU1ENTl5dXZ3eENDUCtNSHIzK2wzT2V0QmdVZ2xxWDZmWWtRNmZCQWg5bUNhVWE2WFE4UUNpcXB1d3MzcTcKZWpzRnNNUUF6TkVkCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K","cidr":"172.20.0.0/16"},"containerd":{},"instance":{"localStorage":{},"network":{}},"kubelet":{"config":{"clusterDNS":["172.20.0.10"],"maxPods":17},"flags":["--node-labels=eks.amazonaws.com/nodegroup-image=ami-052eb47703373f0fe,eks.amazonaws.com/capacityT[2026-04-22T19:51:34.523804]ype=ON_DEMAND,eks.amazonaws.com/nodegroup=logguardian-nodes-lt1"]}},"status":{"instance":{"id":"i-060ddd61d217d9125","region":"eu-west-1","type":"t3.medium","availabilityZone":"eu-west-1a","mac":"02:ca:87:6e:fe:91","privateDnsName":"ip-10-0-132-36.eu-west-1.compute.internal"},"default":{"sandboxImage":"localhost/kubernetes/pause"},"kubeletVersion":"v1.35.3"}}}
[    5.543072] nodeadm[1879]: info init/init.go:91 Validating configuration..
[    5.544472] nodeadm[1879]: info init/init.go:96 Creating daemon manager..
[    5.545768] nodeadm[1879]: info init/init.go:120 Setting up system config aspects...
[    5.547135] nodeadm[1879]: info init/init.go:252 Setting up system aspect.. {"name": "instance-environment"}
[    5.548801] nodeadm[1879]: info system/environment.go:75 No environment variables to configure
[    5.550605] nodeadm[1879]: info init/init.go:256 Set up system aspect {"name": "instance-environment"}
[    5.552199] nodeadm[1879]: info init/init.go:252 Setting up system aspect.. {"name": "resolve"}
[    5.553692] nodeadm[1879]: info init/init.go:256 Set up system aspect {"name": "resolve"}
[    5.555078] nodeadm[1879]: info init/init.go:129 Configuring daemons...
[    5.556221] nodeadm[1879]: info init/init.go:217 Configuring daemon... {"name": "containerd"}
[    5.557787] nodeadm[1879]: info containerd/base_runtime_spec.go:20 Writing containerd base runtime spec... {"path": "/etc/containerd/base-runtime-spec.json"}
[    5.560198] nodeadm[1879]: info containerd/version.go:33 Reading containerd version from file {"path": "/etc/eks/containerd-version.txt"}
[    5.562286] nodeadm[1879]: info containerd/config.go:77 Writing containerd config to file.. {"path": "/etc/containerd/config.toml"}
[    5.564239] nodeadm[1879]: info init/init.go:221 Configured daemon {"name": "containerd"}
[    5.565663] nodeadm[1879]: info init/init.go:217 Configuring daemon... {"name": "kubelet"}
[    5.567085] nodeadm[1879]: info kubelet/config.go:232 Setup IP for node {"ip": "10.0.132.36"}
[    5.568580] nodeadm[1879]: info kubelet/config.go:347 Writing kubelet config to file.. {"path": "/etc/kubernetes/kubelet/config.json"}
[    5.570679] nodeadm[1879]: info kubelet/config.go:372 Writing user kubelet config to drop-in file.. {"path": "/etc/kubernetes/kubelet/config.json.d/40-nodeadm.conf"}
[    5.573245] nodeadm[1879]: info init/init.go:221 Configured daemon {"name": "kubelet"}
[    5.574661] nodeadm[1879]: info init/init.go:156 done! {"duration": 0.336080266}
[    6.219692] cloud-init[1888]: Cloud-init v. 22.2.2 running 'init' at Wed, 22 Apr 2026 19:51:33 +0000. Up 6.19 seconds.
[    6.278122] cloud-init[1888]: ci-info: ++++++++++++++++++++++++++++++++++++++Net device info++++++++++++++++++++++++++++++++++++++
[    6.280268] cloud-init[1888]: ci-info: +--------+------+----------------------------+---------------+--------+-------------------+
[    6.282356] cloud-init[1888]: ci-info: | Device |  Up  |          Address           |      Mask     | Scope  |     Hw-Address    |
[    6.284343] cloud-init[1888]: ci-info: +--------+------+----------------------------+---------------+--------+-------------------+
[    6.286325] cloud-init[1888]: ci-info: |  ens5  | True |        10.0.132.36         | 255.255.240.0 | global | 02:ca:87:6e:fe:91 |
[    6.288307] cloud-init[1888]: ci-info: |  ens5  | True | fe80::ca:87ff:fe6e:fe91/64 |       .       |  link  | 02:ca:87:6e:fe:91 |
[    6.290309] cloud-init[1888]: ci-info: |   lo   | True |         127.0.0.1          |   255.0.0.0   |  host  |         .         |
[    6.292298] cloud-init[1888]: ci-info: |   lo   | True |          ::1/128           |       .       |  host  |         .         |
[    6.294307] cloud-init[1888]: ci-info: +--------+------+----------------------------+---------------+--------+-------------------+
[    6.296282] cloud-init[1888]: ci-info: +++++++++++++++++++++++++++++Route IPv4 info++++++++++++++++++++++++++++++
[    6.298010] cloud-init[1888]: ci-info: +-------+-------------+------------+-----------------+-----------+-------+
[    6.299717] cloud-init[1888]: ci-info: | Route | Destination |  Gateway   |     Genmask     | Interface | Flags |
[    6.301454] cloud-init[1888]: ci-info: +-------+-------------+------------+-----------------+-----------+-------+
[    6.303172] cloud-init[1888]: ci-info: |   0   |   0.0.0.0   | 10.0.128.1 |     0.0.0.0     |    ens5   |   UG  |
[    6.304906] cloud-init[1888]: ci-info: |   1   |   10.0.0.2  | 10.0.128.1 | 255.255.255.255 |    ens5   |  UGH  |
[    6.306669] cloud-init[1888]: ci-info: |   2   |  10.0.128.0 |  0.0.0.0   |  255.255.240.0  |    ens5   |   U   |
[    6.308379] cloud-init[1888]: ci-info: |   3   |  10.0.128.1 |  0.0.0.0   | 255.255.255.255 |    ens5   |   UH  |
[    6.310174] cloud-init[1888]: ci-info: +-------+-------------+------------+-----------------+-----------+-------+
[    6.311887] cloud-init[1888]: ci-info: +++++++++++++++++++Route IPv6 info+++++++++++++++++++
[    6.313325] cloud-init[1888]: ci-info: +-------+-------------+---------+-----------+-------+
[    6.314731] cloud-init[1888]: ci-info: | Route | Destination | Gateway | Interface | Flags |
[    6.316135] cloud-init[1888]: ci-info: +-------+-------------+---------+-----------+-------+
[    6.317575] cloud-init[1888]: ci-info: |   0   |  fe80::/64  |    ::   |    ens5   |   U   |
[    6.318974] cloud-init[1888]: ci-info: |   2   |    local    |    ::   |    ens5   |   U   |
[    6.320399] cloud-init[1888]: ci-info: |   3   |  multicast  |    ::   |    ens5   |   U   |
[    6.321862] cloud-init[1888]: ci-info: +-------+-------------+---------+-----------+-------+
[    6.648595] cloud-init[1888]: 2026-04-22 19:51:33,913 - __init__.py[WARNING]: Unhandled unknown content-type (application/node.eks.aws) userdata: 'b'---'...'
[    6.968557] cloud-init[1888]: Generating public/private ed25519 key pair.
[    6.970097] cloud-init[1888]: Your identification has been saved in /etc/ssh/ssh_host_ed25519_key
[    6.971713] cloud-init[1888]: Your public key has been saved in /etc/ssh/ssh_host_ed25519_key.pub
[    6.973255] cloud-init[1888]: The key fingerprint is:
[    6.974175] cloud-init[1888]: SHA256:7tSYYH8VogpQ7DvN8ynCK8TWOpreaTuzkEeacGH+l+4 root@ip-10-0-132-36.eu-west-1.compute.internal
[    6.976089] cloud-init[1888]: The key's randomart image is:
[    6.977056] cloud-init[1888]: +--[ED25519 256]--+
[    6.977925] cloud-init[1888]: |   ..            |
[    6.978779] cloud-init[1888]: |   ..            |
[    6.979628] cloud-init[1888]: |  +.      . .    |
[    6.980476] cloud-init[1888]: | o o.    . . .   |
[    6.981349] cloud-init[1888]: |..oo.+o S   .    |
[    6.982229] cloud-init[1888]: |..O.+o+* + .     |
[    6.983080] cloud-init[1888]: | B +..+o*.o      |
[    6.983929] cloud-init[1888]: | .B++o.oo.       |
[    6.984765] cloud-init[1888]: |+o.B*+E..        |
[    6.985620] cloud-init[1888]: +----[SHA256]-----+
[    6.986471] cloud-init[1888]: Generating public/private ecdsa key pair.
[    6.987625] cloud-init[1888]: Your identification has been saved in /etc/ssh/ssh_host_ecdsa_key
[    6.989100] cloud-init[1888]: Your public key has been saved in /etc/ssh/ssh_host_ecdsa_key.pub
[    6.990712] cloud-init[1888]: The key fingerprint is:
[    6.991660] cloud-init[1888]: SHA256:EtfOdoRrzIfXU3+sj8yTUcKsyJqyKyd4qASQFY9ZnmQ root@ip-10-0-132-36.eu-west-1.compute.internal
[    6.993598] cloud-init[1888]: The key's randomart image is:
[    6.994579] cloud-init[1888]: +---[ECDSA 256]---+
[    6.995431] cloud-init[1888]: |  o.E            |
[    6.996300] cloud-init[1888]: | o O .   . .     |
[    6.997197] cloud-init[1888]: |o o + . . o .o  .|
[    6.998056] cloud-init[1888]: |.      o = + .+oo|
[    6.998908] cloud-init[1888]: |.     . S.X.+.oo+|
[    6.999758] cloud-init[1888]: |.      . oo+. .o.|
[    7.000652] cloud-init[1888]: | . o     o    .o |
[    7.001560] cloud-init[1888]: |. o + o o    ooo |
[    7.002419] cloud-init[1888]: |.. . +o+      +..|
[    7.003350] cloud-init[1888]: +----[SHA256]-----+
2026/04/22 19:51:34Z: SSM Agent unable to acquire credentials: <error>no valid credentials could be retrieved for ec2 identity. Default Host Management Err: error calling RequestManagedInstanceRoleToken: AccessDeniedException: Systems Manager's instance management role is not configured for account: 148761640356
        status code: 400, request id: a0da9a01-9e1f-408f-ab71-69227710b261</error>
[    7.538191] cloud-init[1970]: Cloud-init v. 22.2.2 running 'modules:config' at Wed, 22 Apr 2026 19:51:34 +0000. Up 7.48 seconds.
[    7.954976] cloud-init[1975]: Cloud-init v. 22.2.2 running 'modules:final' at Wed, 22 Apr 2026 19:51:35 +0000. Up 7.90 seconds.
ci-info: no authorized SSH keys fingerprints found for user ec2-user.
<14>Apr 22 19:51:35 cloud-init: #############################################################
<14>Apr 22 19:51:35 cloud-init: -----BEGIN SSH HOST KEY FINGERPRINTS-----
<14>Apr 22 19:51:35 cloud-init: 256 SHA256:EtfOdoRrzIfXU3+sj8yTUcKsyJqyKyd4qASQFY9ZnmQ root@ip-10-0-132-36.eu-west-1.compute.internal (ECDSA)
<14>Apr 22 19:51:35 cloud-init: 256 SHA256:7tSYYH8VogpQ7DvN8ynCK8TWOpreaTuzkEeacGH+l+4 root@ip-10-0-132-36.eu-west-1.compute.internal (ED25519)
<14>Apr 22 19:51:35 cloud-init: -----END SSH HOST KEY FINGERPRINTS-----
<14>Apr 22 19:51:35 cloud-init: #############################################################
-----BEGIN SSH HOST KEY KEYS-----
ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEOqj28+oUE+KzOfLLU9ksMMQMQX4Xogyw7RakUjZBj6o2ndyPM+oZx42OdMQvwnGw/JtYx9e2+PG0Qyk8+zqlc= root@ip-10-0-132-36.eu-west-1.compute.internal
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBegbolJwkf1GRFhjRZxe2WW7dVsg+qy68eiAe1Dff4p root@ip-10-0-132-36.eu-west-1.compute.internal
-----END SSH HOST KEY KEYS-----
[    8.090254] cloud-init[1975]: Cloud-init v. 22.2.2 finished at Wed, 22 Apr 2026 19:51:35 +0000. Datasource DataSourceEc2.  Up 8.08 seconds
[    8.238309] nodeadm[1988]: info init/init.go:55 Checking user is root..
[    8.239916] nodeadm[1988]: info init/init.go:65 Loading configuration.. {"configSource": ["imds://user-data", "file:///etc/eks/nodeadm.d/"], "configCache": "/run/eks/nodeadm/config.json"}
[    8.246155] nodeadm[1988]: warn configprovider/chain.go:30 Encountered error in config provider {"error": "no config found in directory"}
[    8.248396] nodeadm[1988]: info init/init.go:75 Setting up nodeadm environment aspect...
[    8.249965] nodeadm[1988]: info init/init.go:89 Loaded configuration {"config": {"metadata":{},"spec":{"cluster":{"name":"logguardian","apiServerEndpoint":"https://1B3FC4C4EF0C07B7E44948A0E5644D55.gr7.eu-west-1.eks.amazonaws.com","certificateAuthority":"LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJWFN3YU9QQnJ1Ujh3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TmpBME1qQXlNVEl3TXpSYUZ3MHpOakEwTVRjeU1USTFNelJhTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUURsOHd5YzRuNjhZZmw2MXA3U1hsMGJ0L0ptRzRTa0dnbDBpTjNEQjE0QndWUnVKazRzaGFvTlJWOWwKeHZidjlsNGV1R3BCeXVJL1huODR5SDhUalhqa3FHTjBDb1MxVHp4MDBkbnJ3ZThxZWVDWUZ4Rngyc0FVdFhVSwpMSG9PNG1xUElYbzg4NFVraG45MisvSk5UdjZERTE3MXZMcnNYcUlGdGpiVElkdlhBK1FtL01lRUxuUWRNUHpFCngxYUpXNGRkdUhWN080QzZvVFVrdUR0d21TeFV2amI2Q0ZzbzdvbWZ4ZVVXMmxQRUlCRXpEM3l5aWdVbWk0UUYKak93NXcyVVlEY0YxUVlXMHoxQmpBalpubUlzaGlFZjgvOUZNUGR5cGxxaFI5ZjB2S21WUmEwVStiYVpLU05BQQpJeU1URj[2026-04-22T19:51:37.224063]hjUS9icVByUUF3S0FQdmVnRkRBNlFsQWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJUaml6ck5VT2R4KzZMYlNCQWJ1Ly9VbFRwUWh6QVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQzNGajJ5SzVXQwpuaUNMc0NDbG9SNUN4VXhmMG1KaVZDWWkrc3BpQnpmRGlzR0lHd2RKQjc1eHhKOHNJaVRpWGFpWHVwQjYxdG1FCkQ2OEtKSmkxeVdjeGpSNzY4Skc5cmx3a3k1YjN3bkphNHJvNlJDaXRXWC9aZkJTdWRKNGhsblk2K3dZTGY3SnQKa2hYaTNNK3R5d1FQT0JTUjAxcktOKzI0VVZ2ZDE5ZElLVUtHRG1kdDhGUFF5SFVHdTcwMXBmc0YxWTFjSTFaNApxVXlkVU16eDMzR1Rna2x1ZmFRNkc5L01ERnE2bHIxZHVOQXRIT2VLYWwzRTUraitWNURmRDhSYzU0YjcwVFlICnVHaU1ENTl5dXZ3eENDUCtNSHIzK2wzT2V0QmdVZ2xxWDZmWWtRNmZCQWg5bUNhVWE2WFE4UUNpcXB1d3MzcTcKZWpzRnNNUUF6TkVkCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K","cidr":"172.20.0.0/16"},"containerd":{},"instance":{"localStorage":{},"network":{}},"kubelet":{"config":{"clusterDNS":["172.20.0.10"],"maxPods":17},"flags":["--node-labels=eks.amazonaws.com/nodegroup-image=ami-052eb47703373f0fe,eks.amazonaws.com/capacityT[2026-04-22T19:51:37.224086]ype=ON_DEMAND,eks.amazonaws.com/nodegroup=logguardian-nodes-lt1"]}},"status":{"instance":{"id":"i-060ddd61d217d9125","region":"eu-west-1","type":"t3.medium","availabilityZone":"eu-west-1a","mac":"02:ca:87:6e:fe:91","privateDnsName":"ip-10-0-132-36.eu-west-1.compute.internal"},"default":{"sandboxImage":"localhost/kubernetes/pause"},"kubeletVersion":"v1.35.3"}}}
[    8.282444] nodeadm[1988]: info init/init.go:91 Validating configuration..
[    8.283968] nodeadm[1988]: info init/init.go:96 Creating daemon manager..
[    8.285288] nodeadm[1988]: info init/init.go:141 Setting up system run aspects...
[    8.286567] nodeadm[1988]: info init/init.go:252 Setting up system aspect.. {"name": "marker"}
[    8.288001] nodeadm[1988]: info init/init.go:256 Set up system aspect {"name": "marker"}
[    8.289308] nodeadm[1988]: info init/init.go:252 Setting up system aspect.. {"name": "local-disk"}
[    8.290787] nodeadm[1988]: info system/local_disk.go:24 Not configuring local disks!
[    8.292057] nodeadm[1988]: info init/init.go:256 Set up system aspect {"name": "local-disk"}
[    8.293425] nodeadm[1988]: info init/init.go:150 Running daemons...
[    8.294477] nodeadm[1988]: info init/init.go:233 Ensuring daemon is running.. {"name": "containerd"}
[    9.038912] nodeadm[1988]: info init/init.go:237 Daemon is running {"name": "containerd"}
[    9.040593] nodeadm[1988]: info init/init.go:239 Running post-launch tasks.. {"name": "containerd"}
[    9.042344] nodeadm[1988]: info init/init.go:243 Finished post-launch tasks {"name": "containerd"}
[    9.044126] nodeadm[1988]: info init/init.go:233 Ensuring daemon is running.. {"name": "kubelet"}
[    9.343742] nodeadm[1988]: info init/init.go:237 Daemon is running {"name": "kubelet"}
[    9.345419] nodeadm[1988]: info init/init.go:239 Running post-launch tasks.. {"name": "kubelet"}
[    9.347114] nodeadm[1988]: info init/init.go:243 Finished post-launch tasks {"name": "kubelet"}
[    9.348744] nodeadm[1988]: info init/init.go:156 done! {"duration": 1.1056507}

Amazon Linux 2023.11.20260413
Kernel 6.12.79-101.147.amzn2023.x86_64 on an x86_64 (-)

        2026-04-22T20:50:17+00:00


C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>aws eks describe-cluster --name logguardian --region eu-west-1 --query "cluster.accessConfig" --output json
{
    "authenticationMode": "API"
}


C:\Users\torjm>aws sts get-caller-identity --region eu-west-1
{
    "UserId": "AIDASFIXCPWSI4DFR7I5D",
    "Account": "148761640356",
    "Arn": "arn:aws:iam::148761640356:user/Wassim"
}


C:\Users\torjm>aws ec2 describe-route-tables --filters "Name=association.subnet-id,Values=subnet-047b19142f9c03f04" --query "RouteTables[].{RouteTableId:RouteTableId,Routes:Routes[*].{Dest:DestinationCidrBlock,GatewayId:GatewayId,NatGatewayId:NatGatewayId}}" --output json
[
    {
        "RouteTableId": "rtb-0f405340e748cdc49",
        "Routes": [
            {
                "Dest": "10.0.0.0/16",
                "GatewayId": "local",
                "NatGatewayId": null
            },
            {
                "Dest": "0.0.0.0/0",
                "GatewayId": "igw-0e0967bc0f7e57c60",
                "NatGatewayId": null
            }
        ]
    }
]


C:\Users\torjm>aws ec2 get-console-output --instance-id i-060ddd61d217d9125 --output text
i-060ddd61d217d9125
  Booting `Amazon Linux (6.12.79-101.147.amzn2023.x86_64) 2023'

[    0.059591] RETBleed: WARNING: Spectre v2 mitigation leaves CPU vulnerable to RETBleed attacks, data leaks possible!
[    2.504264] systemd-journald[1197]: Received client request to flush runtime journal.
[    2.724519] RPC: Registered named UNIX socket transport module.
[    2.725269] RPC: Registered udp transport module.
[    2.725870] RPC: Registered tcp transport module.
[    2.726512] RPC: Registered tcp-with-tls transport module.
[    2.727245] RPC: Registered tcp NFSv4.1 backchannel transport module.
[    2.912673] input: Power Button as /devices/LNXSYSTM:00/LNXPWRBN:00/input/input0
[    2.919172] ena 0000:00:05.0: Elastic Network Adapter (ENA) v2.16.1g
[    2.936671] ena 0000:00:05.0: ENA device version: 0.10
[    2.937322] ena 0000:00:05.0: ENA controller version: 0.0.1 implementation version 1
[    2.959237] ACPI: button: Power Button [PWRF]
[    2.959895] input: Sleep Button as /devices/LNXSYSTM:00/LNXSLPBN:00/input/input1
[    2.968189] ACPI: button: Sleep Button [SLPF]
[    2.988224] i8042: PNP: PS/2 Controller [PNP0303:KBD,PNP0f13:MOU] at 0x60,0x64 irq 1,12
[    2.994820] i8042: Warning: Keylock active
[    2.997173] serio: i8042 KBD port at 0x60,0x64 irq 1
[    2.997704] serio: i8042 AUX port at 0x60,0x64 irq 12
[    3.020228] ena 0000:00:05.0: LLQ is not supported Fallback to host mode policy.
[    3.034247] ena 0000:00:05.0: Elastic Network Adapter (ENA) found at mem c0400000, mac addr 02:ca:87:6e:fe:91
[    3.053084] ena 0000:00:05.0 ens5: renamed from eth0
[    3.058569] nvme nvme0: using unchecked data buffer
[    3.214910] zram_generator::config[1806]: zram0: system has too much memory (3834MB), limit is 800MB, ignoring.
[    4.763503] nodeadm-internal[1862]: info boot-book/hook.go:58 Waiting for consistent network interfaces..
[    4.816927] nodeadm-internal[1862]: info system/networking.go:83 checking link states...
[    4.932882] nodeadm-internal[1862]: info system/networking.go:119 secondary link unmanaged {"linkName": "lo"}
[    4.934751] nodeadm-internal[1862]: info system/networking.go:112 link not yet configured {"linkName": "ens5", "linkState": "configuring"}
[    4.983151] nodeadm-internal[1862]: info system/networking.go:83 checking link states...
[    4.989731] nodeadm-internal[1862]: info system/networking.go:119 secondary link unmanaged {"linkName": "lo"}
[    4.991548] nodeadm-internal[1862]: info system/networking.go:109 link configured {"linkName": "ens5"}
[    4.993280] nodeadm-internal[1862]: info boot-book/hook.go:62 Completed boot hook!
[    5.241295] nodeadm[1879]: info init/init.go:55 Checking user is root..
[    5.242602] nodeadm[1879]: info init/init.go:65 Loading configuration.. {"configSource": ["imds://user-data"], "configCache": "/run/eks/nodeadm/config.json"}
[    5.244945] nodeadm[1879]: warn cli/config.go:25 failed to load cached config {"error": "open /run/eks/nodeadm/config.json: no such file or directory"}
[    5.258038] nodeadm[1879]: info init/init.go:75 Setting up nodeadm environment aspect...
[    5.259596] nodeadm[1879]: info init/init.go:84 Enriching configuration..
[    5.260877] nodeadm[1879]: info init/init.go:164 Fetching kubelet version..
[    5.262178] nodeadm[1879]: info kubelet/version.go:30 Reading kubelet version from file {"path": "/etc/eks/kubelet-version.txt"}
[    5.264155] nodeadm[1879]: info init/init.go:170 Fetched kubelet version {"version": "v1.35.3"}
[    5.265769] nodeadm[1879]: info init/init.go:171 Fetching instance details..
[    5.280383] nodeadm[1879]: SDK 2026/04/22 19:51:32 DEBUG attempting waiter request, attempt count: 1
[    5.499797] nodeadm[1879]: info init/init.go:201 Instance details populated {"details": {"id":"i-060ddd61d217d9125","region":"eu-west-1","type":"t3.medium","availabilityZone":"eu-west-1a","mac":"02:ca:87:6e:fe:91","privateDnsName":"ip-10-0-132-36.eu-west-1.compute.internal"}}
[    5.503993] nodeadm[1879]: info init/init.go:202 Fetching default options...
[    5.506274] nodeadm[1879]: info init/init.go:206 Default options populated {"defaults": {"sandboxImage":"localhost/kubernetes/pause"}}
[    5.508351] nodeadm[1879]: info init/init.go:89 Loaded configuration {"config": {"metadata":{},"spec":{"cluster":{"name":"logguardian","apiServerEndpoint":"https://1B3FC4C4EF0C07B7E44948A0E5644D55.gr7.eu-west-1.eks.amazonaws.com","certificateAuthority":"LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJWFN3YU9QQnJ1Ujh3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TmpBME1qQXlNVEl3TXpSYUZ3MHpOakEwTVRjeU1USTFNelJhTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUURsOHd5YzRuNjhZZmw2MXA3U1hsMGJ0L0ptRzRTa0dnbDBpTjNEQjE0QndWUnVKazRzaGFvTlJWOWwKeHZidjlsNGV1R3BCeXVJL1huODR5SDhUalhqa3FHTjBDb1MxVHp4MDBkbnJ3ZThxZWVDWUZ4Rngyc0FVdFhVSwpMSG9PNG1xUElYbzg4NFVraG45MisvSk5UdjZERTE3MXZMcnNYcUlGdGpiVElkdlhBK1FtL01lRUxuUWRNUHpFCngxYUpXNGRkdUhWN080QzZvVFVrdUR0d21TeFV2amI2Q0ZzbzdvbWZ4ZVVXMmxQRUlCRXpEM3l5aWdVbWk0UUYKak93NXcyVVlEY0YxUVlXMHoxQmpBalpubUlzaGlFZjgvOUZNUGR5cGxxaFI5ZjB2S21WUmEwVStiYVpLU05BQQpJeU1URj[2026-04-22T19:51:34.423871]hjUS9icVByUUF3S0FQdmVnRkRBNlFsQWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJUaml6ck5VT2R4KzZMYlNCQWJ1Ly9VbFRwUWh6QVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQzNGajJ5SzVXQwpuaUNMc0NDbG9SNUN4VXhmMG1KaVZDWWkrc3BpQnpmRGlzR0lHd2RKQjc1eHhKOHNJaVRpWGFpWHVwQjYxdG1FCkQ2OEtKSmkxeVdjeGpSNzY4Skc5cmx3a3k1YjN3bkphNHJvNlJDaXRXWC9aZkJTdWRKNGhsblk2K3dZTGY3SnQKa2hYaTNNK3R5d1FQT0JTUjAxcktOKzI0VVZ2ZDE5ZElLVUtHRG1kdDhGUFF5SFVHdTcwMXBmc0YxWTFjSTFaNApxVXlkVU16eDMzR1Rna2x1ZmFRNkc5L01ERnE2bHIxZHVOQXRIT2VLYWwzRTUraitWNURmRDhSYzU0YjcwVFlICnVHaU1ENTl5dXZ3eENDUCtNSHIzK2wzT2V0QmdVZ2xxWDZmWWtRNmZCQWg5bUNhVWE2WFE4UUNpcXB1d3MzcTcKZWpzRnNNUUF6TkVkCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K","cidr":"172.20.0.0/16"},"containerd":{},"instance":{"localStorage":{},"network":{}},"kubelet":{"config":{"clusterDNS":["172.20.0.10"],"maxPods":17},"flags":["--node-labels=eks.amazonaws.com/nodegroup-image=ami-052eb47703373f0fe,eks.amazonaws.com/capacityT[2026-04-22T19:51:34.523804]ype=ON_DEMAND,eks.amazonaws.com/nodegroup=logguardian-nodes-lt1"]}},"status":{"instance":{"id":"i-060ddd61d217d9125","region":"eu-west-1","type":"t3.medium","availabilityZone":"eu-west-1a","mac":"02:ca:87:6e:fe:91","privateDnsName":"ip-10-0-132-36.eu-west-1.compute.internal"},"default":{"sandboxImage":"localhost/kubernetes/pause"},"kubeletVersion":"v1.35.3"}}}
[    5.543072] nodeadm[1879]: info init/init.go:91 Validating configuration..
[    5.544472] nodeadm[1879]: info init/init.go:96 Creating daemon manager..
[    5.545768] nodeadm[1879]: info init/init.go:120 Setting up system config aspects...
[    5.547135] nodeadm[1879]: info init/init.go:252 Setting up system aspect.. {"name": "instance-environment"}
[    5.548801] nodeadm[1879]: info system/environment.go:75 No environment variables to configure
[    5.550605] nodeadm[1879]: info init/init.go:256 Set up system aspect {"name": "instance-environment"}
[    5.552199] nodeadm[1879]: info init/init.go:252 Setting up system aspect.. {"name": "resolve"}
[    5.553692] nodeadm[1879]: info init/init.go:256 Set up system aspect {"name": "resolve"}
[    5.555078] nodeadm[1879]: info init/init.go:129 Configuring daemons...
[    5.556221] nodeadm[1879]: info init/init.go:217 Configuring daemon... {"name": "containerd"}
[    5.557787] nodeadm[1879]: info containerd/base_runtime_spec.go:20 Writing containerd base runtime spec... {"path": "/etc/containerd/base-runtime-spec.json"}
[    5.560198] nodeadm[1879]: info containerd/version.go:33 Reading containerd version from file {"path": "/etc/eks/containerd-version.txt"}
[    5.562286] nodeadm[1879]: info containerd/config.go:77 Writing containerd config to file.. {"path": "/etc/containerd/config.toml"}
[    5.564239] nodeadm[1879]: info init/init.go:221 Configured daemon {"name": "containerd"}
[    5.565663] nodeadm[1879]: info init/init.go:217 Configuring daemon... {"name": "kubelet"}
[    5.567085] nodeadm[1879]: info kubelet/config.go:232 Setup IP for node {"ip": "10.0.132.36"}
[    5.568580] nodeadm[1879]: info kubelet/config.go:347 Writing kubelet config to file.. {"path": "/etc/kubernetes/kubelet/config.json"}
[    5.570679] nodeadm[1879]: info kubelet/config.go:372 Writing user kubelet config to drop-in file.. {"path": "/etc/kubernetes/kubelet/config.json.d/40-nodeadm.conf"}
[    5.573245] nodeadm[1879]: info init/init.go:221 Configured daemon {"name": "kubelet"}
[    5.574661] nodeadm[1879]: info init/init.go:156 done! {"duration": 0.336080266}
[    6.219692] cloud-init[1888]: Cloud-init v. 22.2.2 running 'init' at Wed, 22 Apr 2026 19:51:33 +0000. Up 6.19 seconds.
[    6.278122] cloud-init[1888]: ci-info: ++++++++++++++++++++++++++++++++++++++Net device info++++++++++++++++++++++++++++++++++++++
[    6.280268] cloud-init[1888]: ci-info: +--------+------+----------------------------+---------------+--------+-------------------+
[    6.282356] cloud-init[1888]: ci-info: | Device |  Up  |          Address           |      Mask     | Scope  |     Hw-Address    |
[    6.284343] cloud-init[1888]: ci-info: +--------+------+----------------------------+---------------+--------+-------------------+
[    6.286325] cloud-init[1888]: ci-info: |  ens5  | True |        10.0.132.36         | 255.255.240.0 | global | 02:ca:87:6e:fe:91 |
[    6.288307] cloud-init[1888]: ci-info: |  ens5  | True | fe80::ca:87ff:fe6e:fe91/64 |       .       |  link  | 02:ca:87:6e:fe:91 |
[    6.290309] cloud-init[1888]: ci-info: |   lo   | True |         127.0.0.1          |   255.0.0.0   |  host  |         .         |
[    6.292298] cloud-init[1888]: ci-info: |   lo   | True |          ::1/128           |       .       |  host  |         .         |
[    6.294307] cloud-init[1888]: ci-info: +--------+------+----------------------------+---------------+--------+-------------------+
[    6.296282] cloud-init[1888]: ci-info: +++++++++++++++++++++++++++++Route IPv4 info++++++++++++++++++++++++++++++
[    6.298010] cloud-init[1888]: ci-info: +-------+-------------+------------+-----------------+-----------+-------+
[    6.299717] cloud-init[1888]: ci-info: | Route | Destination |  Gateway   |     Genmask     | Interface | Flags |
[    6.301454] cloud-init[1888]: ci-info: +-------+-------------+------------+-----------------+-----------+-------+
[    6.303172] cloud-init[1888]: ci-info: |   0   |   0.0.0.0   | 10.0.128.1 |     0.0.0.0     |    ens5   |   UG  |
[    6.304906] cloud-init[1888]: ci-info: |   1   |   10.0.0.2  | 10.0.128.1 | 255.255.255.255 |    ens5   |  UGH  |
[    6.306669] cloud-init[1888]: ci-info: |   2   |  10.0.128.0 |  0.0.0.0   |  255.255.240.0  |    ens5   |   U   |
[    6.308379] cloud-init[1888]: ci-info: |   3   |  10.0.128.1 |  0.0.0.0   | 255.255.255.255 |    ens5   |   UH  |
[    6.310174] cloud-init[1888]: ci-info: +-------+-------------+------------+-----------------+-----------+-------+
[    6.311887] cloud-init[1888]: ci-info: +++++++++++++++++++Route IPv6 info+++++++++++++++++++
[    6.313325] cloud-init[1888]: ci-info: +-------+-------------+---------+-----------+-------+
[    6.314731] cloud-init[1888]: ci-info: | Route | Destination | Gateway | Interface | Flags |
[    6.316135] cloud-init[1888]: ci-info: +-------+-------------+---------+-----------+-------+
[    6.317575] cloud-init[1888]: ci-info: |   0   |  fe80::/64  |    ::   |    ens5   |   U   |
[    6.318974] cloud-init[1888]: ci-info: |   2   |    local    |    ::   |    ens5   |   U   |
[    6.320399] cloud-init[1888]: ci-info: |   3   |  multicast  |    ::   |    ens5   |   U   |
[    6.321862] cloud-init[1888]: ci-info: +-------+-------------+---------+-----------+-------+
[    6.648595] cloud-init[1888]: 2026-04-22 19:51:33,913 - __init__.py[WARNING]: Unhandled unknown content-type (application/node.eks.aws) userdata: 'b'---'...'
[    6.968557] cloud-init[1888]: Generating public/private ed25519 key pair.
[    6.970097] cloud-init[1888]: Your identification has been saved in /etc/ssh/ssh_host_ed25519_key
[    6.971713] cloud-init[1888]: Your public key has been saved in /etc/ssh/ssh_host_ed25519_key.pub
[    6.973255] cloud-init[1888]: The key fingerprint is:
[    6.974175] cloud-init[1888]: SHA256:7tSYYH8VogpQ7DvN8ynCK8TWOpreaTuzkEeacGH+l+4 root@ip-10-0-132-36.eu-west-1.compute.internal
[    6.976089] cloud-init[1888]: The key's randomart image is:
[    6.977056] cloud-init[1888]: +--[ED25519 256]--+
[    6.977925] cloud-init[1888]: |   ..            |
[    6.978779] cloud-init[1888]: |   ..            |
[    6.979628] cloud-init[1888]: |  +.      . .    |
[    6.980476] cloud-init[1888]: | o o.    . . .   |
[    6.981349] cloud-init[1888]: |..oo.+o S   .    |
[    6.982229] cloud-init[1888]: |..O.+o+* + .     |
[    6.983080] cloud-init[1888]: | B +..+o*.o      |
[    6.983929] cloud-init[1888]: | .B++o.oo.       |
[    6.984765] cloud-init[1888]: |+o.B*+E..        |
[    6.985620] cloud-init[1888]: +----[SHA256]-----+
[    6.986471] cloud-init[1888]: Generating public/private ecdsa key pair.
[    6.987625] cloud-init[1888]: Your identification has been saved in /etc/ssh/ssh_host_ecdsa_key
[    6.989100] cloud-init[1888]: Your public key has been saved in /etc/ssh/ssh_host_ecdsa_key.pub
[    6.990712] cloud-init[1888]: The key fingerprint is:
[    6.991660] cloud-init[1888]: SHA256:EtfOdoRrzIfXU3+sj8yTUcKsyJqyKyd4qASQFY9ZnmQ root@ip-10-0-132-36.eu-west-1.compute.internal
[    6.993598] cloud-init[1888]: The key's randomart image is:
[    6.994579] cloud-init[1888]: +---[ECDSA 256]---+
[    6.995431] cloud-init[1888]: |  o.E            |
[    6.996300] cloud-init[1888]: | o O .   . .     |
[    6.997197] cloud-init[1888]: |o o + . . o .o  .|
[    6.998056] cloud-init[1888]: |.      o = + .+oo|
[    6.998908] cloud-init[1888]: |.     . S.X.+.oo+|
[    6.999758] cloud-init[1888]: |.      . oo+. .o.|
[    7.000652] cloud-init[1888]: | . o     o    .o |
[    7.001560] cloud-init[1888]: |. o + o o    ooo |
[    7.002419] cloud-init[1888]: |.. . +o+      +..|
[    7.003350] cloud-init[1888]: +----[SHA256]-----+
2026/04/22 19:51:34Z: SSM Agent unable to acquire credentials: <error>no valid credentials could be retrieved for ec2 identity. Default Host Management Err: error calling RequestManagedInstanceRoleToken: AccessDeniedException: Systems Manager's instance management role is not configured for account: 148761640356
        status code: 400, request id: a0da9a01-9e1f-408f-ab71-69227710b261</error>
[    7.538191] cloud-init[1970]: Cloud-init v. 22.2.2 running 'modules:config' at Wed, 22 Apr 2026 19:51:34 +0000. Up 7.48 seconds.
[    7.954976] cloud-init[1975]: Cloud-init v. 22.2.2 running 'modules:final' at Wed, 22 Apr 2026 19:51:35 +0000. Up 7.90 seconds.
ci-info: no authorized SSH keys fingerprints found for user ec2-user.
<14>Apr 22 19:51:35 cloud-init: #############################################################
<14>Apr 22 19:51:35 cloud-init: -----BEGIN SSH HOST KEY FINGERPRINTS-----
<14>Apr 22 19:51:35 cloud-init: 256 SHA256:EtfOdoRrzIfXU3+sj8yTUcKsyJqyKyd4qASQFY9ZnmQ root@ip-10-0-132-36.eu-west-1.compute.internal (ECDSA)
<14>Apr 22 19:51:35 cloud-init: 256 SHA256:7tSYYH8VogpQ7DvN8ynCK8TWOpreaTuzkEeacGH+l+4 root@ip-10-0-132-36.eu-west-1.compute.internal (ED25519)
<14>Apr 22 19:51:35 cloud-init: -----END SSH HOST KEY FINGERPRINTS-----
<14>Apr 22 19:51:35 cloud-init: #############################################################
-----BEGIN SSH HOST KEY KEYS-----
ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEOqj28+oUE+KzOfLLU9ksMMQMQX4Xogyw7RakUjZBj6o2ndyPM+oZx42OdMQvwnGw/JtYx9e2+PG0Qyk8+zqlc= root@ip-10-0-132-36.eu-west-1.compute.internal
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBegbolJwkf1GRFhjRZxe2WW7dVsg+qy68eiAe1Dff4p root@ip-10-0-132-36.eu-west-1.compute.internal
-----END SSH HOST KEY KEYS-----
[    8.090254] cloud-init[1975]: Cloud-init v. 22.2.2 finished at Wed, 22 Apr 2026 19:51:35 +0000. Datasource DataSourceEc2.  Up 8.08 seconds
[    8.238309] nodeadm[1988]: info init/init.go:55 Checking user is root..
[    8.239916] nodeadm[1988]: info init/init.go:65 Loading configuration.. {"configSource": ["imds://user-data", "file:///etc/eks/nodeadm.d/"], "configCache": "/run/eks/nodeadm/config.json"}
[    8.246155] nodeadm[1988]: warn configprovider/chain.go:30 Encountered error in config provider {"error": "no config found in directory"}
[    8.248396] nodeadm[1988]: info init/init.go:75 Setting up nodeadm environment aspect...
[    8.249965] nodeadm[1988]: info init/init.go:89 Loaded configuration {"config": {"metadata":{},"spec":{"cluster":{"name":"logguardian","apiServerEndpoint":"https://1B3FC4C4EF0C07B7E44948A0E5644D55.gr7.eu-west-1.eks.amazonaws.com","certificateAuthority":"LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJWFN3YU9QQnJ1Ujh3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TmpBME1qQXlNVEl3TXpSYUZ3MHpOakEwTVRjeU1USTFNelJhTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUURsOHd5YzRuNjhZZmw2MXA3U1hsMGJ0L0ptRzRTa0dnbDBpTjNEQjE0QndWUnVKazRzaGFvTlJWOWwKeHZidjlsNGV1R3BCeXVJL1huODR5SDhUalhqa3FHTjBDb1MxVHp4MDBkbnJ3ZThxZWVDWUZ4Rngyc0FVdFhVSwpMSG9PNG1xUElYbzg4NFVraG45MisvSk5UdjZERTE3MXZMcnNYcUlGdGpiVElkdlhBK1FtL01lRUxuUWRNUHpFCngxYUpXNGRkdUhWN080QzZvVFVrdUR0d21TeFV2amI2Q0ZzbzdvbWZ4ZVVXMmxQRUlCRXpEM3l5aWdVbWk0UUYKak93NXcyVVlEY0YxUVlXMHoxQmpBalpubUlzaGlFZjgvOUZNUGR5cGxxaFI5ZjB2S21WUmEwVStiYVpLU05BQQpJeU1URj[2026-04-22T19:51:37.224063]hjUS9icVByUUF3S0FQdmVnRkRBNlFsQWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJUaml6ck5VT2R4KzZMYlNCQWJ1Ly9VbFRwUWh6QVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQzNGajJ5SzVXQwpuaUNMc0NDbG9SNUN4VXhmMG1KaVZDWWkrc3BpQnpmRGlzR0lHd2RKQjc1eHhKOHNJaVRpWGFpWHVwQjYxdG1FCkQ2OEtKSmkxeVdjeGpSNzY4Skc5cmx3a3k1YjN3bkphNHJvNlJDaXRXWC9aZkJTdWRKNGhsblk2K3dZTGY3SnQKa2hYaTNNK3R5d1FQT0JTUjAxcktOKzI0VVZ2ZDE5ZElLVUtHRG1kdDhGUFF5SFVHdTcwMXBmc0YxWTFjSTFaNApxVXlkVU16eDMzR1Rna2x1ZmFRNkc5L01ERnE2bHIxZHVOQXRIT2VLYWwzRTUraitWNURmRDhSYzU0YjcwVFlICnVHaU1ENTl5dXZ3eENDUCtNSHIzK2wzT2V0QmdVZ2xxWDZmWWtRNmZCQWg5bUNhVWE2WFE4UUNpcXB1d3MzcTcKZWpzRnNNUUF6TkVkCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K","cidr":"172.20.0.0/16"},"containerd":{},"instance":{"localStorage":{},"network":{}},"kubelet":{"config":{"clusterDNS":["172.20.0.10"],"maxPods":17},"flags":["--node-labels=eks.amazonaws.com/nodegroup-image=ami-052eb47703373f0fe,eks.amazonaws.com/capacityT[2026-04-22T19:51:37.224086]ype=ON_DEMAND,eks.amazonaws.com/nodegroup=logguardian-nodes-lt1"]}},"status":{"instance":{"id":"i-060ddd61d217d9125","region":"eu-west-1","type":"t3.medium","availabilityZone":"eu-west-1a","mac":"02:ca:87:6e:fe:91","privateDnsName":"ip-10-0-132-36.eu-west-1.compute.internal"},"default":{"sandboxImage":"localhost/kubernetes/pause"},"kubeletVersion":"v1.35.3"}}}
[    8.282444] nodeadm[1988]: info init/init.go:91 Validating configuration..
[    8.283968] nodeadm[1988]: info init/init.go:96 Creating daemon manager..
[    8.285288] nodeadm[1988]: info init/init.go:141 Setting up system run aspects...
[    8.286567] nodeadm[1988]: info init/init.go:252 Setting up system aspect.. {"name": "marker"}
[    8.288001] nodeadm[1988]: info init/init.go:256 Set up system aspect {"name": "marker"}
[    8.289308] nodeadm[1988]: info init/init.go:252 Setting up system aspect.. {"name": "local-disk"}
[    8.290787] nodeadm[1988]: info system/local_disk.go:24 Not configuring local disks!
[    8.292057] nodeadm[1988]: info init/init.go:256 Set up system aspect {"name": "local-disk"}
[    8.293425] nodeadm[1988]: info init/init.go:150 Running daemons...
[    8.294477] nodeadm[1988]: info init/init.go:233 Ensuring daemon is running.. {"name": "containerd"}
[    9.038912] nodeadm[1988]: info init/init.go:237 Daemon is running {"name": "containerd"}
[    9.040593] nodeadm[1988]: info init/init.go:239 Running post-launch tasks.. {"name": "containerd"}
[    9.042344] nodeadm[1988]: info init/init.go:243 Finished post-launch tasks {"name": "containerd"}
[    9.044126] nodeadm[1988]: info init/init.go:233 Ensuring daemon is running.. {"name": "kubelet"}
[    9.343742] nodeadm[1988]: info init/init.go:237 Daemon is running {"name": "kubelet"}
[    9.345419] nodeadm[1988]: info init/init.go:239 Running post-launch tasks.. {"name": "kubelet"}
[    9.347114] nodeadm[1988]: info init/init.go:243 Finished post-launch tasks {"name": "kubelet"}
[    9.348744] nodeadm[1988]: info init/init.go:156 done! {"duration": 1.1056507}

Amazon Linux 2023.11.20260413
Kernel 6.12.79-101.147.amzn2023.x86_64 on an x86_64 (-)

        2026-04-22T19:58:05+00:00


C:\Users\torjm>aws eks list-access-entries --cluster-name logguardian --region eu-west-1
{
    "accessEntries": [
        "arn:aws:iam::148761640356:role/aws-service-role/eks.amazonaws.com/AWSServiceRoleForAmazonEKS",
        "arn:aws:iam::148761640356:role/logguardian-eks-node-role",
        "arn:aws:iam::148761640356:root",
        "arn:aws:iam::148761640356:user/Wassim"
    ]
}


C:\Users\torjm>aws ec2 describe-network-acls --filters "Name=association.subnet-id,Values=subnet-044076265d5c4e5a7,subnet-0a27aa6f1017701ad" --region eu-west-1 --output json
{
    "NetworkAcls": [
        {
            "Associations": [
                {
                    "NetworkAclAssociationId": "aclassoc-0acad7c8755879fa2",
                    "NetworkAclId": "acl-0f5cbefbe6a40e8f5",
                    "SubnetId": "subnet-047b19142f9c03f04"
                },
                {
                    "NetworkAclAssociationId": "aclassoc-0ed73e22c81c8c7ce",
                    "NetworkAclId": "acl-0f5cbefbe6a40e8f5",
                    "SubnetId": "subnet-0a27aa6f1017701ad"
                },
                {
                    "NetworkAclAssociationId": "aclassoc-0c201311b639d8846",
                    "NetworkAclId": "acl-0f5cbefbe6a40e8f5",
                    "SubnetId": "subnet-0e33579915f8895b5"
                },
                {
                    "NetworkAclAssociationId": "aclassoc-071a79d9c9665ec2f",
                    "NetworkAclId": "acl-0f5cbefbe6a40e8f5",
                    "SubnetId": "subnet-044076265d5c4e5a7"
                }
            ],
            "Entries": [
                {
                    "CidrBlock": "0.0.0.0/0",
                    "Egress": true,
                    "Protocol": "-1",
                    "RuleAction": "allow",
                    "RuleNumber": 100
                },
                {
                    "CidrBlock": "0.0.0.0/0",
                    "Egress": true,
                    "Protocol": "-1",
                    "RuleAction": "deny",
                    "RuleNumber": 32767
                },
                {
                    "CidrBlock": "0.0.0.0/0",
                    "Egress": false,
                    "Protocol": "-1",
                    "RuleAction": "allow",
                    "RuleNumber": 100
                },
                {
                    "CidrBlock": "0.0.0.0/0",
                    "Egress": false,
                    "Protocol": "-1",
                    "RuleAction": "deny",
                    "RuleNumber": 32767
                }
            ],
            "IsDefault": true,
            "NetworkAclId": "acl-0f5cbefbe6a40e8f5",
            "Tags": [],
            "VpcId": "vpc-0c389465f6951eeaa",
            "OwnerId": "148761640356"
        }
    ]
}


C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>aws eks describe-access-entry --cluster-name logguardian --principal-arn arn:aws:iam::148761640356:role/logguardian-eks-node-role --region eu-west-1
{
    "accessEntry": {
        "clusterName": "logguardian",
        "principalArn": "arn:aws:iam::148761640356:role/logguardian-eks-node-role",
        "kubernetesGroups": [
            "system:nodes"
        ],
        "accessEntryArn": "arn:aws:eks:eu-west-1:148761640356:access-entry/logguardian/role/148761640356/logguardian-eks-node-role/c4ced664-7b8a-11a8-ee4c-e937fc14e4a6",
        "createdAt": "2026-04-20T23:25:44.917000+02:00",
        "modifiedAt": "2026-04-20T23:25:44.917000+02:00",
        "tags": {},
        "username": "system:node:{{SessionName}}",
        "type": "EC2"
    }
}


C:\Users\torjm>aws eks delete-access-entry --cluster-name logguardian --principal-arn arn:aws:iam::148761640356:role/logguardian-eks-node-role --region eu-west-1

C:\Users\torjm>aws eks describe-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes-lt --region eu-west-1 --query "nodegroup.status"
"CREATING"


C:\Users\torjm>aws eks describe-access-entry --cluster-name logguardian --principal-arn arn:aws:iam::148761640356:role/logguardian-eks-node-role --region eu-west-1

aws: [ERROR]: An error occurred (ResourceNotFoundException) when calling the DescribeAccessEntry operation: The specified principalArn could not be found. You can view your available access entries with 'list-access-entries'.

C:\Users\torjm>kubectl get nodes -o wide
No resources found

C:\Users\torjm>aws eks describe-access-entry --cluster-name logguardian --principal-arn arn:aws:iam::148761640356:role/logguardian-eks-node-role --region eu-west-1
{
    "accessEntry": {
        "clusterName": "logguardian",
        "principalArn": "arn:aws:iam::148761640356:role/logguardian-eks-node-role",
        "kubernetesGroups": [
            "system:nodes"
        ],
        "accessEntryArn": "arn:aws:eks:eu-west-1:148761640356:access-entry/logguardian/role/148761640356/logguardian-eks-node-role/a8cedb81-881e-50a5-a519-d0a00802d71c",
        "createdAt": "2026-04-22T23:05:24.604000+02:00",
        "modifiedAt": "2026-04-22T23:05:24.604000+02:00",
        "tags": {},
        "username": "system:node:{{EC2PrivateDNSName}}",
        "type": "EC2_LINUX"
    }
}


C:\Users\torjm>aws eks describe-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes-lt --region eu-west-1 --query "nodegroup.status"
"ACTIVE"


C:\Users\torjm>aws eks delete-access-entry --cluster-name logguardian --principal-arn arn:aws:iam::148761640356:role/logguardian-eks-node-role --region eu-west-1


C:\Users\torjm>kubectl get nodes -o wide
NAME                                         STATUS   ROLES    AGE    VERSION               INTERNAL-IP    EXTERNAL-IP   OS-IMAGE                        KERNEL-VERSION                    CONTAINER-RUNTIME
ip-10-0-142-206.eu-west-1.compute.internal   Ready    <none>   100s   v1.35.3-eks-bbe087e   10.0.142.206   <none>        Amazon Linux 2023.11.20260413   6.12.79-101.147.amzn2023.x86_64   containerd://2.2.1+unknown
ip-10-0-152-229.eu-west-1.compute.internal   Ready    <none>   99s    v1.35.3-eks-bbe087e   10.0.152.229   <none>        Amazon Linux 2023.11.20260413   6.12.79-101.147.amzn2023.x86_64   containerd://2.2.1+unknown

C:\Users\torjm>kubectl get pods -n kube-system
NAME                           READY   STATUS    RESTARTS   AGE
aws-node-4zvxh                 2/2     Running   0          2m8s
aws-node-p2s4z                 2/2     Running   0          2m9s
coredns-7c565cc4db-jr242       1/1     Running   0          47h
coredns-7c565cc4db-ttk6z       1/1     Running   0          47h
eks-pod-identity-agent-6dkfd   1/1     Running   0          2m9s
eks-pod-identity-agent-gp977   1/1     Running   0          2m8s
kube-proxy-5df4c               1/1     Running   0          2m8s
kube-proxy-h8jjs               1/1     Running   0          2m9s

C:\Users\torjm>aws ecr create-repository --repository-name logguardian/log-generator --region eu-west-1
{
    "repository": {
        "repositoryArn": "arn:aws:ecr:eu-west-1:148761640356:repository/logguardian/log-generator",
        "registryId": "148761640356",
        "repositoryName": "logguardian/log-generator",
        "repositoryUri": "148761640356.dkr.ecr.eu-west-1.amazonaws.com/logguardian/log-generator",
        "createdAt": "2026-04-22T23:08:47.747000+02:00",
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": false
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}


C:\Users\torjm>aws ecr create-repository --repository-name logguardian/etl-processor --region eu-west-1
{
    "repository": {
        "repositoryArn": "arn:aws:ecr:eu-west-1:148761640356:repository/logguardian/etl-processor",
        "registryId": "148761640356",
        "repositoryName": "logguardian/etl-processor",
        "repositoryUri": "148761640356.dkr.ecr.eu-west-1.amazonaws.com/logguardian/etl-processor",
        "createdAt": "2026-04-22T23:09:00.863000+02:00",
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": false
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}


C:\Users\torjm>aws ecr create-repository --repository-name logguardian/ml-model --region eu-west-1
{
    "repository": {
        "repositoryArn": "arn:aws:ecr:eu-west-1:148761640356:repository/logguardian/ml-model",
        "registryId": "148761640356",
        "repositoryName": "logguardian/ml-model",
        "repositoryUri": "148761640356.dkr.ecr.eu-west-1.amazonaws.com/logguardian/ml-model",
        "createdAt": "2026-04-22T23:09:04.848000+02:00",
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": false
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}


C:\Users\torjm>aws ecr create-repository --repository-name logguardian/monitoring-ui --region eu-west-1
{
    "repository": {
        "repositoryArn": "arn:aws:ecr:eu-west-1:148761640356:repository/logguardian/monitoring-ui",
        "registryId": "148761640356",
        "repositoryName": "logguardian/monitoring-ui",
        "repositoryUri": "148761640356.dkr.ecr.eu-west-1.amazonaws.com/logguardian/monitoring-ui",
        "createdAt": "2026-04-22T23:09:09.183000+02:00",
        "imageTagMutability": "MUTABLE",
        "imageScanningConfiguration": {
            "scanOnPush": false
        },
        "encryptionConfiguration": {
            "encryptionType": "AES256"
        }
    }
}


C:\Users\torjm>aws ecr describe-repositories --query "repositories[*].repositoryName" --output table --region eu-west-1
-------------------------------
|    DescribeRepositories     |
+-----------------------------+
|  logguardian/log-generator  |
|  logguardian/ml-model       |
|  logguardian/monitoring-ui  |
|  logguardian/etl-processor  |
+-----------------------------+


C:\Users\torjm>aws ecr describe-repositories --query "repositories[*].repositoryName" --output table --regi

aws: [ERROR]: An error occurred (ParamValidation): argument --region: expected one argument

usage: aws [options] <command> <subcommand> [<subcommand> ...] [parameters]
To see help text, you can run:

  aws help
  aws <command> help
  aws <command> <subcommand> help


C:\Users\torjm>aws s3 mb s3://logguardian-datalake-148761640356 --region eu-west-1
make_bucket: logguardian-datalake-148761640356

C:\Users\torjm>aws s3api put-bucket-intelligent-tiering-configuration --bucket logguardian-datalake-148761640356 --id auto-tiering --intelligent-tiering-configuration '{"Id":"auto-tiering","Status":"Enabled","Tierings":[{"AccessTier":"ARCHIVE_ACCESS","Days":90}]}' --region eu-west-1

aws: [ERROR]: An error occurred (ParamValidation): Error parsing parameter '--intelligent-tiering-configuration': Expected: '=', received: ''' for input:
 '{Id:auto-tiering,Status:Enabled,Tierings:[{AccessTier:ARCHIVE_ACCESS,Days:90}]}'
^

C:\Users\torjm>aws secretsmanager create-secret --name logguardian/config --secret-string '{"kafka_topic":"logs-raw","alert_threshold":"0.85","environment":"dev"}' --region eu-west-1
{
    "ARN": "arn:aws:secretsmanager:eu-west-1:148761640356:secret:logguardian/config-qbDGgj",
    "Name": "logguardian/config",
    "VersionId": "a63da831-0991-4519-8baf-6c4d4cd190fb"
}


C:\Users\torjm>aws s3api put-bucket-intelligent-tiering-configuration --bucket logguardian-datalake-148761640356 --id auto-tiering --intelligent-tiering-configuration "{\"Id\":\"auto-tiering\",\"Status\":\"Enabled\",\"Tierings\":[{\"AccessTier\":\"ARCHIVE_ACCESS\",\"Days\":90}]}" --region eu-west-1

C:\Users\torjm>aws ecr describe-repositories --query "repositories[*].repositoryName" --output table --region eu-west-1
-------------------------------
|    DescribeRepositories     |
+-----------------------------+
|  logguardian/log-generator  |
|  logguardian/ml-model       |
|  logguardian/monitoring-ui  |
|  logguardian/etl-processor  |
+-----------------------------+


C:\Users\torjm>aws s3 ls | findstr logguardian
2026-04-22 23:09:40 logguardian-datalake-148761640356

C:\Users\torjm>aws secretsmanager describe-secret --secret-id logguardian/config --query "{Name:Name,Created:CreatedDate}" --region eu-west-1
{
    "Name": "logguardian/config",
    "Created": "2026-04-22T23:11:07.382000+02:00"
}


C:\Users\torjm>aws logs create-log-group --log-group-name /logguardian/application --region eu-west-1

C:\Users\torjm>aws logs put-retention-policy --log-group-name /logguardian/application --retention-in-days 14 --region eu-west-1

C:\Users\torjm>aws logs create-log-group --log-group-name /logguardian/etl --region eu-west-1

C:\Users\torjm>aws logs put-retention-policy --log-group-name /logguardian/etl --retention-in-days 7 --region eu-west-1

C:\Users\torjm>aws logs create-log-group --log-group-name /logguardian/ml-model --region eu-west-1

C:\Users\torjm>aws logs put-retention-policy --log-group-name /logguardian/ml-model --retention-in-days 7 --region eu-west-1

C:\Users\torjm>aws logs put-retention-policy --log-group-name /aws/eks/logguardian/cluster --retention-in-days 7 --region eu-west-1

C:\Users\torjm>aws cloudwatch put-metric-alarm --alarm-name logguardian-cpu-high --metric-name node_cpu_utilization --namespace ContainerInsights --statistic Average --period 300 --threshold 80 --comparison-operator GreaterThanThreshold --evaluation-periods 2 --dimensions Name=ClusterName,Value=logguardian --alarm-actions arn:aws:sns:eu-west-1:148761640356:logguardian-alerts --region eu-west-1

C:\Users\torjm>aws sns create-topic --name logguardian-alerts --region eu-west-1
{
    "TopicArn": "arn:aws:sns:eu-west-1:148761640356:logguardian-alerts"
}


C:\Users\torjm>aws sns subscribe --topic-arn arn:aws:sns:eu-west-1:148761640356:logguardian-alerts --protocol email --notification-endpoint torjmane521@gmail.com --region eu-west-1
{
    "SubscriptionArn": "pending confirmation"
}


C:\Users\torjm>aws cloudwatch put-metric-alarm --alarm-name logguardian-cpu-high --metric-name node_cpu_utilization --namespace ContainerInsights --statistic Average --period 300 --threshold 80 --comparison-operator GreaterThanThreshold --evaluation-periods 2 --dimensions Name=ClusterName,Value=logguardian --alarm-actions arn:aws:sns:eu-west-1:148761640356:logguardian-alerts --region eu-west-1

C:\Users\torjm>aws cloudwatch put-metric-alarm --alarm-name logguardian-memory-high --metric-name node_memory_utilization --namespace ContainerInsights --statistic Average --period 300 --threshold 85 --comparison-operator GreaterThanThreshold --evaluation-periods 2 --dimensions Name=ClusterName,Value=logguardian --alarm-actions arn:aws:sns:eu-west-1:148761640356:logguardian-alerts --region eu-west-1

C:\Users\torjm>aws cloudwatch put-metric-alarm --alarm-name logguardian-pod-restarts --metric-name pod_number_of_container_restarts --namespace ContainerInsights --statistic Sum --period 600 --threshold 3 --comparison-operator GreaterThanThreshold --evaluation-periods 1 --dimensions Name=ClusterName,Value=logguardian --alarm-actions arn:aws:sns:eu-west-1:148761640356:logguardian-alerts --region eu-west-1

C:\Users\torjm>aws logs describe-log-groups --log-group-name-prefix /logguardian --query "logGroups[*].{Name:logGroupName,Retention:retentionInDays}" --output table --region eu-west-1
-------------------------------------------
|            DescribeLogGroups            |
+---------------------------+-------------+
|           Name            |  Retention  |
+---------------------------+-------------+
|  /logguardian/application |  14         |
|  /logguardian/etl         |  7          |
|  /logguardian/ml-model    |  7          |
+---------------------------+-------------+


C:\Users\torjm>aws cloudwatch describe-alarms --alarm-name-prefix logguardian --query "MetricAlarms[*].{Name:AlarmName,State:StateValue}" --output table --region eu-west-1
---------------------------------------------------
|                 DescribeAlarms                  |
+---------------------------+---------------------+
|           Name            |        State        |
+---------------------------+---------------------+
|  logguardian-cpu-high     |  INSUFFICIENT_DATA  |
|  logguardian-memory-high  |  INSUFFICIENT_DATA  |
|  logguardian-pod-restarts |  INSUFFICIENT_DATA  |
+---------------------------+---------------------+


C:\Users\torjm>aws codestar-connections create-connection --provider-type GitHub --connection-name logguardian-github --region eu-west-1
{
    "ConnectionArn": "arn:aws:codestar-connections:eu-west-1:148761640356:connection/7a00dbf3-1105-45ed-aba5-11c8e0166095"
}


C:\Users\torjm>aws codebuild delete-project --name logguardian-build --region eu-west-

aws: [ERROR]: Provided region_name 'eu-west-' doesn't match a supported format.

C:\Users\torjm>aws codebuild delete-project --name logguardian-build --region eu-west-1

C:\Users\torjm>aws codebuild create-project --name logguardian-build --source "{\"type\":\"CODEPIPELINE\",\"buildspec\":\"buildspec.yml\"}" --artifacts "{\"type\":\"CODEPIPELINE\"}" --environment "{\"type\":\"LINUX_CONTAINER\",\"image\":\"aws/codebuild/amazonlinux2-x86_64-standard:5.0\",\"computeType\":\"BUILD_GENERAL1_SMALL\",\"privilegedMode\":true}" --service-role arn:aws:iam::148761640356:role/logguardian-codebuild-role --region eu-west-1

aws: [ERROR]: An error occurred (InvalidInputException) when calling the CreateProject operation: CodeBuild is not authorized to perform: sts:AssumeRole on service role. Please verify that: 1) The provided service role exists, 2) The role name is case-sensitive and matches exactly, and 3) The role has the necessary trust policy configured.

C:\Users\torjm>aws iam get-role --role-name logguardian-codebuild-role --query "Role.AssumeRolePolicyDocument" --output json

aws: [ERROR]: An error occurred (NoSuchEntity) when calling the GetRole operation: The role with name logguardian-codebuild-role cannot be found.

C:\Users\torjm>echo {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"codebuild.amazonaws.com"},"Action":"sts:AssumeRole"}]} > C:\Users\torjm\trust-codebuild.json

C:\Users\torjm>aws iam update-assume-role-policy --role-name logguardian-codebuild-role --policy-document file://C:\Users\torjm\trust-codebuild.json

aws: [ERROR]: An error occurred (NoSuchEntity) when calling the UpdateAssumeRolePolicy operation: The role with name logguardian-codebuild-role cannot be found.

C:\Users\torjm>aws iam create-role --role-name logguardian-codebuild-role --assume-role-policy-document file://C:\Users\torjm\trust-codebuild.json
{
    "Role": {
        "Path": "/",
        "RoleName": "logguardian-codebuild-role",
        "RoleId": "AROASFIXCPWSAFLZWXH57",
        "Arn": "arn:aws:iam::148761640356:role/logguardian-codebuild-role",
        "CreateDate": "2026-04-22T22:31:12+00:00",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "codebuild.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
    }
}


C:\Users\torjm>aws iam attach-role-policy --role-name logguardian-codebuild-role --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPowerUser

C:\Users\torjm>aws iam attach-role-policy --role-name logguardian-codebuild-role --policy-arn arn:aws:iam::aws:policy/CloudWatchLogsFullAccess

C:\Users\torjm>aws iam attach-role-policy --role-name logguardian-codebuild-role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

C:\Users\torjm>aws codebuild create-project --name logguardian-build --source "{\"type\":\"CODEPIPELINE\",\"buildspec\":\"buildspec.yml\"}" --artifacts "{\"type\":\"CODEPIPELINE\"}" --environment "{\"type\":\"LINUX_CONTAINER\",\"image\":\"aws/codebuild/amazonlinux2-x86_64-standard:5.0\",\"computeType\":\"BUILD_GENERAL1_SMALL\",\"privilegedMode\":true}" --service-role arn:aws:iam::148761640356:role/logguardian-codebuild-role --region eu-west-1
{
    "project": {
        "name": "logguardian-build",
        "arn": "arn:aws:codebuild:eu-west-1:148761640356:project/logguardian-build",
        "source": {
            "type": "CODEPIPELINE",
            "buildspec": "buildspec.yml",
            "insecureSsl": false
        },
        "artifacts": {
            "type": "CODEPIPELINE",
            "name": "logguardian-build",
            "packaging": "NONE",
            "encryptionDisabled": false
        },
        "cache": {
            "type": "NO_CACHE"
        },
        "environment": {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/amazonlinux2-x86_64-standard:5.0",
            "computeType": "BUILD_GENERAL1_SMALL",
            "environmentVariables": [],
            "privilegedMode": true,
            "imagePullCredentialsType": "CODEBUILD"
        },
        "serviceRole": "arn:aws:iam::148761640356:role/logguardian-codebuild-role",
        "timeoutInMinutes": 60,
        "queuedTimeoutInMinutes": 480,
        "encryptionKey": "arn:aws:kms:eu-west-1:148761640356:alias/aws/s3",
        "created": "2026-04-23T00:32:06.310000+02:00",
        "lastModified": "2026-04-23T00:32:06.310000+02:00",
        "badge": {
            "badgeEnabled": false
        },
        "projectVisibility": "PRIVATE"
    }
}


C:\Users\torjm>aws iam list-roles --query "Roles[?starts_with(RoleName,'logguardian')].RoleName" --output table
----------------------------------
|            ListRoles           |
+--------------------------------+
|  logguardian-codebuild-role    |
|  logguardian-eks-cluster-role  |
|  logguardian-eks-node-role     |
+--------------------------------+


C:\Users\torjm>aws codebuild list-projects --region eu-west-1
{
    "projects": [
        "logguardian-build"
    ]
}


C:\Users\torjm>aws s3 mb s3://logguardian-pipeline-artifacts-148761640356 --region eu-west-1
make_bucket: logguardian-pipeline-artifacts-148761640356

C:\Users\torjm>aws iam put-role-policy --role-name logguardian-codepipeline-role --policy-name codestar-connection --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"codestar-connections:UseConnection\"],\"Resource\":\"CONNECTION_ARN\"}]}"

aws: [ERROR]: An error occurred (NoSuchEntity) when calling the PutRolePolicy operation: The role with name logguardian-codepipeline-role cannot be found.

C:\Users\torjm>aws iam get-role --role-name logguardian-codepipeline-role --query "Role.RoleName" --output text

aws: [ERROR]: An error occurred (NoSuchEntity) when calling the GetRole operation: The role with name logguardian-codepipeline-role cannot be found.

C:\Users\torjm>echo {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"codepipeline.amazonaws.com"},"Action":"sts:AssumeRole"}]} > C:\Users\torjm\trust-pipeline.json

C:\Users\torjm>aws iam create-role --role-name logguardian-codepipeline-role --assume-role-policy-document file://C:\Users\torjm\trust-pipeline.json
{
    "Role": {
        "Path": "/",
        "RoleName": "logguardian-codepipeline-role",
        "RoleId": "AROASFIXCPWSM7VVT7MZ3",
        "Arn": "arn:aws:iam::148761640356:role/logguardian-codepipeline-role",
        "CreateDate": "2026-04-22T22:34:33+00:00",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "codepipeline.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
    }
}


C:\Users\torjm>aws iam put-role-policy --role-name logguardian-codepipeline-role --policy-name pipeline-permissions --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"codebuild:*\",\"s3:*\",\"ecr:*\",\"codestar-connections:UseConnection\"],\"Resource\":\"*\"}]}"

C:\Users\torjm> codestar-connections list-connections --region eu-west-1 --query "Connections[0].ConnectionArn" --output text
'codestar-connections' n’est pas reconnu en tant que commande interne
ou externe, un programme exécutable ou un fichier de commandes.

C:\Users\torjm>aws codestar-connections list-connections --region eu-west-1 --query "Connections[0].ConnectionArn" --output text
arn:aws:codestar-connections:eu-west-1:148761640356:connection/7a00dbf3-1105-45ed-aba5-11c8e0166095


C:\Users\torjm>aws codepipeline create-pipeline --cli-input-json file://C:\Users\torjm\pipeline.json --region eu-west-1
{
    "pipeline": {
        "name": "logguardian-pipeline",
        "roleArn": "arn:aws:iam::148761640356:role/logguardian-codepipeline-role",
        "artifactStore": {
            "type": "S3",
            "location": "logguardian-pipeline-artifacts-148761640356"
        },
        "stages": [
            {
                "name": "Source",
                "actions": [
                    {
                        "name": "GitHub-Source",
                        "actionTypeId": {
                            "category": "Source",
                            "owner": "AWS",
                            "provider": "CodeStarSourceConnection",
                            "version": "1"
                        },
                        "runOrder": 1,
                        "configuration": {
                            "BranchName": "develop",
                            "ConnectionArn": "arn:aws:codestar-connections:eu-west-1:148761640356:connection/7a00dbf3-1105-45ed-aba5-11c8e0166095",
                            "FullRepositoryId": "WassimTorjmen/logguardian",
                            "OutputArtifactFormat": "CODE_ZIP"
                        },
                        "outputArtifacts": [
                            {
                                "name": "SourceOutput"
                            }
                        ],
                        "inputArtifacts": []
                    }
                ]
            },
            {
                "name": "Build",
                "actions": [
                    {
                        "name": "Docker-Build",
                        "actionTypeId": {
                            "category": "Build",
                            "owner": "AWS",
                            "provider": "CodeBuild",
                            "version": "1"
                        },
                        "runOrder": 1,
                        "configuration": {
                            "ProjectName": "logguardian-build"
                        },
                        "outputArtifacts": [
                            {
                                "name": "BuildOutput"
                            }
                        ],
                        "inputArtifacts": [
                            {
                                "name": "SourceOutput"
                            }
                        ]
                    }
                ]
            }
        ],
        "version": 1,
        "executionMode": "SUPERSEDED",
        "pipelineType": "V2"
    }
}


C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>aws codepipeline get-pipeline-state --name logguardian-pipeline --query "stageStates[*].{Stage:stageName,Status:latestExecution.status}" --output table --region eu-west-1
----------------------
|  GetPipelineState  |
+---------+----------+
|  Stage  | Status   |
+---------+----------+
|  Source |  Failed  |
|  Build  |  None    |
+---------+----------+


C:\Users\torjm>aws codepipeline get-pipeline-state --name logguardian-pipeline --query "stageStates[*].{Stage:stageName,Status:latestExecution.status}" --output table --region eu-west-1
----------------------
|  GetPipelineState  |
+---------+----------+
|  Stage  | Status   |
+---------+----------+
|  Source |  Failed  |
|  Build  |  None    |
+---------+----------+


C:\Users\torjm>aws codestar-connections list-connections --region eu-west-1 --query "Connections[?ConnectionName=='logguardian-github'].{Name:ConnectionName,Status:ConnectionStatus}" --output table
-------------------------------------
|          ListConnections          |
+---------------------+-------------+
|        Name         |   Status    |
+---------------------+-------------+
|  logguardian-github |  AVAILABLE  |
+---------------------+-------------+


C:\Users\torjm>aws codepipeline list-action-executions --pipeline-name logguardian-pipeline --region eu-west-1 --query "actionExecutionDetails[0].{Action:actionName,Status:status,Summary:output.executionResult.externalExecutionSummary}" --output json
{
    "Action": "GitHub-Source",
    "Status": "Failed",
    "Summary": "[GitHub] No Branch [develop] found for FullRepositoryName [WassimTorjmen/logguardian]"
}


C:\Users\torjm>aws codepipeline list-action-executions --pipeline-name logguardian-pipeline --region eu-west-1 --query "actionExecutionDetails[0].{Action:actionName,Status:status,Summary:output.executionResult.externalExecutionSummary}" --output json
{
    "Action": "GitHub-Source",
    "Status": "Failed",
    "Summary": "[GitHub] No Branch [develop] found for FullRepositoryName [WassimTorjmen/logguardian]"
}


C:\Users\torjm>aws codestar-connections list-connections --region eu-west-1 --query "Connections[?ConnectionName=='logguardian-github'].{Name:ConnectionName,Status:ConnectionStatus}" --output table
-------------------------------------
|          ListConnections          |
+---------------------+-------------+
|        Name         |   Status    |
+---------------------+-------------+
|  logguardian-github |  AVAILABLE  |
+---------------------+-------------+


C:\Users\torjm>aws codepipeline create-pipeline --cli-input-json file://C:\Users\torjm\pipeline.json --region eu-west-1
{
    "pipeline": {
        "name": "logguardian-pipeline",
        "roleArn": "arn:aws:iam::148761640356:role/logguardian-codepipeline-role",
        "artifactStore": {
            "type": "S3",
            "location": "logguardian-pipeline-artifacts-148761640356"
        },
        "stages": [
            {
                "name": "Source",
                "actions": [
                    {
                        "name": "GitHub-Source",
                        "actionTypeId": {
                            "category": "Source",
                            "owner": "AWS",
                            "provider": "CodeStarSourceConnection",
                            "version": "1"
                        },
                        "runOrder": 1,
                        "configuration": {
                            "BranchName": "develop",
                            "ConnectionArn": "arn:aws:codestar-connections:eu-west-1:148761640356:connection/7a00dbf3-1105-45ed-aba5-11c8e0166095",
                            "FullRepositoryId": "WassimTorjmen/logguardian",
                            "OutputArtifactFormat": "CODE_ZIP"
                        },
                        "outputArtifacts": [
                            {
                                "name": "SourceOutput"
                            }
                        ],
                        "inputArtifacts": []
                    }
                ]
            },
            {
                "name": "Build",
                "actions": [
                    {
                        "name": "Docker-Build",
                        "actionTypeId": {
                            "category": "Build",
                            "owner": "AWS",
                            "provider": "CodeBuild",
                            "version": "1"
                        },
                        "runOrder": 1,
                        "configuration": {
                            "ProjectName": "logguardian-build"
                        },
                        "outputArtifacts": [
                            {
                                "name": "BuildOutput"
                            }
                        ],
                        "inputArtifacts": [
                            {
                                "name": "SourceOutput"
                            }
                        ]
                    }
                ]
            }
        ],
        "version": 1,
        "executionMode": "SUPERSEDED",
        "pipelineType": "V2"
    }
}


C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>aws codepipeline get-pipeline-state --name logguardian-pipeline --query "stageStates[*].{Stage:stageName,Status:latestExecution.status}" --output table --region eu-west-1
----------------------
|  GetPipelineState  |
+---------+----------+
|  Stage  | Status   |
+---------+----------+
|  Source |  Failed  |
|  Build  |  None    |
+---------+----------+


C:\Users\torjm>aws codestar-connections delete-connection --connection-arn arn:aws:codestar-connections:eu-west-1:148761640356:connection/7a00dbf3-1105-45ed-aba5-11c8e0166095 --region eu-west-1

C:\Users\torjm>aws codestar-connections create-connection --provider-type GitHub --connection-name logguardian-github --region eu-west-1
{
    "ConnectionArn": "arn:aws:codestar-connections:eu-west-1:148761640356:connection/33ae3b47-cdeb-4b1c-a9ea-686f4d951f2a"
}


C:\Users\torjm>aws codestar-connections list-connections --region eu-west-1 --query "Connections[?ConnectionName=='logguardian-github'].{Status:ConnectionStatus,Arn:ConnectionArn}" --output table
-------------------------------------------------------------------------------------------------------------------
|                                                 ListConnections                                                 |
+--------+--------------------------------------------------------------------------------------------------------+
|  Arn   |  arn:aws:codestar-connections:eu-west-1:148761640356:connection/33ae3b47-cdeb-4b1c-a9ea-686f4d951f2a   |
|  Status|  AVAILABLE                                                                                             |
+--------+--------------------------------------------------------------------------------------------------------+


C:\Users\torjm>aws codepipeline get-pipeline --name logguardian-pipeline --region eu-west-1 > C:\Users\torjm\pipeline-current.json

C:\Users\torjm>aws codepipeline update-pipeline --cli-input-json file://C:\Users\torjm\pipeline-current.json --region eu-west-1
{
    "pipeline": {
        "name": "logguardian-pipeline",
        "roleArn": "arn:aws:iam::148761640356:role/logguardian-codepipeline-role",
        "artifactStore": {
            "type": "S3",
            "location": "logguardian-pipeline-artifacts-148761640356"
        },
        "stages": [
            {
                "name": "Source",
                "actions": [
                    {
                        "name": "GitHub-Source",
                        "actionTypeId": {
                            "category": "Source",
                            "owner": "AWS",
                            "provider": "CodeStarSourceConnection",
                            "version": "1"
                        },
                        "runOrder": 1,
                        "configuration": {
                            "BranchName": "develop",
                            "ConnectionArn": "arn:aws:codestar-connections:eu-west-1:148761640356:connection/33ae3b47-cdeb-4b1c-a9ea-686f4d951f2a",
                            "FullRepositoryId": "WassimTorjmen/logguardian",
                            "OutputArtifactFormat": "CODE_ZIP"
                        },
                        "outputArtifacts": [
                            {
                                "name": "SourceOutput"
                            }
                        ],
                        "inputArtifacts": []
                    }
                ]
            },
            {
                "name": "Build",
                "actions": [
                    {
                        "name": "Docker-Build",
                        "actionTypeId": {
                            "category": "Build",
                            "owner": "AWS",
                            "provider": "CodeBuild",
                            "version": "1"
                        },
                        "runOrder": 1,
                        "configuration": {
                            "ProjectName": "logguardian-build"
                        },
                        "outputArtifacts": [
                            {
                                "name": "BuildOutput"
                            }
                        ],
                        "inputArtifacts": [
                            {
                                "name": "SourceOutput"
                            }
                        ]
                    }
                ]
            }
        ],
        "version": 2,
        "executionMode": "SUPERSEDED",
        "pipelineType": "V2",
        "triggers": [
            {
                "providerType": "CodeStarSourceConnection",
                "gitConfiguration": {
                    "sourceActionName": "GitHub-Source",
                    "push": [
                        {
                            "branches": {
                                "includes": [
                                    "develop"
                                ]
                            }
                        }
                    ]
                }
            }
        ]
    }
}


C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>aws codepipeline start-pipeline-execution --name logguardian-pipeline --region eu-west-1
{
    "pipelineExecutionId": "71eed6d6-abb7-4791-81d9-bd97ddb2199a"
}


C:\Users\torjm>aws codepipeline get-pipeline-state --name logguardian-pipeline --query "stageStates[*].{Stage:stageName,Status:latestExecution.status}" --output table --region eu-west-1
--------------------------
|    GetPipelineState    |
+---------+--------------+
|  Stage  |   Status     |
+---------+--------------+
|  Source |  Succeeded   |
|  Build  |  InProgress  |
+---------+--------------+


C:\Users\torjm>aws codepipeline get-pipeline-state --name logguardian-pipeline --query "stageStates[*].{Stage:stageName,Status:latestExecution.status}" --output table --region eu-west-1
-------------------------
|   GetPipelineState    |
+---------+-------------+
|  Stage  |   Status    |
+---------+-------------+
|  Source |  Succeeded  |
|  Build  |  Failed     |
+---------+-------------+


C:\Users\torjm>aws codepipeline list-action-executions --pipeline-name logguardian-pipeline --region eu-west-1 --query "actionExecutionDetails[0].{Action:actionName,Status:status,Summary:output.executionResult.externalExecutionSummary}" --output json
{
    "Action": "Docker-Build",
    "Status": "Failed",
    "Summary": "Build terminated with state: FAILED. Phase: DOWNLOAD_SOURCE, Code: YAML_FILE_ERROR, Message: stat /codebuild/output/src980350499/src/buildspec.yml: no such file or directory"
}


C:\Users\torjm>aws ec2 describe-subnets --filters "Name=vpc-id,Values=TON_VPC_ID" "Name=map-public-ip-on-launch,Values=false" --query "Subnets[*].SubnetId" --output text --region eu-west-1

C:\Users\torjm>aws eks delete-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes --region eu-west-1

aws: [ERROR]: An error occurred (ResourceNotFoundException) when calling the DeleteNodegroup operation: nodeGroup logguardian-nodes not found for cluster logguardian

Additional error details:
clusterName: logguardian
nodegroupName: logguardian-nodes

C:\Users\torjm>aws eks delete-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes-lt --region eu-west-1
{
    "nodegroup": {
        "nodegroupName": "logguardian-nodes-lt",
        "nodegroupArn": "arn:aws:eks:eu-west-1:148761640356:nodegroup/logguardian/logguardian-nodes-lt/7ecedb81-3d6b-0ee6-4031-ccc555ba1bca",
        "clusterName": "logguardian",
        "version": "1.35",
        "releaseVersion": "1.35.3-20260415",
        "createdAt": "2026-04-22T23:04:47.772000+02:00",
        "modifiedAt": "2026-04-23T00:56:05.638000+02:00",
        "status": "DELETING",
        "capacityType": "ON_DEMAND",
        "scalingConfig": {
            "minSize": 1,
            "maxSize": 3,
            "desiredSize": 2
        },
        "instanceTypes": [
            "t3.medium"
        ],
        "subnets": [
            "subnet-044076265d5c4e5a7",
            "subnet-0a27aa6f1017701ad"
        ],
        "amiType": "AL2023_x86_64_STANDARD",
        "nodeRole": "arn:aws:iam::148761640356:role/logguardian-eks-node-role",
        "labels": {},
        "resources": {
            "autoScalingGroups": [
                {
                    "name": "eks-logguardian-nodes-lt-7ecedb81-3d6b-0ee6-4031-ccc555ba1bca"
                }
            ]
        },
        "diskSize": 20,
        "health": {
            "issues": [
                {
                    "code": "AccessDenied",
                    "message": "AccessEntry isn't found in the cluster",
                    "resourceIds": [
                        "arn:aws:iam::148761640356:role/logguardian-eks-node-role"
                    ]
                }
            ]
        },
        "updateConfig": {
            "maxUnavailable": 1,
            "updateStrategy": "DEFAULT"
        },
        "nodeRepairConfig": {
            "enabled": false
        },
        "tags": {}
    }
}


C:\Users\torjm>
C:\Users\torjm>
C:\Users\torjm>aws eks describe-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes --query "nodegroup.status" --output text --region eu-west-1

aws: [ERROR]: An error occurred (ResourceNotFoundException) when calling the DescribeNodegroup operation: No node group found for name: logguardian-nodes.

Additional error details:
clusterName: logguardian
nodegroupName: logguardian-nodes

C:\Users\torjm>aws eks describe-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes-lt --query "nodegroup.status" --output text --region eu-west-1
DELETING

