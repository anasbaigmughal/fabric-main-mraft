# A Study on the Adoption of Blockchain for IoT Devices in Supply Chain
Computational Intelligence and Neuroscience Journal / 2022 / Article<br/>
[Research Publication Link](https://doi.org/10.1155/2022/9228982)

*Muhammad Anas Baig,1 Danish Ali Sunny,2 Abdullah Alqahtani,3 Shtwai Alsubai,3 Adel Binbusayyis,3 and Muhammad Muzammal,4*<br/>
<sub>1 Department of Computer Science, Bahria University, Islamabad 44000, Pakistan</sub><br/>
<sub>2 Department of Applied Mathematics Statistics, Institute of Space Technology, Islamabad 44000, Pakistan</sub><br/>
<sub>3 College of Computer Engineering and Sciences, Prince Sattam bin Abdulaziz University, Al-Kharj 11942, Saudi Arabia</sub><br/>
<sub>4 Department of Software Engineering, Bahria University, H-11 Campus, Islamabad 44000, Pakistan</sub><br/>

### Abstract
The integration of blockchain and IoT enables promising solutions in decentralized environments in contrast with centralized systems. The blockchain brings forth features such as fault tolerance, security, and transparency of the data in IoT devices. As there is requirement of consensus among the network nodes to agree on a single-state-of-ledger, nonetheless, the extensive computational requirement for the consensus protocol becomes a limitation in resource-constrained IoT devices with limited battery, computation, and communication capabilities. This study proposes an empirical approach on the adoption of blockchain in a supply chain environment. Furthermore, a modified version of the Raft consensus protocol is proposed for use in supply-chain environment on the permissioned blockchain Hyperledger. In Raft consensus protocol, each transaction is directed to the leader node that transmits it to the follower nodes, making the leader node the bottleneck thus inhibiting the scalability and throughput of the system. This also results in high latency for the network. The modified RAFT consensus protocol (mRAFT) is based on the idea of utilizing the idle follower nodes in disseminating the vote requests and log replication messages. A detailed empirical evaluation of the solution built on Hyperledger Caliper is performed to demonstrate the applicability of the system. The improved workload division on the peers boosts the throughput and latency of the system in ordering service that enhances the overall efficiency of the system.

### Software and Hardware Environment
The system build and test environment are implemented using Linux virtual machine services. The Hyperledger Fabric network is built, run, and tested using the Raft and mRAFT consensus protocol. The environmental parameters for software and hardware are shown in table given below. The prototype solution for experimental evaluation and testing is based on a virtual server hosted on a laptop computing machine with limited computational resources and capabilities. It must be kept in mind that in the production environment, the blockchain nodes will be held on server machines with heavy computational capabilities whose performance could differ in terms of scale. However, the rationale behind the results would be the same.

![Software and Hardware Environment](swRequirements.PNG)
