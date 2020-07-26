#!/bin/bash
printf "\ncalling mvn package\n"
mvn package
printf "starting servers in background, gateway is zero, followers numbered 2 through 8\n"
java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver 0 & 
java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver 8 &
leader=$!
java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver 7 &
java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver 6 &
java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver 5 &
follower5=$!
java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver 4 &
java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver 3 &
java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.stage4.Driver 2 &
sleep 10
curl -s http://localhost:9002/roles
sleep 10
printf "\nsending nine requests:\n"
printf "\n request 1: public class HelloWorld { public void run() { System.out.println(\"Hello, World1\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World1\"); } } "
printf "\nrequest 2: public class HelloWorld { public void run() { System.out.println(\"Hello, World2\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World2\"); } } "
printf "\nrequest 3: public class HelloWorld { public void run() { System.out.println(\"Hello, World3\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World3\"); } } "
printf "\nrequest 4: public class HelloWorld { public void run() { System.out.println(\"Hello, World4\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World4\"); } } "
printf "\nrequest 5: public class HelloWorld { public void run() { System.out.println(\"Hello, World5\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World5\"); } } "
printf "\nrequest 6: public class HelloWorld { public void run() { System.out.println(\"Hello, World6\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World6\"); } } "
printf "\nrequest 7: public class HelloWorld { public void run() { System.out.println(\"Hello, World7\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World7\"); } } "
printf "\nrequest 8: public class HelloWorld { public void run() { System.out.println(\"Hello, World8\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World8\"); } } "
printf "\nrequest 9: public class HelloWorld { public void run() { System.out.println(\"Hello, World9\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World9\"); } } "
printf "\nkilling follower with id 5\n"
kill -9 $follower5
sleep 10
curl -s http://localhost:9002/roles
printf "\n killing leader\n"
kill -9  $leader
sleep 10
printf "\n sending  nine requests in background\n"
printf "\n request 1: public class HelloWorld { public void run() { System.out.println(\"Hello, World1\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World1\"); } } " &
background1=$!
printf "\nrequest 2: public class HelloWorld { public void run() { System.out.println(\"Hello, World2\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World2\"); } } " &
background2=$!
printf "\nrequest 3: public class HelloWorld { public void run() { System.out.println(\"Hello, World3\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World3\"); } } " &
background3=$!
printf "\nrequest 4: public class HelloWorld { public void run() { System.out.println(\"Hello, World4\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World4\"); } } " &
background4=$!
printf "\nrequest 5: public class HelloWorld { public void run() { System.out.println(\"Hello, World5\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World5\"); } } " &
background5=$!
printf "\nrequest 6: public class HelloWorld { public void run() { System.out.println(\"Hello, World6\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World6\"); } } " &
background6=$!
printf "\nrequest 7: public class HelloWorld { public void run() { System.out.println(\"Hello, World7\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World7\"); } } " &
background7=$!
printf "\nrequest 8: public class HelloWorld { public void run() { System.out.println(\"Hello, World8\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World8\"); } } " &
background8=$!
printf "\nrequest 9: public class HelloWorld { public void run() { System.out.println(\"Hello, World9\"); } }\n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World9\"); } } " &
background9=$!
printf "\nwaiting for background request pids\n"
wait $background1
wait $background2
wait $background3
wait $background4
wait $background5
wait $background6
wait $background7
wait $background8
wait $background9
printf "getting gateway's list of roles form curl\n"
curl -s http://localhost:9002/roles
printf "\npublic class HelloWorld { public void run() { System.out.println(\"Hello, World post leader death1\"); } } \n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World post leader death1\"); } } "
printf "\npublic class HelloWorld { public void run() { System.out.println(\"Hello, World post leader death2\"); } } \n"
curl -s http://localhost:9010/compileandrun -d "public class HelloWorld { public void run() { System.out.println(\"Hello, World post leader death2\"); } } "
printf "\n getting gossip from curl for server 0:\n"
curl -s http://localhost:9001/gossip
printf "\n getting gossip from curl for server 2:\n"
curl -s http://localhost:8021/gossip
printf "\n getting gossip from curl for server 3:\n"
curl -s http://localhost:8031/gossip
printf "\n getting gossip from curl for server 4:\n"
curl -s http://localhost:8041/gossip
printf "\n getting gossip from curl for server 6:\n"
curl -s http://localhost:8061/gossip
printf "\ngetting gossip for server 7:\n"
curl -s http://localhost:8071/gossip
printf "\nthat's all, folks"


killall java

