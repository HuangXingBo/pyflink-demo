# category_count_demo
This demo use kafka as source connector, mysql as sink connector and use python udf in logic.

**contents**

- [Quick Start](#quick-start)
  + [Setup](#setup)
    + [Requirements](#requirements)
    + [Install python3](#install-python3)
    + [Install pip](#install-pip)
    + [Install java 8](#install-java-8)
    + [Install maven](#install-maven)
  + [Build PyFlink](#build-pyflink)
  + [Prepare Kafka](#prepare-kafka)
  + [Prepare MySQL](#prepare-mysql)
  + [Install Dependency](#install-dependency)
  + [Prepare Data](#prepare-data)
  + [Run Demo](#run-the-demo)
    + [See the result](#see-the-result)

## Quick Start

### Setup

#### Requirements
1. python3
2. pip
3. java 1.8
4. maven version >=3.3.0

#### Install python3

macOS
```shell
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
export PATH="/usr/local/bin:/usr/local/sbin:$PATH"
brew install python3 
```
Ubuntu
- Ubuntu 17.10, Ubuntu 18.04(and above)
    + come with Python 3.6 by default. You should be able to invoke it with the command python3.
- Ubuntu 16.10 and 17.04
    ```shell
    sudo apt-get update
    sudo apt-get install python3.6
    ```
- Ubuntu 14.04 or 16.04
    ```shell
    sudo add-apt-repository ppa:deadsnakes/ppa
    sudo apt-get update
    sudo apt-get install python3.6

#### Install pip

```shell 
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py
```

#### Install java 8

[java download page](http://www.oracle.com/technetwork/java/javase/downloads/index.html)

#### Install maven

maven version >=3.3.0

[download maven page](http://maven.apache.org/download.cgi)

```shell
tar -xvf apache-maven-3.6.1-bin.tar.gz
mv -rf apache-maven-3.6.1 /usr/local/
```
configuration environment variables
```shell
MAVEN_HOME=/usr/local/apache-maven-3.6.1
export MAVEN_HOME
export PATH=${PATH}:${MAVEN_HOME}/bin
```


### Build PyFlink

If you want to build a PyFlink package that can be used for pip installation, you need to build Flink jars first, as described in https://ci.apache.org/projects/flink/flink-docs-master/flinkDev/building.html

```shell
mvn clean install -DskipTests -Dfast
```

Then you need to copy the jar package flink-sql-connector-kafka-0.11_*-SNAPSHOT.jar in the directory of flink-connectors/flink-sql-connector-kafka-0.11

```shell
cp flink-connectors/flink-sql-connector-kafka-0.11/target/flink-sql-connector-kafka-0.11_*-SNAPSHOT.jar build-target/lib
```

Then you need to copy the jar package flink-jdbc_*-SNAPSHOT.jar in the directory of flink-connectors/flink-jdbc

```shell
cp flink-connectors/flink-jdbc/target/flink-jdbc_*-SNAPSHOT.jar build-target/lib
```

Next you need to copy the jar package flink-json-*-SNAPSHOT-sql-jar.jar in the directory of flink-formats/flink-json

```shell
cp flink-formats/flink-json/target/flink-json-*-SNAPSHOT-sql-jar.jar build-target/lib
```

Next go to the root directory of flink source code and run this command to build the sdist package and wheel package:

```shell
cd flink-python; python3 setup.py sdist bdist_wheel
```

The sdist and wheel package will be found under `./flink-python/dist/`. Either of them could be used for pip installation, such as:

```shell
pip install dist/*.tar.gz
```

### Prepare Kafka
Some demo choose kafka as source, so you need to install and run kafka in local host. the version we use [kafka_2.11-0.11](https://archive.apache.org/dist/kafka/0.11.0.3/kafka_2.11-0.11.0.3.tgz)
you use the following command to download:

```shell
wget https://archive.apache.org/dist/kafka/0.11.0.3/kafka_2.11-0.11.0.3.tgz
```

Then you depress the tar package:

```shell
tar zxvf kafka_2.11-0.11.0.3.tgz
```

### Prepare Mysql
the category_count_demo need a upsert sink connector and I choose the mysql, so you need to install mysql and put corresponding java jar
into Python directory of site-package/pyflink/lib.

First Install [MySQL](https://dev.mysql.com/downloads/mysql/)
The version of mysql I choose 8.0.18.The installation progress you can refer to https://dev.mysql.com/doc/mysql-osx-excerpt/5.7/en/osx-installation-pkg.html

Then download mysql-connector java jar
```shell
wget http://central.maven.org/maven2/mysql/mysql-connector-java/8.0.18/mysql-connector-java-8.0.18.jar
```

Then put the jar into the Python directory of site-package/pyflink/lib

Next, you should create a database flink_test and create a table sales_volume_table

```mysql
create table sales_volume_table(startTime TIMESTAMP,endTime TIMESTAMP,category_id bigint,sales_volume bigint)

```
NOTE: you should change .property("connector.username", "root") and property("connector.password", "xxtxxthmhxb0643") 
in category_count_demo.py to your mysql username and password.

### Install Dependency
Install environment dependency

```shell
pip install -r requirements.txt
```

### Prepare Data
First you need to replace the variable KAFKA_DIR in file env.sh with your installed KAFKA binary directory, for example in my env.sh:

```shell
KAFKA_DIR=/Users/duanchen/Applications/kafka_2.11-0.11.0.3
```

Next, you need to source the create_data.sh

```shell
source create_data.sh
```

Next, you can start kafka

```shell
start_kafka
```

Next, you can create the topic which will be used in our demo

```shell
create_kafka_topic 1 10 user_behavior
```

Finally, you can send message to to the topic user_behavior

```shell
send_message user_behavior user_behavior.log
```

## Run The Demo
The demo code in category_count_demo.py, you can directly run the code

### See the result
you can see the result in the mysql terminal:

```mysql
select * from sales_volume_table;
```
