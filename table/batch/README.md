# batch
This module includes most basic batch Table API demos. 

**contents**
- [Quick Start](#quick-start)
  + [Setup](#setup)
    + [Requirements](#requirements)
    + [Install python3](#install-python3)
    + [Install pip](#install-pip)
    + [Install java 8](#install-java-8)
    + [Install maven](#install-maven)
  + [Build PyFlink](#build-pyflink)
  + [Install Dependency](#install-dependency)
  + [Run Demo](#run-demo)
    + [[optional] Importing the project on PyCharm](#optionalimporting-the-project-on-pycharm)

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
    ```

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

Next go to the root directory of flink source code and run this command to build the sdist package and wheel package:

```shell
cd flink-python; python3 setup.py sdist bdist_wheel
```

The sdist and wheel package will be found under `./flink-python/dist/`. Either of them could be used for pip installation, such as:

```shell
pip install dist/*.tar.gz
```

### Install Dependency
Install environment dependency

```shell
pip install -r requirements.txt
```

### Run demo
#### [optional]Importing the project on PyCharm
You can use PyCharm to open the project and choose the python interpreter as the python which match the pip tool which install the pyflink and dependency in requirements.txt.
The following documentation describes the steps to setup PyCharm 2019.1.3 ([https://www.jetbrains.com/pycharm/download/](https://www.jetbrains.com/pycharm/download/))

If you are in the PyCharm startup interface:
1. Start PyCharm and choose "Open"
2. Select the pyflink-demo cloned repository.
3. Click on System interpreter in python interpreter option(Pycharm->Preference->python interpreter).
4. Choose the python which have installed the packages of pyflink and dependencies in the requirements.txt

If you have used PyCharm to open a project:
1. Select "File -> Open"
2. Select the pyflink-demo cloned repository.
3. Click on System interpreter in python interpreter option(Pycharm->Preference->python interpreter).
4. Choose the python which have installed the packages of pyflink and dependencies in the requirements.txt
