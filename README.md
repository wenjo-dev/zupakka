# zupakka

a fork of **ddm-akka**

group:
- Daniel Engelbach
- Jonathan Wendt
- Desirée Wenk

## Requirements
- Java 9 >=
- Maven Compiler Version 3.1.8 >=

## Getting started
1. Clone repo
  ```
  git clone https://github.com/UMR-Big-Data-Analytics/ddm-akka.git
  ```
        
2. Decompress test data
  ```
  cd ddm-akka/data
  unzip TPCH.zip
  ```

3. Build project with maven
  ```
  cd ..
  mvn package
  ```

4. First run
  ```
  java -jar target/ddm-akka-1.0.jar master
  ```
