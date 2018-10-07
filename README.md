Rescue Batch Process

----------------------------------------------

Batch process to take backup and restore the database. 
It takes the input as xml file which has below tags:
sample xml:

-------------------------------------------

<databaseDetails>
	<dbInHostIpAddress>127.0.0.1</dbInHostIpAddress>
	<dbOutHostIpAddress>127.0.0.1</dbOutHostIpAddress>
	<dbInHostPort>3306</dbInHostPort>
	<dbOutHostPort>3306</dbOutHostPort>
	<inDbType>mysql</inDbType>
	<outDbType>mysql</outDbType>
	<inDbSchema>test</inDbSchema>
	<outDbSchema>test</outDbSchema> <!--This should be same as inDbSchema -->
	<inDbUsername>root</inDbUsername>
	<outDbUsername>root</outDbUsername>
	<inDbPassword>govind</inDbPassword>
	<outDbPassword>govind</outDbPassword>
	<inExtraCommands></inExtraCommands>
	<outExtraCommands></outExtraCommands>
	<dbScriptFileName>script</dbScriptFileName>
	<dbScriptFilePath>D:\output</dbScriptFilePath>
	<inDbTableNames>table1,table2,table3</inDbTableNames>
	<outDbTableNames>table1,table2,table3</outDbTableNames>
	<verifyLastRowTableNames>table5,table6</verifyLastRowTableNames>
	<verifyNoOfRowsTableNames>table6,table7</verifyNoOfRowsTableNames>
</databaseDetails>

------------------------------------------------
setup application.properties file
staging.directory : directory path of input xml files
mysql.bin.path : Bin folder path of mysql database
spring.datasource.url : datasource for rescue batch

-------------------

Job scheduling:

provide cron expression in application.properties to schedule a batch job

-----------------------

How to compile and run:

compile:
mvn clean install

run:

mvn spring-boot:run
OR
mvn package && java -jar target/rescue-batch-0.1.0.jar


how to compile and run
