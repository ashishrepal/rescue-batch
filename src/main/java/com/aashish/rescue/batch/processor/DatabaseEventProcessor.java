package com.aashish.rescue.batch.processor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.modelmapper.ModelMapper;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.aashish.rescue.batch.constant.BatchConstant;
import com.aashish.rescue.batch.entity.BatchReport;
import com.aashish.rescue.batch.model.InOutDatabaseDetails;
import com.aashish.rescue.batch.util.BatchUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class DatabaseEventProcessor.
 * 
 * 
 */

@Slf4j
@Component
public class DatabaseEventProcessor implements ItemProcessor<InOutDatabaseDetails, BatchReport> {

	@Value("${mysql.bin.path}")
	private String mySqlBinPath;

	private BatchUtil batchUtil = new BatchUtil();

	@Override
	public BatchReport process(final InOutDatabaseDetails databaseDetails) throws Exception {
		Long startTime = System.nanoTime();
		log.info("batch start time is:" + startTime);
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
		String start = sdf.format(new Timestamp(System.currentTimeMillis()));
		validateInputData(databaseDetails);
		BatchReport report = new BatchReport();

		ModelMapper modelMapper = new ModelMapper();
		report = modelMapper.map(databaseDetails, BatchReport.class);
		captureReport(report);
		report.setBatchStartTime(start);
		String filePath = createFilePath(databaseDetails);
		String cmdToExecute = batchUtil.getBackupCommandToExecute(mySqlBinPath, databaseDetails, filePath);

		if (!StringUtils.isEmpty(cmdToExecute)) {
			if (backupDB(cmdToExecute,
					databaseDetails.getDbScriptFilePath() + "\\" + databaseDetails.getDbScriptFileName(), report)) {
				if (databaseDetails.getSchemaWithTimestamp().equalsIgnoreCase("Y")
						&& StringUtils.isEmpty(databaseDetails.getInDbTableNames())) {
					updateSchemaWithTimestamp(filePath, databaseDetails.getInDbSchema());
				}
				String[] restoreCmdToExecute = batchUtil.getRestoreCommandToExecute(mySqlBinPath, databaseDetails,
						filePath);
				restoreDB(restoreCmdToExecute, databaseDetails.getOutDbUsername(), databaseDetails.getOutDbPassword(),
						filePath, report);
			} else {
				log.error("Error occurred while taking database backup file:" + filePath);
				report.setStatus(BatchConstant.FAILED);
				report.setMessage("Error occurred while taking database backup file:" + filePath);
			}
		} else {
			log.error("Database type:" + databaseDetails.getInDbType() + "is not supported");
			report.setStatus(BatchConstant.FAILED);
			report.setMessage("Database type:" + databaseDetails.getInDbType() + "is not supported");
		}

		if (StringUtils.isEmpty(report.getStatus())) {
			if (verifyRestore(filePath, databaseDetails, report)) {
				log.info("Database restore last row and count matched");
				report.setStatus(BatchConstant.SUCCESS);
				report.setMessage("Database backup and restore process completed successfully");
			} else {
				log.info("Database restore last row and count does not match");
				report.setStatus(BatchConstant.FAILED);
				report.setMessage("Database restore last row and count does not match");
			}
		}
		Long endTime = System.nanoTime();
		String end = sdf.format(new Timestamp(System.currentTimeMillis()));
		report.setBatchEndTime(end);
		log.info("batch start time is:" + endTime);
		log.info("Total time taken by batch in millis:" + (endTime - startTime) / 1000000);
		return report;
	}

	private void captureReport(BatchReport report) throws UnknownHostException {
		InetAddress ipAddr = InetAddress.getLocalHost();
		report.setMachineIp(ipAddr.getHostAddress());
		report.setLoggedUserName(System.getProperty("user.name"));
	}

	private void updateSchemaWithTimestamp(String filePath, String schemaName) {

		try {
			final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
			String fileName = "`" + schemaName + "-" + sdf.format(new Timestamp(System.currentTimeMillis())) + "`";

			Path path = Paths.get(filePath);
			Stream<String> lines = Files.lines(path);
			List<String> replaced = lines.map(line -> line.replaceAll("`" + schemaName + "`", fileName))
					.collect(Collectors.toList());
			Files.write(path, replaced);
			lines.close();
			log.info("Find and Replace done updateSchemaWithTimestamp filePath:" + filePath + " schemaName:"
					+ schemaName);
			System.out.println("Find and Replace done!!!");
		} catch (IOException e) {
			log.error("Error occurred while updating the script");
		}
	}

	private void validateInputData(InOutDatabaseDetails databaseDetails) throws Exception {

		if (StringUtils.isEmpty(mySqlBinPath)) {
			log.error("Please set mySql Bin Path in application.properties file.");
			throw new Exception("mySql Bin Path is not set in application.properties file");
		}
		if (StringUtils.isEmpty(databaseDetails.getDbScriptFilePath())) {
			log.error("Please set DB Script file path in inout xml");
			throw new Exception("DB Script file path not set in inout xml");
		}
		if (StringUtils.isEmpty(databaseDetails.getDbScriptFileName())) {
			log.error("Please set DB Script file name in inout xml");
			throw new Exception("DB Script file name not set in inout xml");
		}

	}

	private boolean verifyRestore(String filePath, InOutDatabaseDetails databaseDetails, BatchReport report) {
		if (!StringUtils.isEmpty(databaseDetails.getVerifyLastRowTableNames())) {
			Connection inputDbCon = getConnectionObject(databaseDetails, "I");
			Connection outputDbCon = getConnectionObject(databaseDetails, "O");

			boolean result2 = false;

			boolean result1 = verifyLastRowOfTables(databaseDetails.getVerifyLastRowTableNames(), report, inputDbCon,
					outputDbCon);
			if (result1) {
				log.info("Database restore last row matched");
				report.setStatus(BatchConstant.SUCCESS);
				report.setMessage("Database restore last row matched");
			} else {
				log.info("Database restore last row do not match");
				report.setStatus(BatchConstant.SUCCESS);
				report.setMessage("Database restore last do not row match");
			}
			result2 = verifyRowsCountOfTables(databaseDetails.getVerifyNoOfRowsTableNames(), report, inputDbCon,
					outputDbCon);
			if (result2) {
				log.info("Database restore count matched");
				report.setStatus(BatchConstant.SUCCESS);
				report.setMessage("Database restore count matched");
			} else {
				log.info("Database restore count do not match");
				report.setStatus(BatchConstant.SUCCESS);
				report.setMessage("Database restore count do not match");
			}
			closeConnection(inputDbCon, outputDbCon);
			return result2;

		} else {
			return true;
		}
	}

	private void closeConnection(Connection inputDbCon, Connection outputDbCon) {
		try {
			inputDbCon.close();
			outputDbCon.close();
		} catch (SQLException e) {
			log.error("sql exception occured in closing the database connection", e);
		}
	}

	private boolean verifyRowsCountOfTables(String verifyNoOfRowsTableNames, BatchReport report, Connection inputDbCon,
			Connection outputDbCon) {
		if (!StringUtils.isEmpty(verifyNoOfRowsTableNames)) {
			List<String> tableList = Arrays.asList(verifyNoOfRowsTableNames.split(","));
			for (String table : tableList) {
				if (!verifyRowsCount(table, inputDbCon, outputDbCon)) {
					report.setStatus(BatchConstant.FAILED);
					report.setMessage("Last row record does not match");
					return false;
				}
			}
		}
		return true;
	}

	private boolean verifyRowsCount(String table, Connection inputDbCon, Connection outputDbCon) {

		ResultSet inputRs = null;
		ResultSet outputRs = null;
		boolean result = false;
		try {
			Statement inputStmt = inputDbCon.createStatement();
			Statement outputStmt = outputDbCon.createStatement();
			inputRs = inputStmt.executeQuery("select count(*) from " + table);
			inputRs.next();
			int inputRows = inputRs.getInt(1);
			outputRs = outputStmt.executeQuery("select count(*) from " + table);
			outputRs.next();
			int outputRows = outputRs.getInt(1);

			result = inputRows == outputRows ? true : false;
		} catch (SQLException e) {
			log.error("sql exception occured in verifyLastRows", e);
			try {
				inputRs.close();
				outputRs.close();
			} catch (SQLException e1) {
				log.error("sql exception occured in closing the database connection", e1);
			}

		} finally {
			try {
				inputRs.close();
				outputRs.close();
			} catch (SQLException e) {
				log.error("sql exception occured in closing the database connection", e);
			}

		}
		return result;
	}

	private Connection getConnectionObject(InOutDatabaseDetails databaseDetails, String type) {
		Connection con = null;
		if (databaseDetails.getInDbType().equalsIgnoreCase(BatchConstant.MYSQL)) {
			try {
				Class.forName(BatchConstant.MYSQL_DRIVER);
			} catch (ClassNotFoundException e) {
				log.error("Error occured in getConnectionObject type:" + type, e);
			}
			try {
				if (type.equals("I")) {
					con = DriverManager.getConnection(
							"jdbc:mysql://" + databaseDetails.getDbInHostIpAddress() + ":"
									+ databaseDetails.getDbInHostPort() + "/" + databaseDetails.getInDbSchema(),
							databaseDetails.getInDbUsername(), databaseDetails.getInDbPassword());
				} else if (type.equals("O")) {
					con = DriverManager.getConnection(
							"jdbc:mysql://" + databaseDetails.getDbOutHostIpAddress() + ":"
									+ databaseDetails.getDbOutHostPort() + "/" + databaseDetails.getOutDbSchema(),
							databaseDetails.getOutDbUsername(), databaseDetails.getOutDbPassword());
				}
			} catch (SQLException e) {
				log.error("Error occured in getConnectionObject type:" + type, e);
			}
		}
		return con;
	}

	private boolean verifyLastRowOfTables(String verifyLastRowTableNames, BatchReport report, Connection inputDbCon,
			Connection outputDbCon) {

		Map<String, Map<String, String>> tableColumnMap = new HashMap<String, Map<String, String>>();
		Map<String, String> columnDataMap = new HashMap<String, String>();
		if (!StringUtils.isEmpty(verifyLastRowTableNames)) {
			if (verifyLastRowTableNames.contains("$")) {
				String tableData[] = verifyLastRowTableNames.split("$");
				for (int i = 0; i < tableData.length; i++) {
					columnDataMap = new HashMap<String, String>();
					String tableString = tableData[i];
					if (tableString.contains("#")) {
						String tableAndCols[] = tableString.split("#");
						String colData = tableAndCols[1];
						if (colData.contains("|")) {
							String columns[] = colData.split("|");
							for (int j = 0; j < columns.length; j++) {
								String column = columns[j];
								columnDataMap.put(column.split(":")[0], column.split(":")[1]);
							}
						} else {
							if (colData.contains(":")) {
								columnDataMap.put(colData.split(":")[0], colData.split(":")[1]);
							} else {
								log.error("Please specify column names with its data type in proper format.");
								return false;
							}
						}
						tableColumnMap.put(tableData[0], columnDataMap);
					} else {
						log.error("Please specify column names with its data type in proper format.");
						return false;
					}
				}
			} else {
				if (verifyLastRowTableNames.contains("#")) {
					String tableAndCols[] = verifyLastRowTableNames.split("#");
					String colData = tableAndCols[1];
					if (colData.contains("|")) {
						String columns[] = colData.split("\\|");
						for (int j = 0; j < columns.length; j++) {
							String column = columns[j];
							columnDataMap.put(column.split(":")[0], column.split(":")[1]);
						}
					} else {
						if (colData.contains(":")) {
							columnDataMap.put(colData.split(":")[0], colData.split(":")[1]);
						} else {
							log.error("Please specify column names with its data type in proper format.");
							return false;
						}
					}
					tableColumnMap.put(tableAndCols[0], columnDataMap);
				} else {
					log.error("Please specify Table Name and column names with its data type in proper format.");
					return false;
				}
			}
		}
		Set<String> tableKeys = tableColumnMap.keySet();
		Iterator<String> itr = tableKeys.iterator();
		while (itr.hasNext()) {
			String tableName = (String) itr.next();
			Map<String, String> colMap = tableColumnMap.get(tableName);
			verifyLastRows(tableName, colMap, inputDbCon, outputDbCon);
		}
		/*
		 * for(String table:tableList){
		 * if(!verifyLastRows(table,inputDbCon,outputDbCon)){
		 * report.setStatus(BatchConstant.FAILED); report.setMessage(
		 * "Last row record does not match"); return false; } }
		 */

		return true;
	}

	private boolean verifyLastRows(String table, Map<String, String> colMap, Connection inputDbCon,
			Connection outputDbCon) {
		ResultSetMetaData inputMetaData = null;
		ResultSetMetaData outputMetaData = null;
		ResultSet inputRs = null;
		ResultSet outputRs = null;
		boolean result = false;
		try {
			Statement inputStmt = inputDbCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			Statement outputStmt = outputDbCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			inputRs = inputStmt.executeQuery("select * from " + table);
			outputRs = outputStmt.executeQuery("select * from " + table);

			inputMetaData = inputRs.getMetaData();
			outputMetaData = outputRs.getMetaData();
			result = compareMetaData(inputMetaData, outputMetaData, inputRs, outputRs, colMap);
		} catch (SQLException e) {
			log.error("sql exception occured in verifyLastRows", e);
			try {
				if (!StringUtils.isEmpty(inputRs))
					inputRs.close();
				if (!StringUtils.isEmpty(outputRs))
					outputRs.close();
			} catch (SQLException e1) {
				log.error("sql exception occured in closing the database connection", e1);
			}

		}
		return result;
	}

	private boolean compareMetaData(ResultSetMetaData inputMetaData, ResultSetMetaData outputMetaData,
			ResultSet inputRs, ResultSet outputRs, Map<String, String> colMap) {

		if (inputMetaData != null && outputMetaData != null) {
			try {
				inputRs.afterLast();
				outputRs.afterLast();
				inputRs.previous();
				outputRs.previous();

				Set<String> colNameSet = colMap.keySet();
				Iterator<String> itr = colNameSet.iterator();
				while (itr.hasNext()) {
					String colName = (String) itr.next();
					Object obj1 = inputRs.getObject(colName);
					if (!inputRs.getObject(colName).toString()
							.equalsIgnoreCase(outputRs.getObject(colName).toString())) {
						return false;
					}
				}

			} catch (SQLException e) {
				log.error("sql exception occured in compareMetaData", e);
				return false;
			}
		} else {
			return false;
		}
		return true;
	}

	private boolean verifyLastRows(String table, Connection inputDbCon, Connection outputDbCon) {
		ResultSetMetaData inputMetaData = null;
		ResultSetMetaData outputMetaData = null;
		ResultSet inputRs = null;
		ResultSet outputRs = null;
		boolean result = false;
		try {
			Statement inputStmt = inputDbCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			Statement outputStmt = outputDbCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			inputRs = inputStmt.executeQuery("select * from " + table);
			outputRs = outputStmt.executeQuery("select * from " + table);

			inputMetaData = inputRs.getMetaData();
			outputMetaData = outputRs.getMetaData();
			result = compareMetaData(inputMetaData, outputMetaData, inputRs, outputRs);
		} catch (SQLException e) {
			log.error("sql exception occured in verifyLastRows", e);
			try {
				if (!StringUtils.isEmpty(inputRs))
					inputRs.close();
				if (!StringUtils.isEmpty(outputRs))
					outputRs.close();
			} catch (SQLException e1) {
				log.error("sql exception occured in closing the database connection", e1);
			}

		}
		return result;

	}

	private boolean compareMetaData(ResultSetMetaData inputMetaData, ResultSetMetaData outputMetaData,
			ResultSet inputRs, ResultSet outputRs) {
		if (inputMetaData != null && outputMetaData != null) {
			try {
				inputRs.afterLast();
				outputRs.afterLast();
				List<String> inputRow = new ArrayList<String>();
				List<String> outputRow = new ArrayList<String>();
				while (inputRs.previous() && outputRs.previous()) {
					for (int i = 1; i <= inputMetaData.getColumnCount(); i++) {
						if (inputMetaData.getColumnTypeName(i).equalsIgnoreCase("VARCHAR")) {
							if ((!StringUtils.isEmpty(inputRs.getString(inputMetaData.getColumnName(i)))
									&& (!StringUtils.isEmpty(outputRs.getString(inputMetaData.getColumnName(i))))))
								if (!inputRs.getString(inputMetaData.getColumnName(i))
										.equalsIgnoreCase(outputRs.getString(inputMetaData.getColumnName(i)))) {
									return false;
								}
						} else if (inputMetaData.getColumnTypeName(i).equalsIgnoreCase("INT")) {
							// null check add
							if (!(inputRs.getInt(inputMetaData.getColumnName(i)) == outputRs
									.getInt(inputMetaData.getColumnName(i)))) {
								return false;
							}
						}
						/*
						 * else if(inputMetaData.getColumnTypeName(i).
						 * equalsIgnoreCase("BIGINT")){
						 * if(!inputRs.get(inputMetaData.getColumnName(i)).
						 * equalsIgnoreCase(outputRs.getString(inputMetaData.
						 * getColumnName(i)))){ return false; } }
						 */
						else if (inputMetaData.getColumnTypeName(i).equalsIgnoreCase("bigdecimal")) {
							if (!(inputRs.getBigDecimal(inputMetaData.getColumnName(i)) == outputRs
									.getBigDecimal(inputMetaData.getColumnName(i)))) {
								return false;
							}
						}

					}

				}

			} catch (SQLException e) {
				log.error("sql exception occured in compareMetaData", e);
				return false;
			}
		} else {
			return false;
		}
		return true;
	}

	private String createFilePath(InOutDatabaseDetails databaseDetails) {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
		String fileName = sdf.format(new Timestamp(System.currentTimeMillis())) + ".sql";
		String filePath = databaseDetails.getDbScriptFilePath() + "\\" + databaseDetails.getDbScriptFileName()
				+ fileName;
		databaseDetails.setDbScriptFileName(fileName);
		return filePath;
	}

	private boolean backupDB(String executeCmd, String path, BatchReport report) {
		log.info("backup command:" + executeCmd);
		Process runtimeProcess;
		try {
			runtimeProcess = Runtime.getRuntime().exec(executeCmd);
			int processComplete = runtimeProcess.waitFor();

			if (processComplete == 0) {
				log.info("Backup created successfully file:" + path);
				System.out.println("Backup created successfully");
				return true;
			} else {
				// get the error stream of the process and print it
				String errorMsg = "";
				InputStream error = runtimeProcess.getErrorStream();
				for (int i = 0; i < error.available(); i++) {
					errorMsg += (char) error.read();
				}
				log.info("Error occurred while creating backup file:" + errorMsg);
				System.out.println("Could not create the backup");
				report.setStatus(BatchConstant.FAILED);
				report.setMessage("Error occurred while creating backup file:" + errorMsg);
				return false;
			}
		} catch (Exception ex) {
			log.info("Error occurred while creating backup file:" + ex);
			report.setStatus(BatchConstant.FAILED);
			report.setMessage("Error occurred while creating backup file:" + path);
		}

		return false;
	}

	private boolean restoreDB(String[] restoreCmdToExecute, String dbUserName, String dbPassword, String filePath,
			BatchReport report) {
		log.info("restore command executed");
		try {

			ProcessBuilder processBuilder = new ProcessBuilder(Arrays.asList(restoreCmdToExecute));
			processBuilder.redirectError(Redirect.INHERIT);
			processBuilder.redirectInput(Redirect.from(new File(filePath)));

			Process process = processBuilder.start();
			int processComplete = process.waitFor();
			if (processComplete == 0) {
				log.info("Database restored successfully file:" + filePath);
				System.out.println("Backup restored successfully");
				return true;
			} else {
				String errorMsg = "";
				InputStream error = process.getErrorStream();
				for (int i = 0; i < error.available(); i++) {
					errorMsg += (char) error.read();
				}

				log.error("Error occurred while restoring datbase file:" + filePath + " Message:" + errorMsg);
				report.setStatus(BatchConstant.FAILED);
				report.setMessage("Error occurred while restoring datbase file:" + filePath);
				System.out.println("Could not restore the backup");
			}
		} catch (Exception ex) {
			log.error("Error occurred while restoring datbase file:" + filePath, ex);
			report.setStatus(BatchConstant.FAILED);
			report.setMessage("Error occurred while restoring datbase file:" + filePath);
		}

		return false;
	}

}
