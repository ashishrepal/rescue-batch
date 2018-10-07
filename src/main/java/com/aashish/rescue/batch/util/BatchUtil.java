package com.aashish.rescue.batch.util;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.aashish.rescue.batch.constant.BatchConstant;
import com.aashish.rescue.batch.model.InOutDatabaseDetails;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class BatchUtil {

	public String getBackupCommandToExecute(String mySqlBinPath, InOutDatabaseDetails databaseDetails,String filePath) throws Exception {
		
		Boolean isFullBackup = isFullDBBackup(databaseDetails);
		if(isFullBackup){
			log.info("Full backup request");
			if(databaseDetails.getDbInHostIpAddress().equals(databaseDetails.getDbOutHostIpAddress()) && databaseDetails.getDbInHostPort().equals(databaseDetails.getDbOutHostPort())){
				throw new Exception("Full backup cannot be taken on same server and port");
			}
			databaseDetails.setOutDbSchema(databaseDetails.getInDbSchema());
			return getFullDBBackupCommand( mySqlBinPath,  databaseDetails, filePath);
		}else{
			log.info("Partial backup request");
			return getPartialDBBackupCommand(mySqlBinPath,  databaseDetails, filePath);
		}
		
	}

	private String getPartialDBBackupCommand(String mySqlBinPath, InOutDatabaseDetails databaseDetails,
			String filePath) {
		String tables = getTableNames(databaseDetails.getInDbTableNames());
		if (databaseDetails.getInDbType().equalsIgnoreCase(BatchConstant.MYSQL)) {
			return mySqlBinPath + "\\mysqldump -h "+databaseDetails.getDbInHostIpAddress()+" -u " + databaseDetails.getInDbUsername() + " -p"
					+ databaseDetails.getInDbPassword() + " " + databaseDetails.getInDbSchema() +tables
					+ " -r " + filePath;
		} else
			return null;
	}

	private String getTableNames(String inDbTableNames) {
		
		String tableList="";
		String tables[] = inDbTableNames.split(",");
		for(int i=0;i<tables.length;i++){
			tableList = tableList +" "+ tables[i];
		}
		return tableList;
	}

	private String getFullDBBackupCommand(String mySqlBinPath, InOutDatabaseDetails databaseDetails, String filePath) {
		if (databaseDetails.getInDbType().equalsIgnoreCase(BatchConstant.MYSQL)) {
			return mySqlBinPath + "\\mysqldump -h "+databaseDetails.getDbInHostIpAddress()+" -u " + databaseDetails.getInDbUsername() + " -p"
					+ databaseDetails.getInDbPassword() + " --add-drop-database -B " + databaseDetails.getInDbSchema()
					+ " -r " + filePath;
		} else
			return null;
	}

	private Boolean isFullDBBackup(InOutDatabaseDetails databaseDetails) {
		if(StringUtils.isEmpty(databaseDetails.getInDbTableNames())){
			return true;
		}else{
			return false;
		}
	}

	public String[] getRestoreCommandToExecute(String mySqlBinPath, InOutDatabaseDetails databaseDetails,
			String filePath) {
		
        if (databaseDetails.getInDbType().equalsIgnoreCase(BatchConstant.MYSQL)) {
			/*return mySqlBinPath + "\\mysql -u " + databaseDetails.getOutDbUsername() + " -p"
					+ databaseDetails.getOutDbPassword() + " -h "+databaseDetails.getDbOutHostIpAddress()+" < " + filePath;*/
			return  new String[]{"mysql ", "-u"+databaseDetails.getOutDbUsername(), "-p"+databaseDetails.getOutDbPassword(),"-h"+databaseDetails.getDbOutHostIpAddress(), databaseDetails.getOutDbSchema()};
					
		} else
			return null;
	}

}
