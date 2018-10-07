package com.aashish.rescue.batch.model;

import javax.xml.bind.annotation.XmlRootElement;

import org.springframework.util.Base64Utils;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@XmlRootElement(name="databaseDetails")
public class InOutDatabaseDetails {

	String dbInHostIpAddress;
	String dbOutHostIpAddress;
	String dbInHostPort;
	String dbOutHostPort;
	String inDbName;
	String outDbName;
	String inDbType;
	String outDbType;
	String inDbSchema;
	String outDbSchema;
	String inDbUsername;
	String outDbUsername;
	String inDbPassword;
	String outDbPassword;
	String inExtraCommands;
	String outExtraCommands;
	String inDbTableNames;
	String outDbTableNames;
	String dbScriptFileName;
	String dbScriptFilePath;
	String verifyLastRowTableNames;
	String verifyNoOfRowsTableNames;
    String emailIds;
    String schemaWithTimestamp;
	
    public String getInDbPassword(){
    	byte[] decoder = Base64Utils.decodeFromString(inDbPassword);
		String decodedPassword = new String(decoder);
    	return decodedPassword;
    }
    
    public String getOutDbPassword(){
    	byte[] decoder = Base64Utils.decodeFromString(outDbPassword);
		String decodedPassword = new String(decoder);
    	return decodedPassword;
    }
}
