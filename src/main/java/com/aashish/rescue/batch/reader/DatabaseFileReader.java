package com.aashish.rescue.batch.reader;

import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourceArrayPropertyEditor;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.aashish.rescue.batch.model.InOutDatabaseDetails;

import lombok.extern.slf4j.Slf4j;

/**
 * The Class DatabaseFileReader.
 *
 * 
 */
@Slf4j
@Component
public class DatabaseFileReader extends MultiResourceItemReader<InOutDatabaseDetails> {

	public DatabaseFileReader(){
		super();
	}
	public DatabaseFileReader(String stagingDirectory) throws Exception {
	//	String stagingDirectory = env.getProperty("staging.directory");
		if(StringUtils.isEmpty(stagingDirectory)){
			log.error("Please set Staging Directory field in application.properties.");
			throw new Exception("Staging Directory field is not set");
		}
		Resource[] resources = getResources(stagingDirectory);

		StaxEventItemReader<InOutDatabaseDetails> xmlFileReader = new StaxEventItemReader<>();
        xmlFileReader.setFragmentRootElementName("databaseDetails");
 
        Jaxb2Marshaller databaseMarshaller = new Jaxb2Marshaller();
        databaseMarshaller.setClassesToBeBound(InOutDatabaseDetails.class);
        xmlFileReader.setUnmarshaller(databaseMarshaller);
        
        this.setDelegate(xmlFileReader);
        this.setResources(resources);
	}

	private Resource[] getResources(String stagingDirectory) {
		ResourceArrayPropertyEditor resourceLoader = new ResourceArrayPropertyEditor();
		resourceLoader.setAsText("file:" + stagingDirectory + "/*.xml");
		Resource[] resources = (Resource[]) resourceLoader.getValue();
		return resources;
	}
}
