package com.thomsonreuters.cdb.fdspublisher.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thomsonreuters.ce.dbor.file.ExtensionFilter;
import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.cmfds.api.FdsClient;
import com.thomsonreuters.cmfds.api.exceptions.FdsApiException;

/**
 * This is the interactive publisher application which uses the FDS API to publish files.
 * 
 * @author pavan.rai
 *
 */
public class PublishSDIFiles implements Runnable{	
	
	private static Logger log = LoggerFactory.getLogger(PublishSDIFiles.class);
	
	Properties prop;
	String flag;
	public PublishSDIFiles(Properties propIn, String flagIn){
		prop = propIn;
		flag = flagIn;
		
	}
	
	@Override
	public void run()   	
	{
		 
        try{
			 
			final String username;
			final String password;
			final int increSdiPublishingID;
			final int increSdiPublishingFileTypeID;
			final int fullSdiPublishingID;
			final int fullSdiPublishingFileTypeID;
			int argumentLength = 0;
			final String contentRunID;
			String[] fileNameArrayInput = null;
			int fileNameArrayInputSize = 0;
			final String filename;
			final File file;
			final List<String> fileNameList;
			final String increWaitingFilePath;
			final String increArchiveFilePath;
			final String increFileExtension;
			final String fullWaitingFilePath;
			final String fullArchiveFilePath;
			final String fullFileExtension;
			final String increFlag = "incremental";
			final String fullFlag = "full";
 
 
			System.out.println("Starting FDS...."+flag);
			username = prop.getProperty("username");
			password = prop.getProperty("password");
			increSdiPublishingID = Integer.valueOf(prop.getProperty("incremental.SdiPublishingID"));
			increSdiPublishingFileTypeID = Integer.valueOf(prop.getProperty("incremental.SdiPublishingFileTypeID"));
			fullSdiPublishingID = Integer.valueOf(prop.getProperty("full.SdiPublishingID"));
			fullSdiPublishingFileTypeID = Integer.valueOf(prop.getProperty("full.SdiPublishingFileTypeID"));
//			contentRunID = prop.getProperty("ContentRunID");
			increWaitingFilePath = prop.getProperty("incremental.WaitingFileLocation");
			increArchiveFilePath = prop.getProperty("incremental.ArchiveFileLocation");
			increFileExtension = prop.getProperty("incremental.fileextension");
			fullWaitingFilePath = prop.getProperty("full.WaitingFileLocation");
			fullArchiveFilePath = prop.getProperty("full.archiveFileLocation");
			fullFileExtension = prop.getProperty("full.fileextension");
			contentRunID = (new Date()).toString();
			
			
			File WaitingFolder = null;
			File[] FileList = null;
			
			if(flag.equals(increFlag)){
				 WaitingFolder = new File(increWaitingFilePath);
				 FileList = WaitingFolder.listFiles(new ExtensionFilter(increFileExtension));
			}else if(flag.equals(fullFlag)){
				 WaitingFolder = new File(fullWaitingFilePath);
				 FileList = WaitingFolder.listFiles(new ExtensionFilter(fullFileExtension));
			}			
			
			 
			Arrays.sort(FileList, new Comparator<File>() {
				 
				public int compare(File o1, File o2) {
					long FirstLastModifyDate=o1.lastModified();
					long SecondLastModifyDate=o2.lastModified();
					
					
					if (FirstLastModifyDate<SecondLastModifyDate)
					{
						return -1;
					}
					else if (FirstLastModifyDate>SecondLastModifyDate)
					{
						return 1;
					}
					else
					{
						return 0;
					}
					
				}
			});
						
			 
			final FdsClient fdsClient = new FdsClient();
			long publicationID = 0;
			
			
			log.info("FDS Initialized Successfully for "+flag +" CDBSeriesMetaData !");
			log.info(FileList.length+" files found.");
			if(FileList.length>0){
				if(FileList.length==1){
					String archiveFilePath = null;
					String waitingFilePath = FileList[0].getPath();
					if(flag.equals(increFlag)){
						publicationID = 
								fdsClient.putFile(username, password, increSdiPublishingID, increSdiPublishingFileTypeID, contentRunID, waitingFilePath);
						archiveFilePath = increArchiveFilePath+ FileList[0].getName();
					}else if(flag.equals(fullFlag)){
						publicationID = 
								fdsClient.putFile(username, password, fullSdiPublishingID, fullSdiPublishingFileTypeID, contentRunID, waitingFilePath);
						archiveFilePath = fullArchiveFilePath+ FileList[0].getName();
					}
					log.info("File "+FileList[0].getName()+" is put to FDS.");
					
					File f1 = new File(waitingFilePath);
					File f2 = new File(archiveFilePath);
					FileUtilities.MoveFile(f1, f2);
					
				}else if(FileList.length>1){
					String[] fileNameArray = new String[FileList.length];
					String archiveFilePath = null;
					String filenames = "";
					for(int i=0;i<FileList.length;i++){
						fileNameArray[i] = FileList[i].getPath();	
						filenames = filenames+", "+FileList[i].getName();
					}
					if(flag.equals(increFlag)){
						publicationID = 
								fdsClient.putFile(username, password, increSdiPublishingID, increSdiPublishingFileTypeID, contentRunID, fileNameArray);
						archiveFilePath = increArchiveFilePath;
					}else if(flag.equals(fullFlag)){
						publicationID = 
								fdsClient.putFile(username, password, fullSdiPublishingID, fullSdiPublishingFileTypeID, contentRunID, fileNameArray);
						archiveFilePath = fullArchiveFilePath;
					}
					log.info("Files "+filenames.substring(1)+" are put to FDS.");
					
					for(int i=0;i<fileNameArray.length;i++){
						String archiveFileName = archiveFilePath+ FileList[i].getName();
						File f1 = new File(fileNameArray[i]);
						File f2 = new File(archiveFileName);
						FileUtilities.MoveFile(f1, f2);
					}
				}
				
				if(publicationID != 0)
				{
					log.info("File published successfully!!");
				}
				else
				{
					log.error("Failed to publish the file. Please check the log for error!!");
				}

			}
			        }catch(FdsApiException e){
          log.error("FDS API error, "+e.getMessage());
        } 

	}
	
 
	
	/**
	 * List the files in the directory and return the list of file paths
	 * 
	 * @param baseDir - the base directory
	 * @return list of file paths.
	 */
	private static List<String> listFiles(final File baseDir) 
	{
		List<String> fileNameList = new ArrayList<String>();
        for (File file : baseDir.listFiles()) 
        {
            if (file.isFile()) 
            {
                fileNameList.add(file.getPath());
            }
        }
        return fileNameList;
    }



	
}
