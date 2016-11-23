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
public class PublishSDIFilesIncremental implements Runnable{	
	
	private static Logger log = LoggerFactory.getLogger(PublishSDIFilesIncremental.class);
	Properties prop;
	public PublishSDIFilesIncremental(Properties propParm){
		prop = propParm;
	}
	
	@Override
	public void run()   	
	{
		 
        try{
			boolean cliArguments = true;
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
			final String fileextension;
 
 
			System.out.println("Starting FDS....");
			log.info("Starting FDS....");
			username = prop.getProperty("username");
			password = prop.getProperty("password");
			increSdiPublishingID = Integer.valueOf(prop.getProperty("incremental.SdiPublishingID"));
			increSdiPublishingFileTypeID = Integer.valueOf(prop.getProperty("incremental.SdiPublishingFileTypeID"));
//			fullSdiPublishingID = Integer.valueOf(prop.getProperty("full.SdiPublishingID"));
//			fullSdiPublishingFileTypeID = Integer.valueOf(prop.getProperty("full.SdiPublishingFileTypeID"));
			contentRunID = prop.getProperty("ContentRunID");
			increWaitingFilePath = prop.getProperty("incremental.WaitingFileLocation");
			increArchiveFilePath = prop.getProperty("incremental.ArchiveFileLocation");
			fileextension = prop.getProperty("incremental.fileextension");
			File WaitingFolder = new File(increWaitingFilePath);
			File[] FileList = WaitingFolder.listFiles(new ExtensionFilter(fileextension));
			// Sorting
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
			
			
			//FDS API instance creation.
			final FdsClient fdsClient = new FdsClient();
			final long publicationID;
			
			System.out.println("FDS Initialized Successfully!!");
			System.out.println(FileList.length+" files found.");
			log.info("FDS Initialized Successfully!!");
			log.info(FileList.length+" files found.");
			
			if(FileList.length==1){
				String waitingFilePath = FileList[0].getPath();
				publicationID = 
						fdsClient.putFile(username, password, increSdiPublishingID, increSdiPublishingFileTypeID, contentRunID, waitingFilePath);
				String archiveFilePath = increArchiveFilePath+ FileList[0].getName();
				File f1 = new File(waitingFilePath);
				File f2 = new File(archiveFilePath);
				FileUtilities.MoveFile(f1, f2);
			}
			else{
				String[] fileNameArray = new String[FileList.length];
				for(int i=0;i<FileList.length;i++){
					fileNameArray[i] = FileList[i].getPath();					
				}
				publicationID = 
						fdsClient.putFile(username, password, increSdiPublishingID, increSdiPublishingFileTypeID, contentRunID, fileNameArray);
				for(int i=0;i<fileNameArray.length;i++){
					String archiveFilePath = increArchiveFilePath+ FileList[i].getName();
					File f1 = new File(fileNameArray[i]);
					File f2 = new File(archiveFilePath);
					FileUtilities.MoveFile(f1, f2);
				}
			}
			
			if(publicationID != 0)
			{
				System.out.println("File published successfully!!");
				log.info("File published successfully!!");
			}
			else
			{
				System.out.println("Failed to publish the file. Please check the log for error!!");
				log.info("Failed to publish the file. Please check the log for error!!" );
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
