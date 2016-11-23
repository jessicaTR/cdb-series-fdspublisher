package com.thomsonreuters.cdb.fdspublisher.service;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thomsonreuters.cmfds.api.FdsClient;
import com.thomsonreuters.cmfds.api.exceptions.FdsApiException;

/**
 * This is the interactive subscriber application which uses FDS API for subscribing to files.
 * 
 * @author pavan.rai
 *
 */
public class SubscriberApp 
{
	private static final String Config_File = "src/main/resources/config/local-development/FDSConfig.properties";
	private static Logger log = LoggerFactory.getLogger(SubscriberApp.class);

	public static void main(String[] args) throws IOException, FdsApiException
	{
		boolean runAgain = true;
		 ClassLoader loader = Thread.currentThread().getContextClassLoader();
	        Properties prop = new Properties();
	    
//	        try(InputStream resourceStream = loader.getResourceAsStream(Config_File)) {
//	            prop.load(resourceStream);
	        try{
	       		prop.load(new FileInputStream(Config_File));
			} catch (FileNotFoundException e1) {
				log.error("Config file not found: "+Config_File,e1.getMessage());
			} catch (IOException e1) {
				log.error("Failed to read config file: "+Config_File,e1.getMessage());
			}
	        
			String username = prop.getProperty("username");
			String password = prop.getProperty("password");
			String increSubscriptionid = prop.getProperty("incremental.SubscriptionID");
			String fullSubscriptionid = prop.getProperty("full.SubscriptionID");
			String increDownloadpath = prop.getProperty("incremental.downloadpath");
			String fullDownloadpath = prop.getProperty("full.downloadpath");
			
			//FDS API instance creation.
			final FdsClient fdsClient = new FdsClient();
			final long[] increSuccessfulPublicationCount;
			final long[] fullSuccessfulPublicationCount;
			
			System.out.println("FDS Initialized Successfully!!");
			System.out.println("Subscribing to the file....");
			
			//FDS API getFile call.
			increSuccessfulPublicationCount = 
				fdsClient.getFile(username, password, Long.valueOf(increSubscriptionid), increDownloadpath);
			if(increSuccessfulPublicationCount.length != 0)
			{
				System.out.println(increSuccessfulPublicationCount.length + " incremental publications subscribed to successfully!!");
			}
			else
			{
				System.out.println("Failed to subscribe to any incremental publication. Please check the logs!!");
			}
			 
			fullSuccessfulPublicationCount = 
					fdsClient.getFile(username, password, Long.valueOf(fullSubscriptionid), fullDownloadpath);
				if(fullSuccessfulPublicationCount.length != 0)
				{
					System.out.println(fullSuccessfulPublicationCount.length + " full publications subscribed to successfully!!");
				}
				else
				{
					System.out.println("Failed to subscribe to any full publication. Please check the logs!!");
				}
						
			 
		     System.out.println("FDS shutdown successfully!!");
	}
}
