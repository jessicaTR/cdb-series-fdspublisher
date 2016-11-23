package com.thomsonreuters.cdb.fdspublisher.service;

import com.pointcarbon.esb.bootstrap.service.IFailAwareRunner;
import com.pointcarbon.esb.bootstrap.service.IMainRunner;
import com.pointcarbon.esb.commons.concurrent.FixedThreadPoolExecutor;
import com.pointcarbon.esb.monitoring.service.HealthCheckService;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * Main application's Runner service. Implements {@link IMainRunner} called by bootstrap when starting/stopping the app.
 * Responsible for creating number of worker threads corresponding to the max DB connections.
 */
@Service
public class Runner implements IFailAwareRunner {
    private static Logger log = LoggerFactory.getLogger(Runner.class);

    private HealthCheckService healthCheckService = new HealthCheckService();
    
    private UPAService upaService = new UPAService();

    private FixedThreadPoolExecutor fixedThreadPoolExecutor;
    
    final int workernumnber =1;        
     
    private static final String Config_File = "config/local-development/config.properties";
//    private static final String logcfg_file = "config/local-development/logging.conf";
   
//    private static final String Config_File = "src/main/resources/config/local-development/FDSConfig.properties";


    @Override
    public void run() {
        log.info("Starting in active mode");
        run(true);
    }

    @Override
    public void runInPassiveMode() {
        log.info("Starting in passive mode");
        run(false);
    }

    private void run(boolean isActiveMode) {
        log.info("::: STARTING RUNNER");
        if (isActiveMode) {
            upaService.start();
        }     
        
        
//        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties prop = new Properties();
        InputStream input = null;

//    	try {
//    		input = new FileInputStream(Config_File);
//    		// load a properties file
//    		prop.load(input);
//    	} catch (IOException e) {
//    		log.error("Failed to read config file: "+Config_File,e.getMessage());
//    	}
    
      
        try{
        	input =ClassLoader.getSystemClassLoader().getResourceAsStream(Config_File);
        	prop.load(input);
        	
//        	PropertyConfigurator.configure(logcfg_file);
	 
        	  
            try {
            	 PublishSDIFiles publishIncreSDIFiles = new PublishSDIFiles(prop,"incremental");
            	 PublishSDIFiles publishFullSDIFiles = new PublishSDIFiles(prop,"full");
            	 int increInitialDelay = Integer.valueOf(prop.getProperty("incremental.initialDelay"));
            	 int increInterval = Integer.valueOf(prop.getProperty("incremental.interval"));
            	 int fullInitialDelay = Integer.valueOf(prop.getProperty("full.initialDelay"));
            	 int fullInterval = Integer.valueOf(prop.getProperty("full.interval"));
            	 
            	 ScheduledExecutorService service = Executors.newScheduledThreadPool(2);                                    
                 service.scheduleAtFixedRate(publishIncreSDIFiles, increInitialDelay, increInterval, TimeUnit.SECONDS);
                 service.scheduleAtFixedRate(publishFullSDIFiles, fullInitialDelay, fullInterval, TimeUnit.SECONDS);
				 
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("Publish file failed.",e.getMessage());
			}
        } catch (Exception e) {
        	log.error("Failed to read config file: "+Config_File,e.getMessage());
		}
        

 
//        initHealthService(); 
                    
           
    }
 

    private void initHealthService() {
//        healthCheckService.registerMeter("DequeuedEvents");
//        healthCheckService.registerGauge("LastProcessedMessage");
    }


    @Override
    public void switchToPassiveMode() {
        log.info("::: STOPPING FAILAWARE SERVICES");
        upaService.close();
    }

    @Override
    public void switchToActiveMode() {
        log.info("::: STARTING FAILAWARE SERVICES");
        upaService.start();
    }

    @Override
    public void shutdown() {
       
        upaService.close();
        fixedThreadPoolExecutor.shutdown();
        ((ch.qos.logback.classic.Logger) log).getLoggerContext().stop();
        System.out.println("Done");
    }
}
