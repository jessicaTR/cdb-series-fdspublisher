package com.thomsonreuters.cdb.fdspublisher.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Dummy Service created to test failaware support. Will be used for publishing deltas to UPA-CHE (Elektron)
 */
@Service
public class UPAService {
    private static final Logger log = LoggerFactory.getLogger(UPAService.class);

    public void start() {
        log.info("::: STARTING UPASERVICE");
    }

    public void close() {
        log.info("::: STOPPING UPASERVICE");
    }

}
