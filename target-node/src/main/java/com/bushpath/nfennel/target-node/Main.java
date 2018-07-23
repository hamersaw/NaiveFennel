package com.bushpath.nfennel.target_node;

import com.moandjiezana.toml.Toml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // parse arguments
        if (args.length != 1) {
            System.out.println("Usage: ./nfennel-target-node <config-file>");
            System.exit(1);
        }

        // parse toml file
        Toml toml = new Toml();
        try {
            toml.read(new File(args[0]));
        } catch (Exception e) {
            log.error("failed parsing configuration file: {}", e.getMessage());
            System.exit(1);
        }

        String hostname = toml.getString("hostname");
        short port = toml.getLong("port").shortValue();
        int threadCount = toml.getLong("threadCount").intValue();
        String path = toml.getString("path");

        // start DataService
        WriterManager writerManager = new WriterManager(path);
        DataService dataService =
            new DataService(hostname, port, threadCount, writerManager);
        dataService.start();
        log.info("started data service");

        try {
            // wait for service to finish
            dataService.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
