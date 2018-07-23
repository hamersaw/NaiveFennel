package com.bushpath.nfennel.node;

import com.bushpath.rutils.query.Query;
import com.bushpath.rutils.reader.CsvReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class RepositoryReader {
    protected List<String> files;
    protected Query query;
    protected int bufferSize;
    protected int fileIndex;
    protected CsvReader reader;

    public RepositoryReader(List<String> paths, Query query,
            int bufferSize) throws Exception {
        // initialize files
        this.files = new ArrayList();
        for (String path : paths) {
            File directory = new File(path);
            File[] children = directory.listFiles();
            
            for (int i=0; i<children.length; i++) {
                if (children[i].isFile()) {
                    files.add(children[i].getCanonicalPath());
                }
            }
        }

        this.query = query;
        this.bufferSize = bufferSize;
        this.fileIndex = 1;

        this.reader = new CsvReader(files.get(0), null, null);
    }

    public int getBufferSize() {
        return this.bufferSize;
    }

    public String[] getFeatures() {
        return this.reader.getFeatures();
    }

    public double[] next() throws Exception {
        double[] record = null;
        while (true) {
            // attempt to read record from reader
            record = reader.next();

            // TODO - apply query to record

            if (record != null) {
                break;
            } else {
                // if no record attempt to open next reader
                if (this.fileIndex >= this.files.size()) {
                    break;
                }

                this.reader.close();
                this.reader =
                    new CsvReader(this.files.get(this.fileIndex), null, null);
                this.fileIndex += 1;
            }
        }

        return record;
    }
}
