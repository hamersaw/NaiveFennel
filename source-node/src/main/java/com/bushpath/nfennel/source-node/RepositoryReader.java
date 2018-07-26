package com.bushpath.nfennel.source_node;

import com.bushpath.rutils.query.Query;
import com.bushpath.rutils.query.Expression;
import com.bushpath.rutils.reader.CsvReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RepositoryReader {
    protected List<String> files;
    protected int fileIndex;
    protected int bufferSize;
    protected double sampleProbability;
    protected CsvReader reader;
    protected Random random;
    protected List<QueryWrapper> queries;

    public RepositoryReader(List<String> paths, Query query,
            int bufferSize, double sampleProbability) throws Exception {
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

        this.fileIndex = 1;
        this.bufferSize = bufferSize;
        this.sampleProbability = sampleProbability;
        if (files.size() != 0) {
            this.reader = new CsvReader(files.get(0), null, null);
        }
        this.random = new Random(System.nanoTime());

        // initialize queries
        this.queries = new ArrayList();
        String[] features = reader.getFeatures();
        for (String feature : query.getFeatures()) {
            int index = -1;
            for (int j=0; j<features.length; j++) {
                if (features[j].equals(feature)) {
                    index = j;
                }
            }

            if (index == -1) {
                continue;
            }

            queries.add(new QueryWrapper(query.getExpression(feature), index));
        }
    }

    public int getBufferSize() {
        return this.bufferSize;
    }

    public String[] getFeatures() {
        return this.reader.getFeatures();
    }

    public double[] next() throws Exception {
        if (this.reader == null && this.fileIndex >= this.files.size()) {
            return null;
        }

        double[] record = null;
        while (true) {
            // attempt to read record from reader
            record = reader.next();

            // if no record -> attempt to open next reader
            if (record == null) {
                this.reader.close();
                this.reader = null;
                if (this.fileIndex >= this.files.size()) {
                    break;
                }

                this.reader =
                    new CsvReader(this.files.get(this.fileIndex), null, null);
                this.fileIndex += 1;
                continue;
            }

            // check if record satisfies query
            boolean valid = true;
            for (QueryWrapper queryWrapper : this.queries) {
                if (!queryWrapper.evaluate(record)) {
                    valid = false;
                    break;
                }
            } 

            if (!valid) {
                continue;
            }

            // evaluate sample probability
            if (this.random.nextDouble() > this.sampleProbability) {
                continue;
            }

            // if record passes all the tests return
            break;
        }

        return record;
    }

    protected class QueryWrapper {
        protected Expression expression;
        protected int index;

        public QueryWrapper(Expression expression, int index) {
            this.expression = expression;
            this.index = index;
        }

        public boolean evaluate(double[] record) {
            return expression.evaluate((float) record[index]);
        }
    }
}
