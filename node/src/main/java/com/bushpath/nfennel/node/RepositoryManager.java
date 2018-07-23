package com.bushpath.nfennel.node;

import com.bushpath.rutils.query.Query;

import com.google.flatbuffers.FlatBufferBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.nfennel.flatbuffers.DataResponse;
import com.bushpath.nfennel.flatbuffers.DataRequest;
import com.bushpath.nfennel.flatbuffers.QueryResponse;
import com.bushpath.nfennel.flatbuffers.QueryRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RepositoryManager {
    private final Logger log = LoggerFactory.getLogger(RepositoryManager.class);
    protected Map<String, List<String>> repositories;
    protected Map<Long, RepositoryReader> readers;
    protected ReadWriteLock lock;
    protected Random random;

    public RepositoryManager(Map<String, List<String>> repositories) {
        this.repositories = repositories;
        this.readers = new HashMap();
        this.lock = new ReentrantReadWriteLock();
        this.random = new Random(System.nanoTime());
    }

	public void getData(FlatBufferBuilder flatBufferBuilder,
            DataRequest request) throws Exception {
        log.info("data request on '{}'", request.id());

        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        this.lock.writeLock().lock();
        try {
            // check if repository reader exists
            if (!this.readers.containsKey(request.id())) {
                throw new IllegalArgumentException("Repository reader '"
                    + request.id() + "' does not exist");
            }

            // get repository reader
            RepositoryReader reader = this.readers.get(request.id());
            DataOutputStream out = new DataOutputStream(byteOut);

            int recordCount = 0;
            double[] record = null;
            while ((record = reader.next()) != null) {
                for (double d : record) {
                    out.writeDouble(d);
                }

                recordCount += 1;
                if (recordCount == reader.getBufferSize()) {
                    break;
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }

        // create response
        int dataIndex =
            DataResponse.createDataVector(flatBufferBuilder, byteOut.toByteArray());

        DataResponse.startDataResponse(flatBufferBuilder);
        DataResponse.addData(flatBufferBuilder, dataIndex);
        int rootIndex = DataResponse.endDataResponse(flatBufferBuilder);
        flatBufferBuilder.finish(rootIndex);
    }

	public void query(FlatBufferBuilder flatBufferBuilder,
            QueryRequest request) throws Exception {
        // parse query
        byte[] bytes = new byte[request.queryLength()];
        for (int i=0; i<request.queryLength(); i++) {
            bytes[i] = request.query(i);
        }
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(byteIn);
        Query query = (Query) in.readObject();
        in.close();
        byteIn.close();

        log.info("query on '{}'", query.getEntity());

        long id = -1;
        int featuresIndex = -1;
        this.lock.writeLock().lock();
        try {
            // check if repository exists
            if (!this.repositories.containsKey(query.getEntity())) {
                throw new IllegalArgumentException("Repository '"
                    + query.getEntity() + "' does not exist");
            }

            // create RepositoryReader
            RepositoryReader reader =
                new RepositoryReader(this.repositories.get(query.getEntity()),
                    query, request.bufferSize());

            id = random.nextLong();
            this.readers.put(id, reader);

            // add features to flatbuffer
            String[] features = reader.getFeatures();
            int[] data = new int[features.length];
            for (int i=0; i<features.length; i++) {
                data[i] = flatBufferBuilder.createString(features[i]);
            }

            featuresIndex = QueryResponse.createFeaturesVector(flatBufferBuilder, data);
        } finally {
            this.lock.writeLock().unlock();
        }

        // create response
        QueryResponse.startQueryResponse(flatBufferBuilder);
        QueryResponse.addId(flatBufferBuilder, id);
        QueryResponse.addFeatures(flatBufferBuilder, featuresIndex);
        int rootIndex = QueryResponse.endQueryResponse(flatBufferBuilder);
        flatBufferBuilder.finish(rootIndex);
    }
}
