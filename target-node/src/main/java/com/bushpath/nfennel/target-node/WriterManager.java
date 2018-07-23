package com.bushpath.nfennel.target_node;

import com.google.flatbuffers.FlatBufferBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bushpath.nfennel.flatbuffers.WriterCloseResponse;
import com.bushpath.nfennel.flatbuffers.WriterCloseRequest;
import com.bushpath.nfennel.flatbuffers.WriteResponse;
import com.bushpath.nfennel.flatbuffers.WriteRequest;
import com.bushpath.nfennel.flatbuffers.WriterOpenResponse;
import com.bushpath.nfennel.flatbuffers.WriterOpenRequest;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriterManager {
    private final Logger log = LoggerFactory.getLogger(WriterManager.class);
    protected Map<String, BufferedWriter> writers;
    protected Map<String, Integer> features;
    protected String path;
    protected ReadWriteLock lock;

    public WriterManager(String path) {
        this.writers = new HashMap();
        this.features = new HashMap();
        this.path = path;
        this.lock = new ReentrantReadWriteLock();
    }

	public void close(FlatBufferBuilder flatBufferBuilder,
            WriterCloseRequest request) throws Exception {
        log.info("close writer for '{}'", request.filename());

        this.lock.writeLock().lock();
        try {
            // check if writer exists
            if (!this.writers.containsKey(request.filename())) {
                throw new IllegalArgumentException("Writer '"
                    + request.filename() + "' does not exist");
            }

            // close writer
            BufferedWriter writer = this.writers.get(request.filename());
            writer.close();

            this.writers.remove(request.filename());
            this.features.remove(request.filename());
        } finally {
            this.lock.writeLock().unlock();
        }

        // create response
        WriterCloseResponse.startWriterCloseResponse(flatBufferBuilder);
        int rootIndex = WriterCloseResponse.endWriterCloseResponse(flatBufferBuilder);
        flatBufferBuilder.finish(rootIndex);
    }

	public void open(FlatBufferBuilder flatBufferBuilder,
            WriterOpenRequest request) throws Exception {
        log.info("open writer for '{}'", request.filename());

        this.lock.writeLock().lock();
        try {
            // check if writer exists
            if (this.writers.containsKey(request.filename())) {
                throw new IllegalArgumentException("Writer '"
                    + request.filename() + "' already exists");
            }

            // open writer
            BufferedWriter writer = new BufferedWriter(
                    new FileWriter(this.path + "/" + request.filename())
                );

            for (int i=0; i<request.featuresLength(); i++) {
                writer.write((i==0 ? "" : ",") + request.features(i));
            }
            writer.write("\n");

            this.writers.put(request.filename(), writer);
            this.features.put(request.filename(), request.featuresLength());
        } finally {
            this.lock.writeLock().unlock();
        }

        // create response
        WriterOpenResponse.startWriterOpenResponse(flatBufferBuilder);
        int rootIndex = WriterOpenResponse.endWriterOpenResponse(flatBufferBuilder);
        flatBufferBuilder.finish(rootIndex);
    }

	public void write(FlatBufferBuilder flatBufferBuilder,
            WriteRequest request) throws Exception {
        byte[] bytes = new byte[request.dataLength()];
        for (int i=0; i<request.dataLength(); i++) {
            bytes[i] = request.data(i);
        }

        log.info("write {} bytes to '{}'", bytes.length, request.filename());

        this.lock.writeLock().lock();
        try {
            // check if writer exists
            if (!this.writers.containsKey(request.filename())) {
                throw new IllegalArgumentException("Repository '"
                    + request.filename() + "' does not exist");
            }

            BufferedWriter writer = this.writers.get(request.filename());

            // parse data
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            int bytesRead = 0;
            int featuresCount = this.features.get(request.filename());
            while (bytesRead < bytes.length) {
                for (int i=0; i<featuresCount; i++) {
                    writer.write((i==0 ? "" : ",") + in.readDouble());
                }

                writer.write("\n");
                bytesRead += featuresCount * 8;
            }

            in.close();
        } finally {
            this.lock.writeLock().unlock();
        }

        // create response
        WriteResponse.startWriteResponse(flatBufferBuilder);
        int rootIndex = WriteResponse.endWriteResponse(flatBufferBuilder);
        flatBufferBuilder.finish(rootIndex);
    }
}
