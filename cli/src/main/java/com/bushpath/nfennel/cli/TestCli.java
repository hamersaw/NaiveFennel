package com.bushpath.nfennel.cli;

import com.bushpath.rutils.query.Query;
import com.bushpath.rutils.query.parser.FeatureRangeParser;
import com.bushpath.rutils.query.parser.Parser;

import com.google.flatbuffers.FlatBufferBuilder;

import com.moandjiezana.toml.Toml;
                               
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters; 

import com.bushpath.nfennel.flatbuffers.DataResponse;
import com.bushpath.nfennel.flatbuffers.DataRequest;
import com.bushpath.nfennel.flatbuffers.Failure;
import com.bushpath.nfennel.flatbuffers.MessageType;
import com.bushpath.nfennel.flatbuffers.WriterCloseResponse;
import com.bushpath.nfennel.flatbuffers.WriterCloseRequest;
import com.bushpath.nfennel.flatbuffers.WriterOpenResponse;
import com.bushpath.nfennel.flatbuffers.WriterOpenRequest;
import com.bushpath.nfennel.flatbuffers.WriteResponse;
import com.bushpath.nfennel.flatbuffers.WriteRequest;
import com.bushpath.nfennel.flatbuffers.QueryResponse;
import com.bushpath.nfennel.flatbuffers.QueryRequest;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Command(name = "test",
    description = "Issue a query to NaiveFennel nodes and DO NOT write data.",
    mixinStandardHelpOptions = true)
public class TestCli implements Runnable {
    @Parameters(index = "0", description = "Data repository on NaiveFennel node.")
    private String repository;

    @Option(names = {"-b", "--buffer-size"},
        description = "Size of data buffer [default: 2500].")
    private int bufferSize = 2500;

    @Option(names = {"-c", "--config-file"},
        description = "Path to toml configuration file [default: \"src/main/resources/config.toml\"].")
    public String configurationFile = "src/main/resources/config.toml";

    @Option(names = {"-q", "--query"},
        description = "Feature range query (ie. 'f0:0..10', 'f1:0..', 'f2:..10').")
    private String[] queries;

    @Option(names = {"-s", "--sample-probability"},
        description = "Probability of an observation to be sampled [default=1.0].")
    private double sampleProbability = 1.0;

    @Option(names = {"-t", "--thread-count"},
        description = "Number of threads to concurrently request data [default=8].")
    private int threadCount = 8;
 
    @Override
    public void run() {
        try {
            // parse toml file
            Toml toml = new Toml();
            toml.read(new File(this.configurationFile));

            /**
             * issue queries to source nodes
             */
            // parse query
            Parser parser = new FeatureRangeParser();
            Query query = parser.evaluate(this.queries);
            query.setEntity(this.repository);

			// create QueryRequest
            FlatBufferBuilder qFlatBufferBuilder = new FlatBufferBuilder(1);

            // add query
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream(); 
            ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
            objectOut.writeObject(query);
            objectOut.close(); 
            byteOut.close();
  
            byte[] queryBytes = byteOut.toByteArray();
            int queryIndex =
                QueryRequest.createQueryVector(qFlatBufferBuilder, queryBytes);

            // add QueryRequest
            QueryRequest.startQueryRequest(qFlatBufferBuilder);
            QueryRequest.addQuery(qFlatBufferBuilder, queryIndex);
			QueryRequest.addBufferSize(qFlatBufferBuilder, this.bufferSize);
            QueryRequest.addSampleProbability(qFlatBufferBuilder, this.sampleProbability);
            int rootIndex = QueryRequest.endQueryRequest(qFlatBufferBuilder);

            // finalize byte array
            qFlatBufferBuilder.finish(rootIndex);
            byte[] queryRequest = qFlatBufferBuilder.sizedByteArray();

            // initialize workers and writer
            long startTime = System.currentTimeMillis();
            BlockingQueue<Map<String, Object>> nodeQueue = new ArrayBlockingQueue(256);
            BlockingQueue<double[]> recordQueue = new ArrayBlockingQueue(4096);

            Worker[] workers = new Worker[this.threadCount];
            for (int i=0; i<workers.length; i++) {
                workers[i] = new Worker(nodeQueue, recordQueue, queryRequest);
                workers[i].start();
            }

            Writer writer = new Writer(recordQueue);
            writer.start();

            // send nodes down queue
            for (Object object : toml.getList("source_nodes")) {
                Map<String, Object> node = (Map) object;
                while (!nodeQueue.offer(node)) {}
            }

            for (Worker worker : workers) {
                worker.shutdown();
                worker.join();
            }

            writer.shutdown();
            writer.join();

            long endTime = System.currentTimeMillis();
            System.out.println("read " + writer.getRecordCount() + " record(s) in "
                + (endTime - startTime) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected class Writer extends Thread {
        protected BlockingQueue<double[]> in;
        protected long recordCount;
        protected boolean shutdown;

        public Writer(BlockingQueue in) {
            this.in = in;
            this.recordCount = 0;
            this.shutdown = false;
        }

        @Override
        public void run() {

            double[] record;
            while (!this.in.isEmpty() || !this.shutdown) {
                try {
                    record = this.in.poll(5, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    continue;
                }

                if (record == null) {
                    continue;
                }

                // handle record
                this.recordCount += 1;
            }
        }

        public void shutdown() {
            this.shutdown = true;
        }

        public long getRecordCount() {
            return this.recordCount;
        }
    }

    protected class Worker extends Thread {
        protected BlockingQueue<Map<String, Object>> in;
        protected BlockingQueue<double[]> out;
        protected byte[] queryRequest;
        protected boolean shutdown;

        public Worker(BlockingQueue in, BlockingQueue out, byte[] queryRequest) {
            this.in = in;
            this.out = out;
            this.queryRequest = queryRequest;
            this.shutdown = false;
        }

        @Override
        public void run() {
            while (!this.in.isEmpty() || !this.shutdown) {
                Map<String, Object> node = null;
                try {
                    node = this.in.poll(5, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    continue;
                }

                if (node == null) {
                    continue;
                }

                // parse node hostname and port
                String hostname = node.get("hostname").toString();
                short port = ((Long) node.get("port")).shortValue();

                System.out.println("querying '" + hostname + ":" + port + "'");

                // send QueryRequest
                byte[] qResponseBytes;
                try {
                    qResponseBytes = Main.sendMessage(hostname, port,
                        MessageType.Query, queryRequest, null);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    continue;
                }

                // parse QueryResponse
                ByteBuffer qByteBuffer = ByteBuffer.wrap(qResponseBytes);
                QueryResponse queryResponse =
                    QueryResponse.getRootAsQueryResponse(qByteBuffer);

                // read all data
                FlatBufferBuilder dFlatBufferBuilder = new FlatBufferBuilder(1);

                // add DataRequest
                DataRequest.startDataRequest(dFlatBufferBuilder);
                DataRequest.addId(dFlatBufferBuilder, queryResponse.id());
                int dRootIndex = DataRequest.endDataRequest(dFlatBufferBuilder);

                // finalize byte array
                dFlatBufferBuilder.finish(dRootIndex);
                byte[] dataRequest = dFlatBufferBuilder.sizedByteArray();

                // retrieve all data
                while (true) {
                    // send DataRequest
                    byte[] dResponseBytes;
                    try {
                        dResponseBytes = Main.sendMessage(hostname, port,
                            MessageType.Data, dataRequest, null);
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        break;
                    }

                    // parse DataResponse
                    ByteBuffer dByteBuffer = ByteBuffer.wrap(dResponseBytes);
                    DataResponse dataResponse =
                        DataResponse.getRootAsDataResponse(dByteBuffer);

                    // parse data and distribute over buffers
                    byte[] bytes = new byte[dataResponse.dataLength()];
                    for (int i=0; i<dataResponse.dataLength(); i++) {
                        bytes[i] = dataResponse.data(i);
                    }

                    try {
                        DataInputStream dataIn =
                            new DataInputStream(new ByteArrayInputStream(bytes));
                        int bytesRead = 0;
                        while (bytesRead < bytes.length) {
                            double[] record = new double[queryResponse.featuresLength()];
                            for (int i=0; i<record.length; i++) {
                                record[i] = dataIn.readDouble();
                            }

                            // send record to out queue
                            while (!this.out.offer(record)) {}
                            bytesRead += record.length * 8;
                        }

                        dataIn.close();
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                        break;
                    }

                    // if no data returned -> break
                    if (dataResponse.dataLength() == 0) {
                        break;
                    }
                }
            }
        }

        public void shutdown() {
            this.shutdown = true;
        }
    }
}
