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

@Command(name = "distribute",
    description = "Issue a query to NaiveFennel sourcenodes and distribute data to target nodes.",
    mixinStandardHelpOptions = true)
public class DistributeCli implements Runnable {
    @Parameters(index = "0", description = "Data repository on NaiveFennel node.")
    private String repository;

    @Parameters(index = "1",
        description = "Filename of data on NaiveFennel catcher nodes")
    private String filename;

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
 
    @Override
    public void run() {
        try {
            // parse toml file
            Toml toml = new Toml();
            toml.read(new File(this.configurationFile));

            List<Object> targetNodes = toml.getList("target_nodes");
            List<double[]>[] buffers = new List[targetNodes.size()];
            int bufferIndex = 0;

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

            boolean writersInitialized = false;
            for (Object object : toml.getList("source_nodes")) {
                Map<String, Object> node = (Map) object;
                String hostname = node.get("hostname").toString();
                short port = ((Long) node.get("port")).shortValue();

                // send QueryRequest
                byte[] qResponseBytes = Main.sendMessage(hostname, port,
                    MessageType.Query, queryRequest, null);

                // parse QueryResponse
                ByteBuffer qByteBuffer = ByteBuffer.wrap(qResponseBytes);
                QueryResponse queryResponse =
                    QueryResponse.getRootAsQueryResponse(qByteBuffer);

                /**
                 * initialize writers on target nodes
                 */
                if (!writersInitialized) {
                    /**
                     * open writers on target-nodes
                     */
                    // create WriterOpenRequest
                    FlatBufferBuilder woFlatBufferBuilder = new FlatBufferBuilder(1);

                    // add filename
                    int woFilenameIndex = woFlatBufferBuilder.createString(this.filename);

                    // add features
                    int[] data = new int[queryResponse.featuresLength()];
                    for (int i=0; i<queryResponse.featuresLength(); i++) {
                        data[i] =
                            woFlatBufferBuilder.createString(queryResponse.features(i));
                    }

                    int featuresIndex =
                        WriterOpenRequest.createFeaturesVector(woFlatBufferBuilder, data);

                    // add WriterOpenRequest
                    WriterOpenRequest.startWriterOpenRequest(woFlatBufferBuilder);
                    WriterOpenRequest.addFilename(woFlatBufferBuilder, woFilenameIndex);
                    WriterOpenRequest.addFeatures(woFlatBufferBuilder, featuresIndex);
                    int woRootIndex =
                        WriterOpenRequest.endWriterOpenRequest(woFlatBufferBuilder);

                    // finalize byte array
                    woFlatBufferBuilder.finish(woRootIndex);
                    byte[] writerOpenRequest = woFlatBufferBuilder.sizedByteArray();

                    // opne writers on each target node
                    for (int i=0; i<targetNodes.size(); i++) {
                        Map<String, Object> n = (Map) targetNodes.get(i);
                        String h = n.get("hostname").toString();
                        short p = ((Long) n.get("port")).shortValue();

                        // send WriterOpenRequest
                        byte[] woResponseBytes = Main.sendMessage(h, p,
                            MessageType.WriterOpen, writerOpenRequest, null);

                        buffers[i] = new ArrayList();
                    }

                    writersInitialized = true;
                }

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
                    byte[] dResponseBytes = Main.sendMessage(hostname, port,
                        MessageType.Data, dataRequest, null);

                    // parse DataResponse
                    ByteBuffer dByteBuffer = ByteBuffer.wrap(dResponseBytes);
                    DataResponse dataResponse =
                        DataResponse.getRootAsDataResponse(dByteBuffer);

                    // parse data and distribute over buffers
                    byte[] bytes = new byte[dataResponse.dataLength()];
                    for (int i=0; i<dataResponse.dataLength(); i++) {
                        bytes[i] = dataResponse.data(i);
                    }

                    DataInputStream in =
                        new DataInputStream(new ByteArrayInputStream(bytes));
                    int bytesRead = 0;
                    while (bytesRead < bytes.length) {
                        double[] record = new double[queryResponse.featuresLength()];
                        for (int i=0; i<record.length; i++) {
                            record[i] = in.readDouble();
                        }

                        // add record to buffer
                        buffers[bufferIndex].add(record);

                        if (buffers[bufferIndex].size() == this.bufferSize) {
                            Map<String, Object> n = (Map) targetNodes.get(bufferIndex);
                            String h = n.get("hostname").toString();
                            short p = ((Long) n.get("port")).shortValue();
                            this.writeBuffer(h, p, buffers[bufferIndex], this.filename);
                        }

                        bufferIndex = (bufferIndex + 1) % buffers.length;
                        bytesRead += record.length * 8;
                    }

                    in.close();
                    System.out.println("Received "
                        + dataResponse.dataLength() + " bytes");

                    // if no data returned -> break
                    if (dataResponse.dataLength() == 0) {
                        break;
                    }
                }
            }

            /**
             * close writers on target-nodes
             */
            // create WriterCloseRequest
            FlatBufferBuilder wcFlatBufferBuilder = new FlatBufferBuilder(1);

            // add filename
            int wcFilenameIndex = wcFlatBufferBuilder.createString(this.filename);

            // add WriterCloseRequest
            WriterCloseRequest.startWriterCloseRequest(wcFlatBufferBuilder);
            WriterCloseRequest.addFilename(wcFlatBufferBuilder, wcFilenameIndex);
            int wcRootIndex =
                WriterCloseRequest.endWriterCloseRequest(wcFlatBufferBuilder);

            // finalize byte array
            wcFlatBufferBuilder.finish(wcRootIndex);
            byte[] writerCloseRequest = wcFlatBufferBuilder.sizedByteArray();

            for (int i=0; i<targetNodes.size(); i++) {
                Map<String, Object> node = (Map) targetNodes.get(i);
                String hostname = node.get("hostname").toString();
                short port = ((Long) node.get("port")).shortValue();

                // send WriteRequest
                if (!buffers[i].isEmpty()) {
                    this.writeBuffer(hostname, port, buffers[i], this.filename);
                }

                // send WriterCloseRequest
                byte[] wcResponseBytes = Main.sendMessage(hostname, port,
                    MessageType.WriterClose, writerCloseRequest, null);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void writeBuffer(String hostname, short port,
            List<double[]> buffer, String filename) throws Exception {
        // create WriteRequest
        FlatBufferBuilder flatBufferBuilder = new FlatBufferBuilder(1);

        // add filename
        int filenameIndex = flatBufferBuilder.createString(filename);

        // add data
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteOut);
        for (double[] record : buffer) {
            for (double d : record) {
                out.writeDouble(d);
            }
        }

        int dataIndex =
            WriteRequest.createDataVector(flatBufferBuilder, byteOut.toByteArray());

        // create WriteRequest
        WriteRequest.startWriteRequest(flatBufferBuilder);
        WriteRequest.addFilename(flatBufferBuilder, filenameIndex);
        WriteRequest.addData(flatBufferBuilder, dataIndex);
        int rootIndex = WriteRequest.endWriteRequest(flatBufferBuilder);
        flatBufferBuilder.finish(rootIndex);

        // finalize byte array
        flatBufferBuilder.finish(rootIndex);
        byte[] writeRequest = flatBufferBuilder.sizedByteArray();

        // send message
        byte[] responseBytes = Main.sendMessage(hostname, port,
            MessageType.Write, writeRequest, null);

        // clear buffer
        buffer.clear();
    }
}
