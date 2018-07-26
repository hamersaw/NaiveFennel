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

@Command(name = "query",
    description = "Issue a query to NaiveFennel nodes and write data locally.",
    mixinStandardHelpOptions = true)
public class QueryCli implements Runnable {
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

            BufferedWriter out = new BufferedWriter(new FileWriter(this.filename));
            boolean writerInitialized = false;
            for (Object object : toml.getList("source_nodes")) {
                Map<String, Object> node = (Map) object;
                String hostname = node.get("hostname").toString();
                short port = ((Long) node.get("port")).shortValue();

                System.out.println("querying '" + hostname + ":" + port + "'");

                // send QueryRequest
                byte[] qResponseBytes = Main.sendMessage(hostname, port,
                    MessageType.Query, queryRequest, null);

                // parse QueryResponse
                ByteBuffer qByteBuffer = ByteBuffer.wrap(qResponseBytes);
                QueryResponse queryResponse =
                    QueryResponse.getRootAsQueryResponse(qByteBuffer);

                if (!writerInitialized) {
                    for (int i=0; i<queryResponse.featuresLength(); i++) {
                        out.write((i == 0 ? "" : ",") + queryResponse.features(i));
                    }
                    out.write("\n");

                    writerInitialized = true;
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

                        // write to BufferedReader
                        for (int i=0; i<record.length; i++) {
                            out.write((i == 0 ? "" : ",") + record[i]);
                        }
                        out.write("\n");

                        bytesRead += record.length * 8;
                    }

                    in.close();

                    // if no data returned -> break
                    if (dataResponse.dataLength() == 0) {
                        break;
                    }
                }
            }

            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
