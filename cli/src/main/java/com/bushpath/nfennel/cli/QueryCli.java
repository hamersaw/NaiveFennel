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
import com.bushpath.nfennel.flatbuffers.QueryResponse;
import com.bushpath.nfennel.flatbuffers.QueryRequest;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

@Command(name = "query",
    description = "Issue a query to NaiveFennel nodes.",
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
 
    @Override
    public void run() {
        try {
            // parse toml file
            Toml toml = new Toml();
            toml.read(new File(this.configurationFile));

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
  
            byte[] bytes = byteOut.toByteArray();
            int queryIndex = QueryRequest.createQueryVector(qFlatBufferBuilder, bytes);

            // add QueryRequest
            QueryRequest.startQueryRequest(qFlatBufferBuilder);
            QueryRequest.addQuery(qFlatBufferBuilder, queryIndex);
			QueryRequest.addBufferSize(qFlatBufferBuilder, this.bufferSize);
            int rootIndex = QueryRequest.endQueryRequest(qFlatBufferBuilder);

            // finalize byte array
            qFlatBufferBuilder.finish(rootIndex);
            byte[] queryRequest = qFlatBufferBuilder.sizedByteArray();

            // iterate over nodes and issue queries
            for (Object object : toml.getList("nodes")) {
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

                    // TODO - handle data
                    System.out.println("Received "
                        + dataResponse.dataLength() + " bytes");

                    // if no data returned -> break
                    if (dataResponse.dataLength() == 0) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
