package com.bushpath.nfennel.source_node;

import com.google.flatbuffers.FlatBufferBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.bushpath.nfennel.flatbuffers.DataResponse;
import com.bushpath.nfennel.flatbuffers.DataRequest;
import com.bushpath.nfennel.flatbuffers.Failure;
import com.bushpath.nfennel.flatbuffers.MessageType;
import com.bushpath.nfennel.flatbuffers.QueryResponse;
import com.bushpath.nfennel.flatbuffers.QueryRequest;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class DataService extends Thread {
    private final Logger log = LoggerFactory.getLogger(DataService.class);
    protected String hostname;
    protected short port;
    protected int threadCount;
    protected RepositoryManager repositoryManager;
    protected Map<String, List<String>> repositories;

    public DataService(String hostname, short port,
            int threadCount, RepositoryManager repositoryManager) {
        this.hostname = hostname;
        this.port = port;
        this.threadCount = threadCount;
        this.repositoryManager = repositoryManager;
    }

    @Override
    public void run() {
        Context context = ZMQ.context(1);

        // initialize tcp socket
        Socket clients = context.socket(ZMQ.ROUTER);
        clients.bind("tcp://" + this.hostname + ":" + this.port);

        // initialize workers output socket
        Socket workers = context.socket(ZMQ.DEALER);
        workers.bind("inproc://source-workers");

        // start workers
        for (int i=0; i<this.threadCount; i++) {
            Thread worker = new Worker(context, this.repositoryManager);
            worker.start();
        }

        ZMQ.proxy(clients, workers, null);

        // never will get here, clean up anyways
        clients.close();
        workers.close();
        context.term();
    }

    private class Worker extends Thread {
        protected Context context;
        protected RepositoryManager repositoryManager;

        public Worker(Context context, RepositoryManager repositoryManager) {
            this.context = context;
            this.repositoryManager = repositoryManager;
        }

        @Override
        public void run() {
            Socket socket = context.socket(ZMQ.REP);
            socket.connect("inproc://source-workers");

			// loop receiving requests
            byte[] messageTypeBytes = null;
            byte[] requestBytes = null;
            while (true) {
                // receive request
                short messageType;
                try {
                    messageTypeBytes = socket.recv(0);
                    messageType =
                        (short) ((messageTypeBytes[1] << 8) + messageTypeBytes[0]);
                    requestBytes = socket.recv(0);
                } catch (Exception e) {
                    log.error("Error receiving bytes from socket: " + e.getMessage());
                    continue;
                }
 
                log.trace("Received message type: {}", messageType);
 
                // execute request
                FlatBufferBuilder flatBufferBuilder = new FlatBufferBuilder(1);
                try {
                    ByteBuffer bb = ByteBuffer.wrap(requestBytes);
                    switch(messageType) {
						case MessageType.Data:
                            this.repositoryManager.getData(flatBufferBuilder,
                                DataRequest.getRootAsDataRequest(bb));
                            break;
                        case MessageType.Query:
                            this.repositoryManager.query(flatBufferBuilder,
                                QueryRequest.getRootAsQueryRequest(bb));
                            break;
						default:
                            throw new UnsupportedOperationException(
                                "Unsupported message type.");
                    }
                } catch (Exception e) {
                    log.warn("Request failure: '{}': {}", e.getClass(), e.getMessage());
                    messageTypeBytes[0] = (byte)(MessageType.Failure & 0xff);
                    messageTypeBytes[1] = (byte)((MessageType.Failure >> 8) & 0xff);

                    // add message
                    int messageIndex = flatBufferBuilder.createString(e.toString());

                    // add Failure
                    Failure.startFailure(flatBufferBuilder);
                    Failure.addMessage(flatBufferBuilder, messageIndex);
                    int index = Failure.endFailure(flatBufferBuilder);

                    flatBufferBuilder.finish(index);
                }

                socket.send(messageTypeBytes, ZMQ.SNDMORE);
                byte[] response = flatBufferBuilder.sizedByteArray();
                socket.send(response);
            }
        }
    }
}
