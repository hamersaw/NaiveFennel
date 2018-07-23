package com.bushpath.nfennel.target_node;

import com.google.flatbuffers.FlatBufferBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.bushpath.nfennel.flatbuffers.Failure;
import com.bushpath.nfennel.flatbuffers.MessageType;
import com.bushpath.nfennel.flatbuffers.WriterCloseResponse;
import com.bushpath.nfennel.flatbuffers.WriterCloseRequest;
import com.bushpath.nfennel.flatbuffers.WriteResponse;
import com.bushpath.nfennel.flatbuffers.WriteRequest;
import com.bushpath.nfennel.flatbuffers.WriterOpenResponse;
import com.bushpath.nfennel.flatbuffers.WriterOpenRequest;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class DataService extends Thread {
    private final Logger log = LoggerFactory.getLogger(DataService.class);
    protected String hostname;
    protected short port;
    protected int threadCount;
    protected WriterManager writerManager;

    public DataService(String hostname, short port,
            int threadCount, WriterManager writerManager) {
        this.hostname = hostname;
        this.port = port;
        this.threadCount = threadCount;
        this.writerManager = writerManager;
    }

    @Override
    public void run() {
        Context context = ZMQ.context(1);

        // initialize tcp socket
        Socket clients = context.socket(ZMQ.ROUTER);
        clients.bind("tcp://" + this.hostname + ":" + this.port);

        // initialize workers output socket
        Socket workers = context.socket(ZMQ.DEALER);
        workers.bind("inproc://target-workers");

        // start workers
        for (int i=0; i<this.threadCount; i++) {
            Thread worker = new Worker(context, this.writerManager);
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
        protected WriterManager writerManager;

        public Worker(Context context, WriterManager writerManager) {
            this.context = context;
            this.writerManager = writerManager;
        }

        @Override
        public void run() {
            Socket socket = context.socket(ZMQ.REP);
            socket.connect("inproc://target-workers");

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
						case MessageType.WriterClose:
                            this.writerManager.close(flatBufferBuilder,
                                WriterCloseRequest.getRootAsWriterCloseRequest(bb));
                            break;
						case MessageType.WriterOpen:
                            this.writerManager.open(flatBufferBuilder,
                                WriterOpenRequest.getRootAsWriterOpenRequest(bb));
                            break;
                        case MessageType.Write:
                            this.writerManager.write(flatBufferBuilder,
                                WriteRequest.getRootAsWriteRequest(bb));
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
