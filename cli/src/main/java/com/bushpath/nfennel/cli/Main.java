package com.bushpath.nfennel.cli;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import com.bushpath.nfennel.flatbuffers.Failure;
import com.bushpath.nfennel.flatbuffers.MessageType;

import java.nio.ByteBuffer;

@Command(name = "nfennel",
    version = "0.1",
    description = "Cli application for NaiveFennel.",
    mixinStandardHelpOptions = true,
    subcommands = {QueryCli.class})
public class Main implements Runnable {
    public static void main(String[] args) {
        CommandLine.run(new Main(), System.err, args);
    }
 
    @Override
    public void run() {
        CommandLine.usage(new Main(), System.out);
        return;
    }

    public static byte[] sendMessage(String hostname, short port,
            short messageType, byte[] request, byte[] data) throws Exception  {
        // connect zeromq socket
        Context context = ZMQ.context(1);
        Socket socket = context.socket(ZMQ.REQ);
        socket.connect("tcp://" + hostname + ":" + port);

        try {
            // send command type
            byte[] messageTypeBytes = new byte[2];
            messageTypeBytes[0] = (byte)(messageType & 0xff);
            messageTypeBytes[1] = (byte)((messageType >> 8) & 0xff);
            socket.send(messageTypeBytes, ZMQ.SNDMORE);

            // send request
            if (data != null) {
                socket.send(request, ZMQ.SNDMORE);
                socket.send(data);
            } else {
                socket.send(request);
            }

            // read response message type and bytes
            byte[] responseMessageTypeBytes = socket.recv(0);
            short responseMessageType = (short) ((responseMessageTypeBytes[1] << 8)
                + responseMessageTypeBytes[0]);
            byte[] response = socket.recv(0);

            if (responseMessageType == MessageType.Failure) {
                // if there's an error on the server
                ByteBuffer byteBuffer = ByteBuffer.wrap(response);
                Failure failure = Failure.getRootAsFailure(byteBuffer);

                throw new Exception(failure.message());
            } else if (responseMessageType != messageType) {
                // check if the response message type matches the request
                throw new Exception("Unexpected response message type");
            }

            return response;
        } finally {
            socket.close();
            context.term();
        }
    }
}
