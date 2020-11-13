package ru.rt.restream.reindexer.connector.binding.cproto;

import ru.rt.restream.reindexer.connector.exceptions.NetworkException;
import sun.misc.IOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class Connection implements AutoCloseable {

    private final RpcEncoder encoder = new RpcEncoder();
    private final RpcDecoder decoder = new RpcDecoder();

    private final Socket clientSocket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private int seq;

    public Connection(String host, int port) {
        try {
            clientSocket = new Socket(host, port);
            out = new DataOutputStream(clientSocket.getOutputStream());
            in = new DataInputStream(clientSocket.getInputStream());
        } catch (IOException e) {
            throw new NetworkException(e);
        }
    }

    public RpcResult rpcCall(int command, Object... args) {
        try {
            byte[] bytes = encoder.encode(command, seq, args);
            out.write(bytes);
            seq++;
            return readReply(in);
        } catch (IOException e) {
            throw new NetworkException(e);
        }
    }

    private RpcResult readReply(DataInputStream inFromServer) {
        try {
            return decoder.decode(inFromServer);
        } catch (IOException e) {
            throw new NetworkException(e);
        }
    }

    @Override
    public void close() throws Exception {
        clientSocket.close();
    }
}
