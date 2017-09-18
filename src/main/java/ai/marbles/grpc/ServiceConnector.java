/*
 * Copyright (c) Marbles AI Corp. 2016-2017.
 * All rights reserved.
 * Author: Paul Glendenning
 */

package ai.marbles.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Channel;
/* import io.grpc.Status; */
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import java.io.UnsupportedEncodingException;
import java.net.UnknownServiceException;
import java.util.Collections;
import java.math.BigInteger;
import java.security.SecureRandom;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.DAYS;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import ai.marbles.grpc.ServiceEndpoints;
import ai.marbles.grpc.DiscoveryGrpc;

/**
 * Lucida client of gRPC service.
 */
public final class ServiceConnector {
    private static final Logger logger = LogManager.getLogger(ServiceConnector.class);

    private final ManagedChannel channel_;
    private final SecureRandom random_ = new SecureRandom();

    /**
     * Construct client for accessing a Lucida service at {@code host:port}.
     * If port=443 then the channel will be secure via TLS, otherwise the channel
     * is insecure. No authentication is provided in either case.
     *
     * @param host  Fully qualified host name
     * @param port  The port number [1,65536)
     */
    public ServiceConnector(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
    }

    /**
     * Construct client for accessing a Lucida service using an existing channel.
     * The channel credentials and encryption is dictated by the channelBuilder.
     *
     * @param channelBuilder The channel builder.
     * @see io.grpc.ManagedChannelBuilder
     */
    public ServiceConnector(ManagedChannelBuilder<?> channelBuilder) {
        channel_ = channelBuilder.build();
    }

    /**
     * Channel accessor.
     *
     * @return The io.grpc.Channel for this client.
     */
    public Channel getChannel() {
        return channel_;
    }

    /**
     * Shutdown the client connection gracefully.
     */
    public ServiceConnector shutdown() {
        if (channel_ != null) {
            channel_.shutdown();
        }
        return this;
    }

    /**
     * Shutdown the client connection.
     *
     * @param   force   Force an immediate shutdown.
     */
    public ServiceConnector shutdown(boolean force) {
        if (channel_ != null) {
            if (force)
                channel_.shutdownNow();
            else
                channel_.shutdown();
        }
        return this;
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (channel_ != null) {
            while (channel_.awaitTermination(365, DAYS)) {
                /* do nothing */
            }
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     *
     * @param timeout   Timeout in milliseconds.
     * @return          True if shutdown completed. False on timeout.
     */
    public boolean blockUntilShutdown(long timeout) throws InterruptedException {
        if (channel_ != null) {
            return channel_.awaitTermination(timeout, MILLISECONDS);
        }
        return true;
    }

    /**
     * Get supported services
     */
    public ServiceEndpoints discoverEndpoints() throws UnknownServiceException {
        DiscoveryGrpc.DiscoveryBlockingStub stub = DiscoveryGrpc.newBlockingStub(channel_);
        try {
            return stub.endpoints(Empty.newBuilder().build());
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed", e);
            throw new UnknownServiceException(e.getMessage());
        }
    }
}
