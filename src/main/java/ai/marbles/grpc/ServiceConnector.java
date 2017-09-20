/*
 * Copyright (c) Marbles AI Corp. 2016-2017.
 * All rights reserved.
 * Author: Paul Glendenning
 */

package ai.marbles.grpc;

import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.DAYS;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import ai.marbles.grpc.Configuration;
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
     * Configure and endpoint.
     *
     * @param conf      The configuration
     * @param timeout   A timeout in millseconds. Zero means infinite
     * @return The ConfigResult if successful, else null if it times out.
     */
    public ConfigResult configure(Configuration conf, long timeout) throws UnknownServiceException,
            InterruptedException, ExecutionException {
        DiscoveryGrpc.DiscoveryFutureStub stub = DiscoveryGrpc.newFutureStub(channel_);
        try {
            ListenableFuture<ConfigResult> rpc = stub.configure(conf);
            ConfigResult result;
            if (timeout > 0) {
                result = rpc.get(timeout, TimeUnit.MILLISECONDS);
            } else {
                result = rpc.get();
            }
            return result;
        } catch (TimeoutException e) {
            return null;
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed", e);
            throw new UnknownServiceException(e.getMessage());
        }
    }

    /**
     * Do nothing - useful when starting server and we want to wait until its ready.
     * @param timeout   A timeout in millseconds. Zero means infinite
     * @return True if successful, else false if it times out.
     */
    public boolean ping(long timeout) throws UnknownServiceException,
            InterruptedException, ExecutionException {
        DiscoveryGrpc.DiscoveryFutureStub stub = DiscoveryGrpc.newFutureStub(channel_);
        try {
            ListenableFuture<Empty> rpc = stub.ping(Empty.newBuilder().build());
            if (timeout > 0) {
                rpc.get(timeout, TimeUnit.MILLISECONDS);
            } else {
                rpc.get();
            }
        } catch (TimeoutException e) {
            return false;
        } catch (StatusRuntimeException e) {
            logger.warn("RPC failed", e);
            throw new UnknownServiceException(e.getMessage());
        }
        return true;
    }
}
