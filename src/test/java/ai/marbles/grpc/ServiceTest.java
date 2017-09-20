/*
 * Copyright (c) Marbles AI Corp. 2016-2017.
 * All rights reserved.
 * Author: Paul Glendenning
 */

package ai.marbles.grpc;

//Java packages
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import com.google.common.util.concurrent.ListenableFuture;

import static org.junit.Assert.*;
import org.junit.Test;

public class ServiceTest {
	public class TestHandler extends DiscoveryGrpc.DiscoveryImplBase  {
		@Override
		public void ping(Empty request, StreamObserver<Empty> responseObserver) {
			responseObserver.onNext(Empty.newBuilder().build());
			responseObserver.onCompleted();
		}

		@Override
		public void configure(Configuration request, StreamObserver<ConfigResult> responseObserver) {
			responseObserver.onNext(ConfigResult.newBuilder()
					.setStatus(ConfigResult.Status.WARNING).build());
			responseObserver.onCompleted();
		}
	}

	@Test
	public void testSyncClientSyncServer() {

		try {
			ServiceAcceptor server = new ServiceAcceptor(9001, new TestHandler());
			server.start();

			ServiceConnector client = new ServiceConnector("localhost", 9001);
			DiscoveryGrpc.DiscoveryBlockingStub stub = DiscoveryGrpc.newBlockingStub(client.getChannel());

			ConfigResult conf = stub.configure(Configuration.newBuilder().build());
			assertTrue(conf != null);
			assertTrue(conf.getStatus().equals(ConfigResult.Status.WARNING));

			assertTrue(client.shutdown().blockUntilShutdown(3000));
			assertTrue(server.shutdown().blockUntilShutdown(3000));
		} catch(Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testAsyncClientSyncServer() {
		try {
			ServiceAcceptor server = new ServiceAcceptor(9002, new TestHandler());
			server.start();

			ServiceConnector client = new ServiceConnector("localhost", 9002);
			DiscoveryGrpc.DiscoveryFutureStub stub = DiscoveryGrpc.newFutureStub(client.getChannel());

			ListenableFuture<ConfigResult> rpc2 = stub.configure(Configuration.newBuilder().build());
			ConfigResult conf = rpc2.get();
			assertTrue(conf != null);
			assertTrue(conf.getStatus().equals(ConfigResult.Status.WARNING));

			assertTrue(client.ping(0));
			assertTrue(client.ping(1000));

			assertTrue(client.shutdown().blockUntilShutdown(3000));
			assertTrue(server.shutdown().blockUntilShutdown(3000));
		} catch(Exception e) {
			fail(e.getMessage());
		}
	}
}
