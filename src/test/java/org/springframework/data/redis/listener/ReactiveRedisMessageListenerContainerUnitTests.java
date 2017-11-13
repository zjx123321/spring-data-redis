/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.listener;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.util.ByteUtils.*;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveRedisPubSubCommands;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.redis.connection.ReactiveSubscription.PatternMessage;

/**
 * Unit tests for {@link ReactiveRedisMessageListenerContainer}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveRedisMessageListenerContainerUnitTests {

	ReactiveRedisMessageListenerContainer container;

	@Mock ReactiveRedisConnectionFactory connectionFactoryMock;
	@Mock ReactiveRedisConnection connectionMock;
	@Mock ReactiveRedisPubSubCommands commandsMock;
	@Mock ReactiveSubscription subscriptionMock;

	@Before
	public void before() {

		when(connectionFactoryMock.getReactiveConnection()).thenReturn(connectionMock);
		when(connectionMock.pubSubCommands()).thenReturn(commandsMock);
		when(commandsMock.createSubscription()).thenReturn(Mono.just(subscriptionMock));
		when(subscriptionMock.subscribe(any())).thenReturn(Mono.empty());
		when(subscriptionMock.pSubscribe(any())).thenReturn(Mono.empty());
	}

	@Test // DATAREDIS-612
	public void shouldSubscribeToPattern() {

		when(subscriptionMock.receive()).thenReturn(Flux.never());
		when(subscriptionMock.terminate()).thenReturn(Mono.empty());
		container = createContainer();

		StepVerifier.create(container.receive(new PatternTopic("foo*"))).thenAwait().thenCancel().verify();

		verify(subscriptionMock).pSubscribe(getByteBuffer("foo*"));
	}

	@Test // DATAREDIS-612
	public void shouldSubscribeToMultiplePatterns() {

		when(subscriptionMock.receive()).thenReturn(Flux.never());
		when(subscriptionMock.terminate()).thenReturn(Mono.empty());
		container = createContainer();

		StepVerifier.create(container.receive(new PatternTopic("foo*"), new PatternTopic("bar*"))).thenRequest(1)
				.thenAwait().thenCancel().verify();

		verify(subscriptionMock).pSubscribe(getByteBuffer("foo*"), getByteBuffer("bar*"));
	}

	@Test // DATAREDIS-612
	public void shouldSubscribeToChannel() {

		when(subscriptionMock.receive()).thenReturn(Flux.never());
		when(subscriptionMock.terminate()).thenReturn(Mono.empty());
		container = createContainer();

		StepVerifier.create(container.receive(new ChannelTopic("foo"))).thenAwait().thenCancel().verify();

		verify(subscriptionMock).subscribe(getByteBuffer("foo"));
	}

	@Test // DATAREDIS-612
	public void shouldSubscribeToMultipleChannels() {

		when(subscriptionMock.receive()).thenReturn(Flux.never());
		when(subscriptionMock.terminate()).thenReturn(Mono.empty());
		container = createContainer();

		StepVerifier.create(container.receive(new ChannelTopic("foo"), new ChannelTopic("bar"))).thenAwait().thenCancel()
				.verify();

		verify(subscriptionMock).subscribe(getByteBuffer("foo"), getByteBuffer("bar"));
	}

	@Test // DATAREDIS-612
	public void shouldEmitChannelMessage() {

		DirectProcessor<ChannelMessage<ByteBuffer, ByteBuffer>> processor = DirectProcessor.create();

		when(subscriptionMock.receive()).thenReturn(processor);
		when(subscriptionMock.terminate()).thenReturn(Mono.empty());
		container = createContainer();

		Flux<ChannelMessage<String, String>> messageStream = container.receive(new ChannelTopic("foo"));

		StepVerifier.create(messageStream).then(() -> {
			processor.onNext(createChannelMessage("foo", "message"));
		}).assertNext(msg -> {

			assertThat(msg.getChannel()).isEqualTo("foo");
			assertThat(msg.getMessage()).isEqualTo("message");
		}).thenCancel().verify();
	}

	@Test // DATAREDIS-612
	public void shouldEmitPatternMessage() {

		DirectProcessor<ChannelMessage<ByteBuffer, ByteBuffer>> processor = DirectProcessor.create();

		when(subscriptionMock.receive()).thenReturn(processor);
		when(subscriptionMock.terminate()).thenReturn(Mono.empty());
		container = createContainer();

		Flux<PatternMessage<String, String, String>> messageStream = container.receive(new PatternTopic("foo*"));

		StepVerifier.create(messageStream).then(() -> {
			processor.onNext(createPatternMessage("foo*", "foo", "message"));
		}).assertNext(msg -> {

			assertThat(msg.getPattern()).isEqualTo("foo*");
			assertThat(msg.getChannel()).isEqualTo("foo");
			assertThat(msg.getMessage()).isEqualTo("message");
		}).thenCancel().verify();
	}

	@Test // DATAREDIS-612
	public void shouldUnsubscribeOnCancel() {

		when(subscriptionMock.receive()).thenReturn(DirectProcessor.create());
		when(subscriptionMock.terminate()).thenReturn(Mono.empty());
		container = createContainer();

		Flux<PatternMessage<String, String, String>> messageStream = container.receive(new PatternTopic("foo*"));

		StepVerifier.create(messageStream).then(() -> {

			// Then required to trigger cancel.

		}).thenCancel().verify();

		verify(subscriptionMock).terminate();
	}

	@Test // DATAREDIS-612
	public void shouldTerminateSubscriptionsOnShutdown() {

		DirectProcessor<ChannelMessage<ByteBuffer, ByteBuffer>> processor = DirectProcessor.create();

		when(subscriptionMock.receive()).thenReturn(processor);
		when(subscriptionMock.terminate()).thenReturn(Mono.defer(() -> {

			processor.onError(new CancellationException());
			return Mono.empty();
		}));
		container = createContainer();

		Flux<PatternMessage<String, String, String>> messageStream = container.receive(new PatternTopic("foo*"));

		StepVerifier.create(messageStream).then(() -> {
			container.destroy();
		}).verifyError(CancellationException.class);
	}

	@Test // DATAREDIS-612
	public void shouldCleanupDownstream() {

		DirectProcessor<ChannelMessage<ByteBuffer, ByteBuffer>> processor = DirectProcessor.create();

		when(subscriptionMock.receive()).thenReturn(processor);
		when(subscriptionMock.terminate()).thenReturn(Mono.empty());
		container = createContainer();

		Flux<PatternMessage<String, String, String>> messageStream = container.receive(new PatternTopic("foo*"));

		StepVerifier.create(messageStream).then(() -> {
			assertThat(processor.hasDownstreams()).isTrue();
			processor.onNext(createPatternMessage("foo*", "foo", "message"));
		}).expectNextCount(1).thenCancel().verify();

		assertThat(processor.hasDownstreams()).isFalse();
	}

	private ReactiveRedisMessageListenerContainer createContainer() {
		return new ReactiveRedisMessageListenerContainer(connectionFactoryMock);
	}

	private static ChannelMessage<ByteBuffer, ByteBuffer> createChannelMessage(String channel, String body) {
		return new ChannelMessage<>(getByteBuffer(channel), getByteBuffer(body));
	}

	private static PatternMessage<ByteBuffer, ByteBuffer, ByteBuffer> createPatternMessage(String pattern, String channel,
			String body) {
		return new PatternMessage<>(getByteBuffer(pattern), getByteBuffer(channel), getByteBuffer(body));
	}
}
