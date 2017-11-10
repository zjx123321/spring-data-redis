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

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
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
import org.springframework.data.redis.connection.ReactiveRedisPubSubCommands.ChannelMessage;
import org.springframework.data.redis.connection.ReactiveRedisPubSubCommands.PatternMessage;

/**
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveRedisMessageListenerContainerUnitTests {

	ReactiveRedisMessageListenerContainer container;

	@Mock ReactiveRedisConnectionFactory connectionFactory;
	@Mock ReactiveRedisConnection connection;
	@Mock ReactiveRedisPubSubCommands commands;

	@Before
	public void before() {

		when(connectionFactory.getReactiveConnection()).thenReturn(connection);
		when(connection.pubSubCommands()).thenReturn(commands);
	}

	@Test // DATAREDIS-612
	public void shouldSubscribeToPattern() {

		container = createContainer();
		when(commands.pSubscribe(any(ByteBuffer.class))).thenReturn(Flux.empty());

		container.subscribe(new PatternTopic("foo*"));

		verify(commands).pSubscribe(byteBuffer("foo*"));
	}

	@Test // DATAREDIS-612
	public void shouldSubscribeToMultiplePatterns() {

		container = createContainer();
		when(commands.pSubscribe(any(ByteBuffer.class))).thenReturn(Flux.empty());

		container.subscribe(new PatternTopic("foo*"), new PatternTopic("bar*"));

		verify(commands).pSubscribe(byteBuffer("foo*"));
		verify(commands).pSubscribe(byteBuffer("bar*"));
	}

	@Test // DATAREDIS-612
	public void shouldSubscribeToChannel() {

		container = createContainer();
		when(commands.subscribe(any(ByteBuffer.class))).thenReturn(Flux.empty());

		container.subscribe(new ChannelTopic("foo"));

		verify(commands).subscribe(byteBuffer("foo"));
	}

	@Test // DATAREDIS-612
	public void shouldSubscribeToMultipleChannels() {

		container = createContainer();
		when(commands.subscribe(any(ByteBuffer.class))).thenReturn(Flux.empty());

		container.subscribe(new ChannelTopic("foo"), new ChannelTopic("bar"));

		verify(commands).subscribe(byteBuffer("foo"));
		verify(commands).subscribe(byteBuffer("bar"));
	}

	@Test // DATAREDIS-612
	public void shouldEmitChannelMessage() {

		DirectProcessor<ChannelMessage> processor = DirectProcessor.create();

		container = createContainer();
		when(commands.subscribe(any(ByteBuffer.class))).thenReturn(processor);

		Flux<ReactiveRedisMessageListenerContainer.ChannelMessage<String, String>> messageStream = container
				.subscribe(new ChannelTopic("foo"));

		StepVerifier.create(messageStream).then(() -> {
			processor.onNext(createChannelMessage("foo", "message"));
		}).assertNext(msg -> {

			assertThat(msg.getChannel()).isEqualTo("foo");
			assertThat(msg.getBody()).isEqualTo("message");
		}).thenCancel().verify();
	}

	@Test // DATAREDIS-612
	public void shouldEmitPatternMessage() {

		DirectProcessor<PatternMessage> processor = DirectProcessor.create();

		container = createContainer();
		when(commands.pSubscribe(any(ByteBuffer.class))).thenReturn(processor);

		Flux<ReactiveRedisMessageListenerContainer.PatternMessage<String, String>> messageStream = container
				.subscribe(new PatternTopic("foo*"));

		StepVerifier.create(messageStream).then(() -> {
			processor.onNext(createPatternMessage("foo*", "foo", "message"));
		}).assertNext(msg -> {

			assertThat(msg.getPattern()).isEqualTo("foo*");
			assertThat(msg.getChannel()).isEqualTo("foo");
			assertThat(msg.getBody()).isEqualTo("message");
		}).thenCancel().verify();
	}

	@Test // DATAREDIS-612
	public void shouldTerminateSubscriptionsOnShutdown() {

		DirectProcessor<PatternMessage> processor = DirectProcessor.create();

		container = createContainer();
		when(commands.pSubscribe(any(ByteBuffer.class))).thenReturn(processor);

		Flux<ReactiveRedisMessageListenerContainer.PatternMessage<String, String>> messageStream = container
				.subscribe(new PatternTopic("foo*"));

		StepVerifier.create(messageStream).then(() -> {
			container.destroy();
		}).verifyError(CancellationException.class);
	}

	@Test // DATAREDIS-612
	public void shouldCleanupDownstream() {

		DirectProcessor<PatternMessage> processor = DirectProcessor.create();

		container = createContainer();
		when(commands.pSubscribe(any(ByteBuffer.class))).thenReturn(processor);

		Flux<ReactiveRedisMessageListenerContainer.PatternMessage<String, String>> messageStream = container
				.subscribe(new PatternTopic("foo*"));

		StepVerifier.create(messageStream).then(() -> {
			assertThat(processor.hasDownstreams()).isTrue();
			processor.onNext(createPatternMessage("foo*", "foo", "message"));
		}).expectNextCount(1).thenCancel().verify();

		assertThat(processor.hasDownstreams()).isFalse();
	}

	private ReactiveRedisMessageListenerContainer createContainer() {
		return new ReactiveRedisMessageListenerContainer(connectionFactory);
	}

	private static ByteBuffer byteBuffer(String content) {
		return ByteBuffer.wrap(content.getBytes());
	}

	private static ChannelMessage createChannelMessage(String channel, String body) {
		return new ChannelMessage(byteBuffer(channel), byteBuffer(body));
	}

	private static PatternMessage createPatternMessage(String pattern, String channel, String body) {
		return new PatternMessage(byteBuffer(pattern), byteBuffer(channel), byteBuffer(body));
	}

}
