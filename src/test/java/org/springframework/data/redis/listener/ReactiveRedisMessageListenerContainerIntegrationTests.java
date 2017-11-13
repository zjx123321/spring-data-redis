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
import static org.springframework.data.redis.util.ByteUtils.*;

import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.ReactiveRedisClusterConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.lang.Nullable;

/**
 * Integration tests for {@link ReactiveRedisMessageListenerContainer} via Lettuce.
 * 
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class ReactiveRedisMessageListenerContainerIntegrationTests {

	static final String CHANNEL1 = "my-channel";
	static final String PATTERN1 = "my-chan*";

	private final LettuceConnectionFactory connectionFactory;
	private @Nullable RedisConnection connection;

	@Parameters(name = "{1}")
	public static Collection<Object[]> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	/**
	 * @param connectionFactory
	 * @param label parameterized test label, no further use besides that.
	 */
	public ReactiveRedisMessageListenerContainerIntegrationTests(LettuceConnectionFactory connectionFactory,
			String label) {

		this.connectionFactory = connectionFactory;
		ConnectionFactoryTracker.add(connectionFactory);
	}

	@Before
	public void before() {
		connection = connectionFactory.getConnection();
	}

	@After
	public void tearDown() {

		if (connection != null) {
			connection.close();
		}
	}

	@Test // DATAREDIS-612
	public void shouldReceiveChannelMessages() {

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(connectionFactory);

		StepVerifier.create(container.receive(new ChannelTopic(CHANNEL1))) //
				.thenAwait(Duration.ofMillis(100)) // Need to await until Listeners are set up and the actual subscription
																						// command is executed
				.then(() -> connection.publish(CHANNEL1.getBytes(), "hello world".getBytes())) //
				.assertNext(c -> {

					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo("hello world");
				}) //
				.thenCancel().verify();

		container.destroy();
	}

	@Test // DATAREDIS-612
	public void shouldReceivePatternMessages() {

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(connectionFactory);

		StepVerifier.create(container.receive(new PatternTopic(PATTERN1))) //
				.thenAwait(Duration.ofMillis(100)) // Need to await until Listeners are set up and the actual subscription
																						// command is executed
				.then(() -> connection.publish(CHANNEL1.getBytes(), "hello world".getBytes())) //
				.assertNext(c -> {

					assertThat(c.getPattern()).isEqualTo(PATTERN1);
					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo("hello world");
				}) //
				.thenCancel().verify();

		container.destroy();
	}

	@Test // DATAREDIS-612
	public void shouldPublishAndReceiveMessage() {

		ReactiveRedisConnection reactiveConnection = connectionFactory.getReactiveConnection();

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(
				new ReactiveRedisConnectionFactory() {
					@Override
					public ReactiveRedisConnection getReactiveConnection() {
						return reactiveConnection;
					}

					@Override
					public ReactiveRedisClusterConnection getReactiveClusterConnection() {
						return null;
					}

					@Nullable
					@Override
					public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
						return null;
					}
				});

		StepVerifier.create(container.receive(new PatternTopic(PATTERN1))) //
				.thenAwait(Duration.ofMillis(100)).then(() -> {

					reactiveConnection.pubSubCommands().publish(getByteBuffer(CHANNEL1), getByteBuffer("hello world")).toFuture();
				}) //
				.assertNext(c -> {

					assertThat(c.getPattern()).isEqualTo(PATTERN1);
					assertThat(c.getChannel()).isEqualTo(CHANNEL1);
					assertThat(c.getMessage()).isEqualTo("hello world");
				}) //
				.thenCancel().verify();

		container.destroy();
	}
}
