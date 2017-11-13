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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.connection.ReactiveSubscription.ChannelMessage;
import org.springframework.data.redis.connection.ReactiveSubscription.PatternMessage;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Container providing a stream of {@link ChannelMessage} for messages received via Redis Pub/Sub listeners. Handles the
 * low level details of listening, converting and message dispatching.
 * <p />
 * Note the container allocates a single connection when it is created and releases the connection on
 * {@link #destroy()}. Connections are allocated eagerly to not interfere with non-blocking use during application
 * operations. Using reactive infrastructure allows usage of a single connection due to channel multiplexing.
 * <p />
 * This class is thread-safe and allows subscription by multiple concurrent threads.
 * 
 * @author Mark Paluch
 * @since 2.1
 * @see ReactiveSubscription
 * @see org.springframework.data.redis.connection.ReactiveRedisPubSubCommands
 */
public class ReactiveRedisMessageListenerContainer implements DisposableBean {

	private final SerializationPair<String> stringSerializationPair = SerializationPair
			.fromSerializer(RedisSerializer.string());
	private final Map<ReactiveSubscription, Boolean> subscriptions = new ConcurrentHashMap<>();

	@Nullable private volatile ReactiveRedisConnection connection;

	/**
	 * Create a new {@link ReactiveRedisMessageListenerContainer} given {@link ReactiveRedisConnectionFactory}.
	 * 
	 * @param connectionFactory must not be {@literal null}.
	 */
	public ReactiveRedisMessageListenerContainer(ReactiveRedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ReactiveRedisConnectionFactory must not be null!");
		this.connection = connectionFactory.getReactiveConnection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() {

		ReactiveRedisConnection connection = this.connection;

		if (connection != null) {

			Flux<Void> terminationSignals = null;
			while (!subscriptions.isEmpty()) {

				Map<ReactiveSubscription, Boolean> local = new HashMap<>(subscriptions);
				List<Mono<Void>> monos = local.keySet().stream() //
						.peek(subscriptions::remove) //
						.map(ReactiveSubscription::terminate) //
						.collect(Collectors.toList());

				if (terminationSignals == null) {
					terminationSignals = Flux.merge(monos);
				} else {
					terminationSignals = terminationSignals.mergeWith(Flux.merge(monos));
				}
			}

			if (terminationSignals != null) {
				terminationSignals.blockLast();
			}

			connection.close();
			this.connection = null;
		}
	}

	/**
	 * Subscribe to one or more {@link ChannelTopic}s and receive a stream of {@link ChannelMessage}. Messages and channel
	 * names are treated as {@link String}. The message stream subscribes lazily to the Redis channels and unsubscribes if
	 * the {@link org.reactivestreams.Subscription} is {@link org.reactivestreams.Subscription#cancel() cancelled}.
	 * 
	 * @param channelTopics the channels to subscribe.
	 * @return the message stream.
	 * @throws InvalidDataAccessApiUsageException if {@code patternTopics} is empty.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 */
	public Flux<ChannelMessage<String, String>> receive(ChannelTopic... channelTopics) {

		Assert.notNull(channelTopics, "ChannelTopics must not be null!");
		Assert.noNullElements(channelTopics, "ChannelTopics must not contain null elements!");

		return receive(Arrays.asList(channelTopics), stringSerializationPair, stringSerializationPair);
	}

	/**
	 * Subscribe to one or more {@link PatternTopic}s and receive a stream of {@link PatternMessage}. Messages, pattern,
	 * and channel names are treated as {@link String}. The message stream subscribes lazily to the Redis channels and
	 * unsubscribes if the {@link org.reactivestreams.Subscription} is {@link org.reactivestreams.Subscription#cancel()
	 * cancelled}.
	 * 
	 * @param channelTopics the channels to subscribe.
	 * @return the message stream.
	 * @throws InvalidDataAccessApiUsageException if {@code patternTopics} is empty.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 */
	@SuppressWarnings("unchecked")
	public Flux<PatternMessage<String, String, String>> receive(PatternTopic... patternTopics) {

		Assert.notNull(patternTopics, "PatternTopic must not be null!");
		Assert.noNullElements(patternTopics, "PatternTopic must not contain null elements!");

		return receive(Arrays.asList(patternTopics), stringSerializationPair, stringSerializationPair)
				.map(m -> (PatternMessage<String, String, String>) m);
	}

	/**
	 * Subscribe to one or more {@link Topic}s and receive a stream of {@link ChannelMessage} The stream may contain
	 * {@link PatternMessage} if subscribed to patterns. Messages, and channel names are serialized/deserialized using the
	 * given {@code channelSerializer} and {@code messageSerializer}. The message stream subscribes lazily to the Redis
	 * channels and unsubscribes if the {@link org.reactivestreams.Subscription} is
	 * {@link org.reactivestreams.Subscription#cancel() cancelled}.
	 * 
	 * @param topics the channels to subscribe.
	 * @return the message stream.
	 * @see #receive(Iterable, SerializationPair, SerializationPair)
	 * @throws InvalidDataAccessApiUsageException if {@code topics} is empty.
	 */
	public <C, B> Flux<ChannelMessage<C, B>> receive(Iterable<? extends Topic> topics,
			SerializationPair<C> channelSerializer, SerializationPair<B> messageSerializer) {

		Assert.notNull(topics, "Topics must not be null!");

		ReactiveRedisConnection connection = this.connection;
		if (connection == null) {
			throw new IllegalStateException("ReactiveRedisMessageListenerContainer is already disposed!");
		}

		Mono<ReactiveSubscription> subscription = connection.pubSubCommands().createSubscription();
		subscription = subscription.doOnNext(it -> {
			subscriptions.put(it, true);
		});

		ByteBuffer[] patterns = getTargets(topics, PatternTopic.class);
		ByteBuffer[] channels = getTargets(topics, ChannelTopic.class);

		if (patterns.length == 0 && channels.length == 0) {
			throw new InvalidDataAccessApiUsageException("No channels or patterns to subscribe");
		}

		Flux<ChannelMessage<ByteBuffer, ByteBuffer>> messageStream = subscription.flatMapMany(it -> {

			Mono<Void> subscribe = null;
			if (patterns.length != 0) {
				subscribe = it.pSubscribe(patterns);
			}

			if (channels.length != 0) {

				Mono<Void> channelsSubscribe = it.subscribe(channels);

				if (subscribe == null) {
					subscribe = channelsSubscribe;
				} else {
					subscribe = subscribe.and(channelsSubscribe);
				}
			}

			return it.receive().mergeWith(subscribe.then(Mono.empty())) //
					.doFinally(signal -> subscriptions.remove(it)) //
					.doOnCancel(() -> it.terminate().subscribe());
		});

		return messageStream
				.map(message -> readMessage(channelSerializer.getReader(), messageSerializer.getReader(), message));
	}

	private ByteBuffer[] getTargets(Iterable<? extends Topic> topics, Class<?> classFilter) {

		return StreamSupport.stream(topics.spliterator(), false) //
				.filter(classFilter::isInstance) //
				.map(Topic::getTopic) //
				.map(stringSerializationPair::write) //
				.toArray(ByteBuffer[]::new);
	}

	@SuppressWarnings("unchecked")
	private <C, B> ChannelMessage<C, B> readMessage(RedisElementReader<C> channelSerializer,
			RedisElementReader<B> messageSerializer, ChannelMessage<ByteBuffer, ByteBuffer> message) {

		if (message instanceof PatternMessage) {

			PatternMessage<ByteBuffer, ByteBuffer, ByteBuffer> patternMessage = (PatternMessage) message;

			String pattern = read(stringSerializationPair.getReader(), patternMessage.getPattern());
			C channel = read(channelSerializer, patternMessage.getChannel());
			B body = read(messageSerializer, patternMessage.getMessage());

			return new PatternMessage<>(pattern, channel, body);
		}

		C channel = read(channelSerializer, message.getChannel());
		B body = read(messageSerializer, message.getMessage());

		return new ChannelMessage<>(channel, body);
	}

	private static <C> C read(RedisElementReader<C> reader, ByteBuffer buffer) {

		try {
			buffer.mark();
			return reader.read(buffer);
		} finally {
			buffer.reset();
		}
	}
}
