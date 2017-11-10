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

import lombok.ToString;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveRedisPubSubCommands;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisElementWriter;
import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.lang.Nullable;

/**
 * @author Mark Paluch
 * @since 2.1
 */
public class ReactiveRedisMessageListenerContainer implements DisposableBean {

	private final ReactiveRedisConnectionFactory connectionFactory;
	private final SerializationPair<String> serializationPair = SerializationPair
			.fromSerializer(RedisSerializer.string());
	private final Subscriptions<ReactiveRedisPubSubCommands.ChannelMessage> channels = new Subscriptions<>();
	private final Subscriptions<ReactiveRedisPubSubCommands.PatternMessage> patterns = new Subscriptions<>();

	@Nullable private volatile ReactiveRedisConnection connection;

	public ReactiveRedisMessageListenerContainer(ReactiveRedisConnectionFactory connectionFactory) {

		this.connectionFactory = connectionFactory;
		this.connection = connectionFactory.getReactiveConnection();
	}

	@Override
	public void destroy() {

		ReactiveRedisConnection connection = this.connection;

		if (connection != null) {
			channels.terminate();
			patterns.terminate();
			connection.close();
			this.connection = null;
		}
	}

	public Flux<ChannelMessage<String, String>> subscribe(ChannelTopic... foo) {
		return subscribe(Arrays.asList(foo), serializationPair, serializationPair);
	}

	public Flux<PatternMessage<String, String>> subscribe(PatternTopic... foo) {
		return subscribe(Arrays.asList(foo), serializationPair, serializationPair)
				.map(m -> (PatternMessage<String, String>) m);
	}

	public <C, B> Flux<ChannelMessage<C, B>> subscribe(Iterable<? extends Topic> topics,
			SerializationPair<C> channelSerializer, SerializationPair<B> bodySerializer) {

		ReactiveRedisConnection connection = this.connection;
		if (connection == null) {
			throw new IllegalStateException("ReactiveRedisMessageListenerContainer is already disposed!");
		}

		Flux<ChannelMessage<C, B>> flux = null;

		for (Topic topic : topics) {

			if (topic instanceof PatternTopic) {

				PatternTopic patternTopic = (PatternTopic) topic;

				Flux<ReactiveRedisPubSubCommands.PatternMessage> patternMessages = patterns
						.subscribe(serializationPair.write(patternTopic.getTopic()), connection.pubSubCommands()::pSubscribe);

				Flux<ChannelMessage<C, B>> subscription = patternMessages.map(patternMessage -> {

					String pattern = read(serializationPair.getReader(), patternMessage.getPattern());
					C channel = read(channelSerializer.getReader(), patternMessage.getChannel());
					B body = read(bodySerializer.getReader(), patternMessage.getBody());

					return new PatternMessage<>(pattern, channel, body);
				});

				if (flux == null) {
					flux = subscription;
				} else {
					flux = flux.mergeWith(subscription);
				}
			}

			if (topic instanceof ChannelTopic) {

				ChannelTopic channelTopic = (ChannelTopic) topic;

				Flux<ReactiveRedisPubSubCommands.ChannelMessage> channelMessages = channels
						.subscribe(serializationPair.write(channelTopic.getTopic()), connection.pubSubCommands()::subscribe);

				Flux<ChannelMessage<C, B>> subscription = channelMessages.map(channelMessage -> {

					C channel = read(channelSerializer.getReader(), channelMessage.getChannel());
					B body = read(bodySerializer.getReader(), channelMessage.getBody());

					return new ChannelMessage<>(channel, body);
				});

				if (flux == null) {
					flux = subscription;
				} else {
					flux = flux.mergeWith(subscription);
				}
			}
		}

		return flux == null ? Flux.empty() : flux;
	}

	private static <C> C read(RedisElementReader<C> reader, ByteBuffer buffer) {

		try {
			buffer.mark();
			return reader.read(buffer);
		} finally {
			buffer.reset();
		}
	}

	public Mono<Long> publish(ChannelMessage<String, String> message) {
		return publish(message, serializationPair.getWriter(), serializationPair.getWriter());
	}

	public <C, B> Mono<Long> publish(ChannelMessage<C, B> message, RedisElementWriter<C> channelSerializer,
			RedisElementWriter<B> bodySerializer) {

		ReactiveRedisConnection connection = this.connection;
		if (connection == null) {
			throw new IllegalStateException("ReactiveRedisMessageListenerContainer is already disposed!");
		}

		return connection.pubSubCommands().publish(channelSerializer.write(message.getChannel()),
				bodySerializer.write(message.getBody()));
	}

	@ToString
	public static class PatternMessage<C, B> extends ChannelMessage<C, B> {

		private final String pattern;

		public PatternMessage(String pattern, C channel, B body) {
			super(channel, body);
			this.pattern = pattern;
		}

		public String getPattern() {
			return pattern;
		}
	}

	@ToString
	public static class ChannelMessage<C, B> {

		private final C channel;
		private final B body;

		public ChannelMessage(C channel, B body) {
			this.channel = channel;
			this.body = body;
		}

		public C getChannel() {
			return channel;
		}

		public B getBody() {
			return body;
		}
	}

	static class Subscriptions<E> {

		private final Map<String, Subscription<E>> subscriptions = new ConcurrentHashMap<>();

		public Flux<E> subscribe(ByteBuffer target, Function<ByteBuffer, Flux<E>> subscribeFunction) {

			String id = StandardCharsets.UTF_8.decode(target.duplicate()).toString();

			Subscription<E> subscription = subscriptions.computeIfAbsent(id, key -> {

				Flux<E> redisSubscription = subscribeFunction.apply(target);
				return new Subscription<>(redisSubscription);
			});

			Flux<E> messageStream = Flux.defer(subscription::activate) //
					.doOnCancel(() -> {

						if (subscription.terminate()) {
							subscriptions.remove(id);
						}
					});

			return messageStream;
		}

		void terminate() {

			while (!subscriptions.isEmpty()) {

				Map<String, Subscription<E>> subscriptions = new HashMap<>(this.subscriptions);

				subscriptions.forEach((k, v) -> {

					v.cancel();
					this.subscriptions.remove(k);
				});

			}
		}
	}

	static class Subscription<E> {

		private final AtomicLong subscriber = new AtomicLong();
		private final Flux<E> redisSubscription;
		private volatile @Nullable ConnectableFlux<E> processor;

		private volatile AtomicReference<Disposable> disposable = new AtomicReference<>();

		public Subscription(Flux<E> redisSubscription) {
			this.redisSubscription = redisSubscription;
		}

		public Flux<E> activate() {

			if (subscriber.incrementAndGet() == 1 && disposable.get() == null) {

				ConnectableFlux<E> flux = redisSubscription.publish();
				disposable.set(flux.connect());
				this.processor = flux;
			}

			do {
				Flux<E> processor = this.processor;
				if (processor != null) {
					return processor;
				}
			} while (processor == null);

			throw new IllegalStateException("Should never happen...obviously it happened...");
		}

		public void cancel() {

			Disposable disposable = this.disposable.get();

			if (disposable != null && this.disposable.compareAndSet(disposable, null)) {
				disposable.dispose();
			}
		}

		/**
		 * Terminate subscription. Returns {@literal true} if the subscription has no more
		 *
		 * @return {@literal true} if this subscription has no more downstream subscribers.
		 */
		public boolean terminate() {

			long count = subscriber.get();
			if (count <= 0) {
				return false;
			}

			boolean updated = subscriber.compareAndSet(count, count - 1);

			if (updated && count == 1) {
				cancel();
			}

			return updated;
		}
	}
}
