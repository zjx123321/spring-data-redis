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
package org.springframework.data.redis.connection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;

import org.reactivestreams.Publisher;

/**
 * Redis <a href="https://redis.io/commands/#pubsub">Pub/Sub</a> commands executed using reactive infrastructure.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public interface ReactiveRedisPubSubCommands {

	/**
	 * Indicates whether the current connection is subscribed (to at least one channel) or not.
	 *
	 * @return {@literal true} if the connection is subscribed, {@literal false} otherwise.
	 */
	boolean isSubscribed();

	/**
	 * Publishes the given message to the given channel.
	 *
	 * @param channel the channel to publish to. Must not be {@literal null}.
	 * @param message message to publish. Must not be {@literal null}.
	 * @return the number of clients that received the message.
	 * @see <a href="http://redis.io/commands/publish">Redis Documentation: PUBLISH</a>
	 */
	default Mono<Long> publish(ByteBuffer channel, ByteBuffer message) {
		return publish(Mono.just(new ChannelMessage(channel, message))).next();
	}

	/**
	 * Publishes the given messages to the {@link ChannelMessage#getChannel() appropriate channels}.
	 *
	 * @param messageStream the messages to publish to. Must not be {@literal null}.
	 * @return the number of clients that received the message.
	 * @see <a href="http://redis.io/commands/publish">Redis Documentation: PUBLISH</a>
	 */
	Flux<Long> publish(Publisher<ChannelMessage> messageStream);

	/**
	 * Subscribes the connection to the given {@code channels}. Once subscribed, a connection enters listening mode and
	 * can only subscribe to other channels or unsubscribe. No other commands are accepted until the connection is
	 * unsubscribed.
	 * <p/>
	 * Note that cancellation of the {@link Flux} will unsubscribe from {@code channels}.
	 *
	 * @param channels channel names, must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/subscribe">Redis Documentation: SUBSCRIBE</a>
	 */
	Flux<ChannelMessage> subscribe(ByteBuffer... channels);

	/**
	 * Subscribes the connection to all channels matching the given {@code patterns}. Once subscribed, a connection enters
	 * listening mode and can only subscribe to other channels or unsubscribe. No other commands are accepted until the
	 * connection is unsubscribed.
	 * <p />
	 * Note that cancellation of the {@link Flux} will unsubscribe from {@code patterns}.
	 *
	 * @param patterns channel name patterns, must not be {@literal null}.
	 * @see <a href="http://redis.io/commands/psubscribe">Redis Documentation: PSUBSCRIBE</a>
	 */
	Flux<PatternMessage> pSubscribe(ByteBuffer... patterns);

	class ChannelMessage {

		private final ByteBuffer channel;
		private final ByteBuffer body;

		public ChannelMessage(ByteBuffer channel, ByteBuffer body) {
			this.channel = channel;
			this.body = body;
		}

		public ByteBuffer getChannel() {
			return channel;
		}

		public ByteBuffer getBody() {
			return body;
		}
	}

	class PatternMessage extends ChannelMessage {

		private final ByteBuffer pattern;

		public PatternMessage(ByteBuffer pattern, ByteBuffer channel, ByteBuffer body) {
			super(channel, body);
			this.pattern = pattern;
		}

		public ByteBuffer getPattern() {
			return pattern;
		}
	}
}
