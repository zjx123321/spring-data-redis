/*
 * Copyright 2011-2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * String/Value-specific commands supported by Redis.
 * 
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public interface RedisStringCommands {

	public enum BitOperation {
		AND, OR, XOR, NOT;
	}

	/**
	 * Get the value of {@code key}.
	 * <p>
	 * See http://redis.io/commands/get
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	byte[] get(byte[] key);

	/**
	 * Set value of {@code key} and return its old value.
	 * <p>
	 * See http://redis.io/commands/getset
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	byte[] getSet(byte[] key, byte[] value);

	/**
	 * Get the values of all given {@code keys}.
	 * <p>
	 * See http://redis.io/commands/mget
	 * 
	 * @param keys
	 * @return
	 */
	List<byte[]> mGet(byte[]... keys);

	/**
	 * Set {@code value} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/set
	 * 
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 */
	void set(byte[] key, byte[] value);

	/**
	 * Set {@code value} for {@code key} applying timeouts from {@code expiration} if set and inserting/updating values
	 * depending on {@code option}.
	 * <p>
	 * See http://redis.io/commands/set
	 *
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @param expiration can be {@literal null}. Defaulted to {@link Expiration#persistent()}.
	 * @param option can be {@literal null}. Defaulted to {@link SetOption#UPSERT}.
	 * @since 1.7
	 */
	void set(byte[] key, byte[] value, Expiration expiration, SetOption option);

	/**
	 * Set {@code value} for {@code key}, only if {@code key} does not exist.
	 * <p>
	 * See http://redis.io/commands/setnx
	 * 
	 * @param key must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 * @return
	 */
	Boolean setNX(byte[] key, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code seconds} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/setex
	 * 
	 * @param key must not be {@literal null}.
	 * @param seconds
	 * @param value must not be {@literal null}.
	 */
	void setEx(byte[] key, long seconds, byte[] value);

	/**
	 * Set the {@code value} and expiration in {@code milliseconds} for {@code key}.
	 * <p>
	 * See http://redis.io/commands/psetex
	 * 
	 * @param key must not be {@literal null}.
	 * @param milliseconds
	 * @param value must not be {@literal null}.
	 * @since 1.3
	 */
	void pSetEx(byte[] key, long milliseconds, byte[] value);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple}.
	 * <p>
	 * See http://redis.io/commands/mset
	 * 
	 * @param tuple
	 */
	void mSet(Map<byte[], byte[]> tuple);

	/**
	 * Set multiple keys to multiple values using key-value pairs provided in {@code tuple} only if the provided key does
	 * not exist.
	 * <p>
	 * See http://redis.io/commands/msetnx
	 * 
	 * @param tuple
	 */
	Boolean mSetNX(Map<byte[], byte[]> tuple);

	/**
	 * Increment value of {@code key} by 1.
	 * <p>
	 * See http://redis.io/commands/incr
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long incr(byte[] key);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/incrby
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long incrBy(byte[] key, long value);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/incrbyfloat
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Double incrBy(byte[] key, double value);

	/**
	 * Decrement value of {@code key} by 1.
	 * <p>
	 * See http://redis.io/commands/decr
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long decr(byte[] key);

	/**
	 * Increment value of {@code key} by {@code value}.
	 * <p>
	 * See http://redis.io/commands/decrby
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long decrBy(byte[] key, long value);

	/**
	 * Append a {@code value} to {@code key}.
	 * <p>
	 * See http://redis.io/commands/append
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Long append(byte[] key, byte[] value);

	/**
	 * Get a substring of value of {@code key} between {@code begin} and {@code end}.
	 * <p>
	 * See http://redis.io/commands/getrange
	 * 
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 */
	byte[] getRange(byte[] key, long begin, long end);

	/**
	 * Overwrite parts of {@code key} starting at the specified {@code offset} with given {@code value}.
	 * <p>
	 * See http://redis.io/commands/setrange
	 * 
	 * @param key must not be {@literal null}.
	 * @param value
	 * @param offset
	 */
	void setRange(byte[] key, byte[] value, long offset);

	/**
	 * Get the bit value at {@code offset} of value at {@code key}.
	 * <p>
	 * See http://redis.io/commands/getbit
	 * 
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @return
	 */
	Boolean getBit(byte[] key, long offset);

	/**
	 * Sets the bit at {@code offset} in value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/setbit
	 * 
	 * @param key must not be {@literal null}.
	 * @param offset
	 * @param value
	 * @return the original bit value stored at {@code offset}.
	 */
	Boolean setBit(byte[] key, long offset, boolean value);

	/**
	 * Count the number of set bits (population counting) in value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/bitcount
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long bitCount(byte[] key);

	/**
	 * Count the number of set bits (population counting) of value stored at {@code key} between {@code begin} and
	 * {@code end}.
	 * <p>
	 * See http://redis.io/commands/bitcount
	 * 
	 * @param key must not be {@literal null}.
	 * @param begin
	 * @param end
	 * @return
	 */
	Long bitCount(byte[] key, long begin, long end);

	/**
	 * Perform bitwise operations between strings.
	 * <p>
	 * See http://redis.io/commands/bitop
	 * 
	 * @param op
	 * @param destination
	 * @param keys
	 * @return
	 */
	Long bitOp(BitOperation op, byte[] destination, byte[]... keys);

	/**
	 * Get the length of the value stored at {@code key}.
	 * <p>
	 * See http://redis.io/commands/strlen
	 * 
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Long strLen(byte[] key);

	/**
	 * Get / Manipulate specific integer fields of varying bit widths and arbitrary non (necessary) aligned offset stored
	 * at a given {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param operation must not be {@literal null}.
	 * @return never {@literal null}.
	 * @since 1.8
	 */
	List<Long> bitfield(byte[] key, BitfieldCommand operation);

	/**
	 * {@code SET} command arguments for {@code NX}, {@code XX}.
	 * 
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static enum SetOption {

		/**
		 * Do not set any additional command argument.
		 * 
		 * @return
		 */
		UPSERT,

		/**
		 * {@code NX}
		 * 
		 * @return
		 */
		SET_IF_ABSENT,

		/**
		 * {@code XX}
		 * 
		 * @return
		 */
		SET_IF_PRESENT;

		/**
		 * Do not set any additional command argument.
		 * 
		 * @return
		 */
		public static SetOption upsert() {
			return UPSERT;
		}

		/**
		 * {@code XX}
		 * 
		 * @return
		 */
		public static SetOption ifPresent() {
			return SET_IF_PRESENT;
		}

		/**
		 * {@code NX}
		 * 
		 * @return
		 */
		public static SetOption ifAbsent() {
			return SET_IF_ABSENT;
		}
	}

	/**
	 * The actual {@code BITFIELD} command representation holding several {@link BitfieldSubCommand}s to execute.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	class BitfieldCommand {

		private final List<BitfieldSubCommand> subCommands;

		private BitfieldCommand(List<BitfieldSubCommand> subCommands) {

			this.subCommands = new ArrayList<BitfieldSubCommand>(
					subCommands != null ? subCommands : Collections.<BitfieldSubCommand> emptyList());
		}

		private BitfieldCommand(List<BitfieldSubCommand> subCommands, BitfieldSubCommand subCommand) {

			this(subCommands);

			Assert.notNull(subCommand, "SubCommand must not be null!");
			this.subCommands.add(subCommand);
		}

		/**
		 * Creates a new {@link BitfieldCommand}.
		 *
		 * @return
		 */
		public static BitfieldCommand newBitfieldCommand() {
			return new BitfieldCommand(Collections.<BitfieldSubCommand> emptyList());
		}

		/**
		 * Obtain a new {@link BitfieldGetBuilder} for creating and adding a {@link BitfieldGet} sub command.
		 *
		 * @param type must not be {@literal null}.
		 * @return
		 */
		public BitfieldGetBuilder get(BitfieldType type) {
			return new BitfieldGetBuilder(this).forType(type);
		}

		/**
		 * Create new {@link BitfieldCommand} adding given {@link BitfieldGet} to the sub commands.
		 *
		 * @param get must not be {@literal null}.
		 * @return
		 */
		protected BitfieldCommand get(BitfieldGet get) {
			return new BitfieldCommand(subCommands, get);
		}

		/**
		 * Obtain a new {@link BitfieldSetBuilder} for creating and adding a {@link BitfieldSet} sub command.
		 *
		 * @param type must not be {@literal null}.
		 * @return
		 */
		public BitfieldSetBuilder set(BitfieldType type) {
			return new BitfieldSetBuilder(this).forType(type);
		}

		/**
		 * Create new {@link BitfieldCommand} adding given {@link BitfieldSet} to the sub commands.
		 *
		 * @param get must not be {@literal null}.
		 * @return
		 */
		protected BitfieldCommand set(BitfieldSet set) {
			return new BitfieldCommand(subCommands, set);
		}

		/**
		 * Obtain a new {@link BitfieldIncrByBuilder} for creating and adding a {@link BitfieldIncrby} sub command.
		 *
		 * @param type must not be {@literal null}.
		 * @return
		 */
		public BitfieldIncrByBuilder incr(BitfieldType type) {
			return new BitfieldIncrByBuilder(this).forType(type);
		}

		/**
		 * Create new {@link BitfieldCommand} adding given {@link BitfieldIncrBy} to the sub commands.
		 *
		 * @param get must not be {@literal null}.
		 * @return
		 */
		protected BitfieldCommand incr(BitfieldIncrBy incrBy) {
			return new BitfieldCommand(subCommands, incrBy);
		}

		/**
		 * Get the {@link List} of sub commands.
		 *
		 * @return never {@literal null}.
		 */
		public List<BitfieldSubCommand> getSubCommands() {
			return subCommands;
		}

		/**
		 * @author Christoph Strobl
		 */
		public class BitfieldSetBuilder {

			private BitfieldCommand ref;

			BitfieldSet set = new BitfieldSet();

			private BitfieldSetBuilder(BitfieldCommand ref) {
				this.ref = ref;
			}

			public BitfieldSetBuilder forType(BitfieldType type) {
				this.set.type = type;
				return this;
			}

			/**
			 * Set the zero based bit {@literal offset}.
			 *
			 * @param offset must not be {@literal null}.
			 * @return
			 */
			public BitfieldSetBuilder valueAt(Long offset) {
				return valueAt(Offset.offset(offset));
			}

			/**
			 * Set the bit offset.
			 *
			 * @param offset must not be {@literal null}.
			 * @return
			 */
			public BitfieldSetBuilder valueAt(Offset offset) {

				Assert.notNull(offset, "Offset must not be null!");

				this.set.offset = offset;
				return this;
			}

			/**
			 * Set the value.
			 *
			 * @param value must not be {@literal null}.
			 * @return
			 */
			public BitfieldCommand to(Long value) {

				Assert.notNull(value, "Value must not be null!");

				this.set.value = value;
				return ref.set(this.set);
			}
		}

		/**
		 * @author Christoph Strobl
		 */
		public class BitfieldGetBuilder {

			private BitfieldCommand ref;

			BitfieldGet get = new BitfieldGet();

			private BitfieldGetBuilder(BitfieldCommand ref) {
				this.ref = ref;
			}

			public BitfieldGetBuilder forType(BitfieldType type) {
				this.get.type = type;
				return this;
			}

			/**
			 * Set the zero based bit {@literal offset}.
			 *
			 * @param offset must not be {@literal null}.
			 * @return
			 */
			public BitfieldCommand valueAt(Long offset) {
				return valueAt(Offset.offset(offset));
			}

			/**
			 * Set the bit offset.
			 *
			 * @param offset must not be {@literal null}.
			 * @return
			 */
			public BitfieldCommand valueAt(Offset offset) {

				Assert.notNull(offset, "Offset must not be null!");

				this.get.offset = offset;
				return ref.get(this.get);
			}
		}

		/**
		 * @author Christoph Strobl
		 */
		public class BitfieldIncrByBuilder {

			private BitfieldCommand ref;

			BitfieldIncrBy incrBy = new BitfieldIncrBy();

			private BitfieldIncrByBuilder(BitfieldCommand ref) {
				this.ref = ref;
			}

			public BitfieldIncrByBuilder forType(BitfieldType type) {
				this.incrBy.type = type;
				return this;
			}

			/**
			 * Set the zero based bit {@literal offset}.
			 *
			 * @param offset must not be {@literal null}.
			 * @return
			 */
			public BitfieldIncrByBuilder valueAt(Long offset) {
				return valueAt(Offset.offset(offset));
			}

			/**
			 * Set the bit offset.
			 *
			 * @param offset must not be {@literal null}.
			 * @return
			 */
			public BitfieldIncrByBuilder valueAt(Offset offset) {

				Assert.notNull(offset, "Offset must not be null!");
				this.incrBy.offset = offset;
				return this;
			}

			/**
			 * Set the {@link BitfieldIncrBy.Overflow} to be used for this and any subsequent {@link BitfieldIncrBy} commands.
			 *
			 * @param overflow
			 * @return
			 */
			public BitfieldIncrByBuilder overflow(BitfieldIncrBy.Overflow overflow) {
				this.incrBy.overflow = overflow;
				return this;
			}

			/**
			 * Set the value used for increasing.
			 *
			 * @param value
			 * @return
			 */
			public BitfieldCommand by(Long value) {

				Assert.notNull(value, "Value must not be null!");

				this.incrBy.value = value;
				return ref.incr(this.incrBy);
			}
		}
	}

	/**
	 * Sub command to be used as part of {@link BitfieldCommand}.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	interface BitfieldSubCommand {

		/**
		 * The actual sub command
		 *
		 * @return never {@literal null}.
		 */
		String getCommand();

		/**
		 * The {@link BitfieldType} to apply for the command.
		 *
		 * @return never {@literal null}.
		 */
		BitfieldType getType();

		/**
		 * The bit offset to apply for the command.
		 *
		 * @return never {@literal null}.
		 */
		Offset getOffset();
	}

	/**
	 * Offset used inside a {@link BitfieldSubCommand}. Can be zero or type based. See
	 * <a href="http://redis.io/commands/bitfield#bits-and-positional-offsets">Bits and positional offsets</a> in the
	 * Redis reference.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	class Offset {

		private final Long offset;
		private final Boolean zeroBased;

		private Offset(Long offset, Boolean zeroBased) {

			Assert.notNull(offset, "Offset must not be null!");
			Assert.notNull(zeroBased, "ZeroBased must not be null!");

			this.offset = offset;
			this.zeroBased = zeroBased;
		}

		/**
		 * Creates new zero based offset. <br />
		 * <b>NOTE:</b> change to type based offset by calling {@link #multipliedByTypeLength()}.
		 *
		 * @param offset must not be {@literal null}.
		 * @return
		 */
		public static Offset offset(Long offset) {
			return new Offset(offset, Boolean.TRUE);
		}

		/**
		 * Creates new type based offset.
		 *
		 * @return
		 */
		public Offset multipliedByTypeLength() {
			return new Offset(offset, Boolean.FALSE);
		}

		/**
		 * @return true if offset starts at 0 and is not multiplied by the type length.
		 */
		public Boolean isZeroBased() {
			return zeroBased;
		}

		/**
		 * @return the actual offset value
		 */
		public Long getValue() {
			return offset;
		}

		/**
		 * @return the Redis Command representation
		 */
		public String asString() {
			return (isZeroBased() ? "" : "#") + getValue();
		}
	}

	/**
	 * The actual Redis bitfield type representation for signed and unsigned integers used with
	 * {@link BitfieldSubCommand}.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	class BitfieldType {

		/** 8 bit signed Integer */
		public static final BitfieldType INT_8 = new BitfieldType(Boolean.TRUE, 8);

		/** 16 bit signed Integer */
		public static final BitfieldType INT_16 = new BitfieldType(Boolean.TRUE, 16);

		/** 32 bit signed Integer */
		public static final BitfieldType INT_32 = new BitfieldType(Boolean.TRUE, 32);

		/** 64 bit signed Integer */
		public static final BitfieldType INT_64 = new BitfieldType(Boolean.TRUE, 64);

		/** 8 bit unsigned Integer */
		public static final BitfieldType UINT_8 = new BitfieldType(Boolean.FALSE, 8);

		/** 16 bit unsigned Integer */
		public static final BitfieldType UINT_16 = new BitfieldType(Boolean.FALSE, 16);

		/** 32 bit unsigned Integer */
		public static final BitfieldType UINT_32 = new BitfieldType(Boolean.FALSE, 32);

		/** 64 bit unsigned Integer */
		public static final BitfieldType UINT_64 = new BitfieldType(Boolean.FALSE, 64);

		private final Boolean signed;
		private final Integer bits;

		private BitfieldType(Boolean signed, Integer bits) {

			Assert.notNull(signed, "Signed must not be null!");
			Assert.notNull(bits, "Bits must not be null!");

			this.signed = signed;
			this.bits = bits;
		}

		/**
		 * Create new signed {@link BitfieldType}.
		 *
		 * @param bits must not be {@literal null}.
		 * @return
		 */
		public static BitfieldType signed(Integer bits) {
			return new BitfieldType(Boolean.TRUE, bits);
		}

		/**
		 * Create new unsigned {@link BitfieldType}.
		 *
		 * @param bits must not be {@literal null}.
		 * @return
		 */
		public static BitfieldType unsigned(Integer bits) {
			return new BitfieldType(Boolean.FALSE, bits);
		}

		/**
		 * @return true if {@link BitfieldType} is signed.
		 */
		public boolean isSigned() {
			return ObjectUtils.nullSafeEquals(signed, Boolean.TRUE);
		}

		/**
		 * Get the actual bits of the type.
		 *
		 * @return never {@literal null}.
		 */
		public Integer getBits() {
			return bits;
		}

		/**
		 * Get the Redis Command representation.
		 *
		 * @return
		 */
		public String asString() {
			return (isSigned() ? "i" : "u") + getBits();
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	abstract class AbstractBitfieldSubCommand implements BitfieldSubCommand {

		BitfieldType type;
		Offset offset;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitfieldSubCommand#getType()
		 */
		@Override
		public BitfieldType getType() {
			return type;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitfieldSubCommand#getOffset()
		 */
		@Override
		public Offset getOffset() {
			return offset;
		}
	}

	/**
	 * The {@code SET} sub command used with {@link BitfieldCommand}.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	class BitfieldSet extends AbstractBitfieldSubCommand {

		private Long value;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitfieldSubCommand#getCommand()
		 */
		@Override
		public String getCommand() {
			return "SET";
		}

		/**
		 * Get the value to set.
		 *
		 * @return never {@literal null}.
		 */
		public Long getValue() {
			return value;
		}

	}

	/**
	 * The {@code GET} sub command used with {@link BitfieldCommand}.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	class BitfieldGet extends AbstractBitfieldSubCommand {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitfieldSubCommand#getCommand()
		 */
		@Override
		public String getCommand() {
			return "GET";
		}

	}

	/**
	 * The {@code INCRBY} sub command used with {@link BitfieldCommand}.
	 *
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	class BitfieldIncrBy extends AbstractBitfieldSubCommand {

		private Long value;
		private Overflow overflow;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitfieldSubCommand#getCommand()
		 */
		@Override
		public String getCommand() {
			return "INCRBY";
		}

		/**
		 * Get the increment value.
		 *
		 * @return never {@literal null}.
		 */
		public Long getValue() {
			return value;
		}

		/**
		 * Get the overflow to apply. Can be {@literal null} to use redis defaults.
		 *
		 * @return can be {@literal null}.
		 */
		public Overflow getOverflow() {
			return overflow;
		}

		/**
		 * @author Christoph Strobl
		 */
		public enum Overflow {
			SAT, FAIL, WRAP
		}
	}
}
