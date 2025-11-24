@file:Suppress("UNCHECKED_CAST")

package day6

import java.util.concurrent.atomic.*
import kotlin.math.absoluteValue

class ConcurrentHashTable<K : Any, V : Any>(initialCapacity: Int) {
    private val table = AtomicReference(Table<K, V>(initialCapacity))

    fun put(key: K, value: V): V? {
        while (true) {
            val putResult = table.get().put(key, value)
            if (putResult === NEEDS_REHASH) {
                resize()
            } else {
                return putResult as V?
            }
        }
    }

    fun get(key: K): V? {
        return table.get().get(key)
    }

    fun remove(key: K): V? {
        return table.get().remove(key)
    }

    private fun resize() {
        val currentTable = table.get()
        val newCapacity = currentTable.capacity * 2
        val newTable = Table<K, V>(newCapacity)

        for (i in 0 until currentTable.capacity) {
            val key = currentTable.keys.get(i)
            val value = currentTable.values.get(i)

            if (key != null && key !== EMPTY && key !== TOMBSTONE) {
                @Suppress("UNCHECKED_CAST")
                val actualKey = key as K
                if (value != null && value !== TOMBSTONE) {
                    @Suppress("UNCHECKED_CAST")
                    val actualValue = value
                    newTable.putForResize(actualKey, actualValue)
                }
            }
        }
        table.compareAndSet(currentTable, newTable)
    }

    class Table<K : Any, V : Any>(val capacity: Int) {
        val keys = AtomicReferenceArray<Any?>(capacity)
        val values = AtomicReferenceArray<V?>(capacity)

        private val size = AtomicInteger(0)
        private val threshold = (capacity * 0.75).toInt()

        init {
            for (i in 0 until capacity) {
                keys.set(i, EMPTY)
                values.set(i, null)
            }
        }

        fun put(key: K, value: V): Any? {
            val hash = key.hashCode().absoluteValue
            var index = hash % capacity
            var firstTombstone = -1

            for (attempt in 0 until capacity) {
                val currentKey = keys.get(index)

                when (currentKey) {
                    EMPTY -> {
                        if (firstTombstone != -1) firstTombstone else index
                        if (firstTombstone == -1) {
                            if (keys.compareAndSet(index, EMPTY, key)) {
                                values.set(index, value)
                                if (size.incrementAndGet() > threshold) {
                                    return NEEDS_REHASH
                                }
                                return null
                            }
                        } else {
                            val tombstoneKey = keys.get(firstTombstone)
                            if (tombstoneKey !== TOMBSTONE) {
                                return put(key, value)
                            }

                            if (keys.compareAndSet(firstTombstone, TOMBSTONE, key)) {
                                values.set(firstTombstone, value)
                                size.incrementAndGet()
                                return null
                            }
                            return put(key, value)
                        }
                    }
                    TOMBSTONE -> {
                        if (firstTombstone == -1) {
                            firstTombstone = index
                        }
                    }
                    key -> {
                        val oldValue = values.getAndSet(index, value)
                        return oldValue
                    }
                    else -> { }
                }

                index = (index + 1) % capacity
            }

            return NEEDS_REHASH
        }

        fun get(key: K): V? {
            val hash = key.hashCode().absoluteValue
            var index = hash % capacity

            for (attempt in 0 until capacity) {
                val currentKey = keys.get(index)

                when (currentKey) {
                    EMPTY -> return null
                    key -> {
                        val value = values.get(index)
                        return if (value === TOMBSTONE) null else value as V?
                    }
                    TOMBSTONE -> { }
                }

                index = (index + 1) % capacity
            }

            return null
        }

        fun remove(key: K): V? {
            val hash = key.hashCode().absoluteValue
            var index = hash % capacity

            for (attempt in 0 until capacity) {
                val currentKey = keys.get(index)

                when (currentKey) {
                    EMPTY -> return null
                    key -> {
                        val oldValue = values.get(index)
                        if (oldValue !== TOMBSTONE) {
                            // Mark value as tombstone
                            if (values.compareAndSet(index, oldValue, TOMBSTONE as V?)) {
                                keys.compareAndSet(index, key, TOMBSTONE)
                                size.decrementAndGet()
                                return oldValue
                            }
                            return remove(key)
                        }
                        return null
                    }
                    TOMBSTONE -> { }
                }

                index = (index + 1) % capacity
            }

            return null
        }

        internal fun putForResize(key: K, value: V) {
            val hash = key.hashCode().absoluteValue
            var index = hash % capacity

            for (attempt in 0 until capacity) {
                val currentKey = keys.get(index)

                if (currentKey === EMPTY || currentKey === TOMBSTONE) {
                    if (keys.compareAndSet(index, currentKey, key)) {
                        values.set(index, value)
                        return
                    }
                } else if (currentKey == key) {
                    values.set(index, value)
                    return
                }

                index = (index + 1) % capacity
            }
        }
    }
}

private val NEEDS_REHASH = Any()
private val EMPTY = Any()
private val TOMBSTONE = Any()