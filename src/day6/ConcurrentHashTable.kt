@file:Suppress("UNCHECKED_CAST")

package day6

import java.util.concurrent.atomic.*

class ConcurrentHashTable<K : Any, V : Any>(initialCapacity: Int) {
    private val table = AtomicReference(Table<K, V>(initialCapacity))

    fun put(key: K, value: V): V? {
        while (true) {
            val currentTable  = table.get()
            val putResult = currentTable.put(key, value)
            if (putResult === NEEDS_REHASH) {
                resize(currentTable)
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

    private fun resize(curTable: Table<K, V>) {
        val newTable = Table<K, V>(curTable.capacity * 2)
        curTable.next.compareAndSet(null, newTable)
        for (i in 0 until curTable.capacity) {
            curTable.copySlot(i)
        }
        table.compareAndSet(curTable, curTable.next.get())
    }

    class Table<K : Any, V : Any>(val capacity: Int) {
        val keys = AtomicReferenceArray<Any?>(capacity)
        val values = AtomicReferenceArray<Any?>(capacity)
        val next = AtomicReference<Table<K, V>?>(null)

        fun put(key: K, value: V): Any? {
            var idx = index(key)
            val start = idx

            while (true) {
                if (next.get() != null) {
                    copySlot(idx)
                }

                when (val k = keys.get(idx)) {
                    is KeyValue -> {
                        helpPut(idx, k)
                        continue
                    }
                    is MovedKey -> {
                        if (k.key == key) {
                            return next.get()!!.put(key, value)
                        }
                    }
                    null -> {
                        val kv = KeyValue(key, value)
                        if (keys.compareAndSet(idx, null, kv)) {
                            helpPut(idx, kv)
                            return null
                        }
                        continue
                    }
                    key -> {
                        while (true) {
                            val curVal = values.get(idx)
                            if (curVal is MovedValue) {
                                copySlot(idx)
                                break
                            }
                            if (values.compareAndSet(idx, curVal, value)) {
                                return if (curVal === TOMBSTONE) null else curVal as V?
                            }
                        }
                        continue
                    }
                }

                idx = (idx + 1) % capacity
                if (idx == start) return NEEDS_REHASH
            }
        }

        fun get(key: K): V? {
            var idx = index(key)
            val start = idx

            while (true) {
                if (next.get() != null) {
                    copySlot(idx)
                }

                when (val k = keys.get(idx)) {
                    is KeyValue -> {
                        helpPut(idx, k)
                        continue
                    }
                    is MovedKey -> {
                        if (k.key == key) {
                            return next.get()!!.get(key)
                        }
                    }
                    null -> return null
                    key -> {
                        val v = values.get(idx)
                        return when {
                            v === TOMBSTONE -> null
                            v is MovedValue -> v.value as V?
                            else -> v as V?
                        }
                    }
                }

                idx = (idx + 1) % capacity
                if (idx == start) return null
            }
        }

        fun remove(key: K): V? {
            var idx = index(key)
            val start = idx

            while (true) {
                if (next.get() != null) {
                    copySlot(idx)
                }

                when (val k = keys.get(idx)) {
                    is KeyValue -> {
                        helpPut(idx, k)
                        continue
                    }
                    is MovedKey -> {
                        if (k.key == key) {
                            return next.get()!!.remove(key)
                        }
                    }
                    null -> return null
                    key -> {
                        while (true) {
                            when (val curVal = values.get(idx)) {
                                is MovedValue -> {
                                    copySlot(idx)
                                    return next.get()!!.remove(key)
                                }
                                TOMBSTONE -> return null
                                else -> {
                                    if (values.compareAndSet(idx, curVal, TOMBSTONE)) {
                                        return curVal as V?
                                    }
                                }
                            }
                        }
                    }
                }

                idx = (idx + 1) % capacity
                if (idx == start) return null
            }
        }

        fun copySlot(idx: Int) {
            val nextTable = next.get() ?: return

            while (true) {
                when (val k = keys.get(idx)) {
                    null -> return
                    is MovedKey -> return
                    is KeyValue -> {
                        helpPut(idx, k)
                        continue
                    }
                    else -> {
                        when (val v = values.get(idx)) {
                            null, TOMBSTONE -> return
                            is MovedValue -> {
                                val newIdx = nextTable.index(k as K)
                                val kv = KeyValue(k, v.value)
                                nextTable.keys.compareAndSet(newIdx, null, kv)
                                keys.set(idx, MovedKey(k))
                                return
                            }
                            else -> {
                                if (!values.compareAndSet(idx, v, MovedValue(v))) {
                                    continue
                                }
                                val newIdx = nextTable.index(k as K)
                                val kv = KeyValue(k, v)
                                nextTable.keys.compareAndSet(newIdx, null, kv)
                                keys.set(idx, MovedKey(k))
                                return
                            }
                        }
                    }
                }
            }
        }

        private fun helpPut(idx: Int, kv: KeyValue) {
            values.compareAndSet(idx, null, kv.value)
            keys.compareAndSet(idx, kv, kv.key)
        }

        private fun index(key: K): Int {
            return (key.hashCode() and Int.MAX_VALUE) % capacity
        }
    }
}

private val NEEDS_REHASH = Any()

private val TOMBSTONE = Any()

private class KeyValue(val key: Any?, val value: Any?)

private class MovedKey(val key: Any?)

private class MovedValue(val value: Any?)