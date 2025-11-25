package day6

import TestBase
import org.jetbrains.lincheck.datastructures.*

class ConcurrentHashTableTest : TestBase(
    sequentialSpecification = SequentialHashTableIntInt::class,
    scenarios = 300
) {
    private val hashTable = ConcurrentHashTable<Int, Int>(initialCapacity = 2)

    @Operation
    fun put(key: Int, value: Int): Int? = hashTable.put(key, value)

    @Operation
    fun get(key: Int): Int? = hashTable.get(key)

    @Operation
    fun remove(key: Int): Int? = hashTable.remove(key)
}

class SequentialHashTableIntInt {
    private val map = HashMap<Int, Int>()

    fun put(key: Int, value: Int): Int? = map.put(key, value)

    fun get(key: Int): Int? = map.get(key)

    fun remove(key: Int): Int? = map.remove(key)
}
