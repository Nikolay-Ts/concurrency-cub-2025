@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day5

import java.util.concurrent.atomic.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any?>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    fun get(index: Int): E? {
        while (true) {
            when (val value = array[index]) {
                is CAS2Descriptor<*> -> {
                    val desc = value as CAS2Descriptor<E>
                    desc.complete()
                }
                is DCSSDescriptor<*> -> {
                    val desc = value as DCSSDescriptor<E>
                    desc.complete()
                }
                else -> return value as E?
            }
        }
    }

    fun cas2(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?, update2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }

        val (idx1, exp1, upd1, idx2, exp2, upd2) = if (index1 < index2) {
            CAS2Params(index1, expected1, update1, index2, expected2, update2)
        } else {
            CAS2Params(index2, expected2, update2, index1, expected1, update1)
        }

        val descriptor = CAS2Descriptor(
            array = array,
            index1 = idx1,
            expected1 = exp1,
            update1 = upd1,
            index2 = idx2,
            expected2 = exp2,
            update2 = upd2
        )

        descriptor.complete()

        return descriptor.status.get() == Status.SUCCESS
    }

    private data class CAS2Params<E>(
        val index1: Int, val expected1: E?, val update1: E?,
        val index2: Int, val expected2: E?, val update2: E?
    )

    private interface Descriptor {
        fun complete(): Boolean
    }

    private enum class Status {
        UNDECIDED, SUCCESS, FAILED
    }

    private class CAS2Descriptor<E : Any>(
        val array: AtomicReferenceArray<Any?>,
        val index1: Int,
        val expected1: E?,
        val update1: E?,
        val index2: Int,
        val expected2: E?,
        val update2: E?
    ) : Descriptor {
        val status = AtomicReference(Status.UNDECIDED)

        override fun complete(): Boolean {
            if (status.get() != Status.UNDECIDED) {
                applyPhysicalUpdate(index1)
                applyPhysicalUpdate(index2)
                return status.get() == Status.SUCCESS
            }

            if (!installDescriptor(index1, expected1)) {
                status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                array.compareAndSet(index1, this, expected1)
                return false
            }

            val installed = dcssInstall(index2, expected2)

            if (!installed) {
                status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                applyPhysicalUpdate(index1)
                applyPhysicalUpdate(index2)
                return false
            }

            status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)

            applyPhysicalUpdate(index1)
            applyPhysicalUpdate(index2)

            return status.get() == Status.SUCCESS
        }

        private fun installDescriptor(index: Int, expected: E?): Boolean {
            while (true) {
                when (val current = array[index]) {
                    expected -> {
                        if (array.compareAndSet(index, expected, this)) {
                            return true
                        }
                    }
                    this -> return true
                    is CAS2Descriptor<*> -> {
                        val desc = current as CAS2Descriptor<E>
                        desc.complete()
                    }
                    is DCSSDescriptor<*> -> {
                        val desc = current as DCSSDescriptor<E>
                        desc.complete()
                    }
                    else -> return false
                }
            }
        }

        private fun dcssInstall(index: Int, expected: E?): Boolean {
            while (true) {
                when (val current = array[index]) {
                    expected -> {
                        if (status.get() != Status.UNDECIDED) {
                            return status.get() == Status.SUCCESS
                        }

                        val dcssDesc = DCSSDescriptor(
                            array = array,
                            index = index,
                            expected = expected,
                            update = this,
                            statusRef = status,
                            expectedStatus = Status.UNDECIDED
                        )

                        return dcssDesc.complete()
                    }
                    this -> return true
                    is CAS2Descriptor<*> -> {
                        val desc = current as CAS2Descriptor<E>
                        desc.complete()
                    }
                    is DCSSDescriptor<*> -> {
                        val desc = current as DCSSDescriptor<E>
                        desc.complete()
                    }
                    else -> return false
                }
            }
        }

        private fun applyPhysicalUpdate(index: Int) {
            while (true) {
                val current = array[index]

                if (current !is CAS2Descriptor<*> || current !== this) {
                    return
                }

                val newValue = when (status.get()) {
                    Status.SUCCESS -> if (index == index1) update1 else update2
                    else -> if (index == index1) expected1 else expected2
                }

                if (array.compareAndSet(index, this, newValue)) {
                    return
                }
            }
        }
    }

    private class DCSSDescriptor<E : Any>(
        val array: AtomicReferenceArray<Any?>,
        val index: Int,
        val expected: E?,
        val update: Any?,
        val statusRef: AtomicReference<Status>,
        val expectedStatus: Status
    ) : Descriptor {
        val status = AtomicReference(Status.UNDECIDED)

        override fun complete(): Boolean {
            val currentStatus = status.get()
            if (currentStatus != Status.UNDECIDED) {
                applyPhysicalUpdate()
                return currentStatus == Status.SUCCESS
            }

            if (!installSelf()) {
                status.set(Status.FAILED)
                return false
            }

            val actualStatus = statusRef.get()
            val success = actualStatus == expectedStatus
            status.compareAndSet(Status.UNDECIDED,
                if (success) Status.SUCCESS else Status.FAILED)

            applyPhysicalUpdate()

            return status.get() == Status.SUCCESS
        }

        private fun installSelf(): Boolean {
            while (true) {
                when (val current = array[index]) {
                    expected -> {
                        if (array.compareAndSet(index, expected, this)) {
                            return true
                        }
                    }
                    this -> return true
                    is CAS2Descriptor<*> -> {
                        val desc = current as CAS2Descriptor<E>
                        desc.complete()
                    }
                    is DCSSDescriptor<*> -> {
                        val desc = current as DCSSDescriptor<E>
                        desc.complete()
                    }
                    else -> return false
                }
            }
        }

        private fun applyPhysicalUpdate() {
            while (true) {
                val current = array[index]

                if (current !is DCSSDescriptor<*> || current !== this) {
                    return
                }

                val newValue = when (status.get()) {
                    Status.SUCCESS -> update
                    else -> expected
                }

                if (array.compareAndSet(index, this, newValue)) {
                    return
                }
            }
        }
    }
}