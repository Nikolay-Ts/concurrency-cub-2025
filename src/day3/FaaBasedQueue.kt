package day3

import day2.*
import java.util.concurrent.atomic.*

class FAABasedQueue<E> : Queue<E> {
    private val first = Segment(0)
    private val head = AtomicReference(first)
    private val tail = AtomicReference(first)
    private val enqIndex = AtomicLong(0)
    private val deqIndex = AtomicLong(0)

    private val DONE = Any()

    override fun enqueue(element: E) {
        while (true) {
            val i = enqIndex.getAndIncrement()
            val segId = i / SEGMENT_SIZE
            val cellIdx = (i % SEGMENT_SIZE).toInt()

            val seg = locateFrom(tail, segId)
            moveTailForward(seg)

            if (seg.cells.compareAndSet(cellIdx, null, element)) return
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (!shouldTryToDequeue()) return null

            val i = deqIndex.getAndIncrement()
            val segId = i / SEGMENT_SIZE
            val cellIdx = (i % SEGMENT_SIZE).toInt()

            val seg = locateFrom(head, segId)
            moveHeadForward(seg)

            val cells = seg.cells
            while (true) {
                val v = cells.get(cellIdx)
                when {
                    v == null -> {
                        if (cells.compareAndSet(cellIdx, null, DONE)) break
                        continue
                    }
                    v === DONE -> {
                        break
                    }
                    else -> {
                        if (cells.compareAndSet(cellIdx, v, DONE)) return v as E
                        continue
                    }
                }
            }
        }
    }

    private fun locateFrom(ref: AtomicReference<Segment>, targetId: Long): Segment {
        val start = ref.get()
        val safeStart = if (start.id.toLong() <= targetId) start else first
        return findSegment(safeStart, targetId)
    }

    private fun findSegment(start: Segment, id: Long): Segment {
        var cur = start
        while (cur.id.toLong() < id) {
            val next = cur.next.get()
            cur = if (next == null) {
                val newSeg = Segment(cur.id + 1)
                if (cur.next.compareAndSet(null, newSeg)) newSeg else cur.next.get()!!
            } else next
        }
        return cur
    }

    private fun moveTailForward(to: Segment) {
        while (true) {
            val t = tail.get()
            if (to.id <= t.id) return
            val n = t.next.get() ?: run {
                t.next.compareAndSet(null, Segment(t.id + 1))
                t.next.get()
            } ?: return
            if (!tail.compareAndSet(t, n)) return
        }
    }

    private fun moveHeadForward(to: Segment) {
        while (true) {
            val h = head.get()
            if (to.id <= h.id) return
            val n = h.next.get() ?: return
            if (!head.compareAndSet(h, n)) return
        }
    }

    private fun shouldTryToDequeue(): Boolean {
        while (true) {
            val e1 = enqIndex.get()
            val d  = deqIndex.get()
            val e2 = enqIndex.get()
            if (e1 == e2) return d < e1
        }
    }
}

private class Segment(val id: Int) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2