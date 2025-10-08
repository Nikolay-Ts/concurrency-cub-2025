package day1

import java.util.concurrent.*
import java.util.concurrent.atomic.*
import kotlin.math.PI

open class TreiberStackWithElimination<E> : Stack<E> {
    private val stack = TreiberStack<E>()

    // TODO: Try to optimize concurrent push and pop operations,
    // TODO: synchronizing them in an `eliminationArray` cell.
    private val eliminationArray = AtomicReferenceArray<Any?>(ELIMINATION_ARRAY_SIZE)

    override fun push(element: E) {
        if (tryPushElimination(element)) return
        stack.push(element)
    }

    protected open fun tryPushElimination(element: E): Boolean {
        val cellId = randomCellIndex()

        if (!eliminationArray.compareAndSet(cellId, CELL_STATE_EMPTY, element)) {
            return false
        }

        repeat(ELIMINATION_WAIT_CYCLES) {
            if (eliminationArray.get(cellId) === CELL_STATE_RETRIEVED) {
                eliminationArray.set(cellId, CELL_STATE_EMPTY)
                return true
            }
        }

        if (eliminationArray.compareAndSet(cellId, element, CELL_STATE_EMPTY)) {
            return false
        }

        if (eliminationArray.get(cellId) === CELL_STATE_RETRIEVED) {
            eliminationArray.set(cellId, CELL_STATE_EMPTY)
            return true
        }

        eliminationArray.set(cellId, CELL_STATE_EMPTY)
        return false

    }

    override fun pop(): E? = tryPopElimination() ?: stack.pop()

    private fun tryPopElimination(): E? {
        val cellId = randomCellIndex()
        val cur = eliminationArray.get(cellId) ?: return null
        if (cur === CELL_STATE_RETRIEVED) return null

        if (eliminationArray.compareAndSet(cellId, cur, CELL_STATE_RETRIEVED)) {
            val value = cur as E
            return value
        }
        return null

    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(eliminationArray.length())

    companion object {
        private const val ELIMINATION_ARRAY_SIZE = 2 // Do not change!
        private const val ELIMINATION_WAIT_CYCLES = 1 // Do not change!

        // Initially, all cells are in EMPTY state.
        private val CELL_STATE_EMPTY = null

        // `tryPopElimination()` moves the cell state
        // to `RETRIEVED` if the cell contains element.
        private val CELL_STATE_RETRIEVED = Any()
    }
}