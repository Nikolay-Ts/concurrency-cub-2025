package day7

import java.util.Comparator
import java.util.concurrent.Phaser
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

private val NODE_DISTANCE_COMPARATOR =
    Comparator<Node> { o1, o2 -> o1.distance.compareTo(o2.distance) }

fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    start.distance = 0

    val q = PriorityBlockingQueue<Node>(11, NODE_DISTANCE_COMPARATOR)
    q.add(start)

    val active = AtomicInteger(1)
    val onFinish = Phaser(workers + 1)

    repeat(workers) {
        thread {
            while (true) {
                val cur = q.poll()

                if (cur == null) {
                    if (active.get() == 0) {
                        break
                    }
                    Thread.yield()
                    continue
                }
                for (e in cur.outgoingEdges) {
                    while (true) {
                        val oldDistance = e.to.distance
                        val newDistance = cur.distance + e.weight

                        if (oldDistance <= newDistance) {
                            break
                        }
                        if (e.to.casDistance(oldDistance, newDistance)) {
                            q.add(e.to)
                            active.incrementAndGet()
                            break
                        }
                    }
                }
                active.decrementAndGet()
            }
            onFinish.arrive()
        }
    }

    onFinish.arriveAndAwaitAdvance()
}