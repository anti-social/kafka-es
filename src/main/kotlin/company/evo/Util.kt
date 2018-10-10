package company.evo

import java.util.concurrent.TimeoutException


inline fun <reified T> castOrFail(obj: Any?, field: String? = null): T {
    return obj as? T ?:
            if (field != null) {
                throw IllegalArgumentException(
                        "[$field] must be ${T::class.java} but was: ${obj?.javaClass}"
                )
            } else {
                throw IllegalArgumentException(
                        "Expected ${T::class.java} but was: ${obj?.javaClass}")
            }
}

class Timeout(private val initialTimeoutMs: Long) {
    private var startedAt = System.nanoTime()

    fun reset() {
        startedAt = System.nanoTime()
    }

    fun timeLeft(): Long {
        val measuredIntervalMs = Math.max(
                (System.nanoTime() - startedAt) / 1_000_000,
                0L
        )
        return initialTimeoutMs - measuredIntervalMs
    }

    fun timeLeftOrFail(): Long {
        val t = timeLeft()
        if (t <= 0) {
            throw TimeoutException()
        }
        return t
    }
}
