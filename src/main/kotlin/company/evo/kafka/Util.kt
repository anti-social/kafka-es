package company.evo.kafka

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
    private val startedAt = System.nanoTime()

    fun drift(): Long {
        return initialTimeoutMs - (System.nanoTime() - startedAt) / 1_000_000
    }

    fun driftOrFail(): Long {
        val t = drift()
        if (t <= 0) {
            throw TimeoutException()
        }
        return t
    }
}
