package dev.evo.kafka

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
