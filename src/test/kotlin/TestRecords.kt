import java.io.*
import java.nio.ByteBuffer

data class TestRecord(val key: Int, val pointer: PageId) {
    fun toByteArray(): ByteArray  = ByteBuffer.allocate(Int.SIZE_BYTES + PageId.SIZE_BYTES).also {
        it.putInt(key)
        it.putInt(pointer)
    }.array()

    companion object {
        fun fromByteArray(bytes: ByteArray): TestRecord =
            ByteBuffer.wrap(bytes).let {
                TestRecord(it.int, it.int)
            }

    }
}

class VarLengthRecord: Serializable {
    var key: Int = 0
    var value: String = ""


    fun toByteArray(): ByteArray =
        ByteArrayOutputStream().use {
            ObjectOutputStream(it).writeObject(this)
            it.toByteArray()
        }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as VarLengthRecord

        if (key != other.key) return false
        if (value != other.value) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key
        result = 31 * result + value.hashCode()
        return result
    }

    companion object {
        fun fromByteArray(bytes: ByteArray) =
            ByteArrayInputStream(bytes).use {
                ObjectInputStream(it).readObject() as VarLengthRecord
            }
    }
}

