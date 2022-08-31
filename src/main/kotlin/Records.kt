import java.nio.ByteBuffer

sealed class AttributeType<T>(val byteSize: Int = -1) {
    val isFixedSize: Boolean = byteSize > 0

    abstract fun asBytes(value: T): ByteArray

    abstract fun fromBytes(bytes: ByteArray): Pair<T, Int>
}

class IntAttribute: AttributeType<Int>(Int.SIZE_BYTES) {
    override fun asBytes(value: Int): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.putInt(value)
    }.array()

    override fun fromBytes(bytes: ByteArray) = ByteBuffer.wrap(bytes).int to Int.SIZE_BYTES
}
fun intField(value: Int = 0) = IntAttribute() to value

class StringAttribute: AttributeType<String>() {
    override fun asBytes(value: String): ByteArray =
        value.toCharArray().let {chars ->
            ByteBuffer.allocate(chars.size * Char.SIZE_BYTES + Int.SIZE_BYTES).also {
                var buf = it.putInt(chars.size)
                chars.forEach { c -> buf = buf.putChar(c) }
            }.array()
        }


    override fun fromBytes(bytes: ByteArray): Pair<String, Int> {
        val length = ByteBuffer.wrap(bytes).int * Char.SIZE_BYTES
        return ByteBuffer.wrap(bytes, Int.SIZE_BYTES, length).asCharBuffer().toString() to Int.SIZE_BYTES + length
    }
}
fun stringField(value: String = "") = StringAttribute() to value

class Record1<T1: Any>(f1: Pair<AttributeType<T1>, T1>) {
    val type1 = f1.first
    val value1 = f1.second

    fun asBytes(): ByteArray = type1.asBytes(value1)
    fun fromBytes(bytes: ByteArray) = type1.fromBytes(bytes)
}

class Record2<T1: Any, T2: Any>(
    f1: Pair<AttributeType<T1>, T1>,
    f2: Pair<AttributeType<T2>, T2>) {

    val type1 = f1.first
    val value1 = f1.second
    val type2 = f2.first
    val value2 = f2.second

    fun asBytes(): ByteArray = type1.asBytes(value1) + type2.asBytes(value2)
    fun fromBytes(bytes: ByteArray): Record2<T1, T2> {
        val buffer = ByteBuffer.wrap(bytes)
        val v1: T1 = buffer.readAttribute(type1)
        val v2: T2 = buffer.readAttribute(type2)
        return Record2(type1 to v1, type2 to v2)
    }
}

class Record3<T1: Any, T2: Any, T3: Any>(
    f1: Pair<AttributeType<T1>, T1>,
    f2: Pair<AttributeType<T2>, T2>,
    f3: Pair<AttributeType<T3>, T3>) {

    val type1 = f1.first
    val value1 = f1.second
    val type2 = f2.first
    val value2 = f2.second
    val type3 = f3.first
    val value3 = f3.second

    fun asBytes(): ByteArray = type1.asBytes(value1) + type2.asBytes(value2) + type3.asBytes(value3)
    fun fromBytes(bytes: ByteArray): Record3<T1, T2, T3> {
        val buffer = ByteBuffer.wrap(bytes)
        val v1: T1 = buffer.readAttribute(type1)
        val v2: T2 = buffer.readAttribute(type2)
        val v3: T3 = buffer.readAttribute(type3)
        return Record3(type1 to v1, type2 to v2, type3 to v3)
    }
}

fun <T> ByteBuffer.readAttribute(attrType: AttributeType<T>): T {
    val (value, size) = attrType.fromBytes(this.slice().toBytes())
    this.position(this.position() + size)
    return value
}

private fun ByteBuffer.toBytes() = ByteArray(this.limit()).also {this.get(it)}
