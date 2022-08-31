import java.nio.ByteBuffer
import java.time.Instant
import java.util.*

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

class LongAttribute: AttributeType<Long>(Long.SIZE_BYTES) {
    override fun asBytes(value: Long): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.putLong(value)
    }.array()

    override fun fromBytes(bytes: ByteArray) = ByteBuffer.wrap(bytes).long to Long.SIZE_BYTES
}
fun longField(value: Long = 0L) = LongAttribute() to value

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

class BooleanAttribute: AttributeType<Boolean>(Byte.SIZE_BYTES) {
    override fun asBytes(value: Boolean): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.put(if (value) 1 else 0)
    }.array()

    override fun fromBytes(bytes: ByteArray) = (ByteBuffer.wrap(bytes).get().toInt() == 1) to Byte.SIZE_BYTES
}
fun booleanField(value: Boolean = false) = BooleanAttribute() to value

class DoubleAttribute: AttributeType<Double>(Double.SIZE_BYTES) {
    override fun asBytes(value: Double): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.putDouble(value)
    }.array()

    override fun fromBytes(bytes: ByteArray) = ByteBuffer.wrap(bytes).double to Double.SIZE_BYTES
}
fun doubleField(value: Double = 0.0) = DoubleAttribute() to value

class DateAttribute: AttributeType<Date>(Long.SIZE_BYTES) {
    override fun asBytes(value: Date): ByteArray = ByteBuffer.allocate(byteSize).also {
        it.putLong(value.time)
    }.array()

    override fun fromBytes(bytes: ByteArray) = ByteBuffer.wrap(bytes).long.let {
        Date.from(Instant.ofEpochMilli(it))
    } to Long.SIZE_BYTES
}
fun dateField(value: Date = Date.from(Instant.EPOCH)) = DateAttribute() to value

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
