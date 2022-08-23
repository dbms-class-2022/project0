import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable
import java.nio.ByteBuffer
import kotlin.random.Random
import kotlin.test.*

data class TestRecord(val key: Int, val pointer: PageId, val payload: String? = null) {
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


class DiskPageImplTest {
    @Test
    fun `check empty page properties`() {
        createDiskPage().let {
            assertTrue(it.getRecord(0).isOutOfRange)
        }
    }

    @Test
    fun `add and get record`() {
        createDiskPage().let {
            val record = TestRecord(42, 1)
            assertTrue(it.putRecord(record.toByteArray()).isOk)

            assertEquals(record, TestRecord.fromByteArray(it.getRecord(0).bytes))

        }
    }

    @Test
    fun `add and update record`() {
        createDiskPage().let {
            assertTrue(it.putRecord(TestRecord(42, 1).toByteArray()).isOk)
            val newRecord = TestRecord(43, 2)
            assertTrue(it.putRecord(newRecord.toByteArray(), 0).isOk)

            assertEquals(newRecord, TestRecord.fromByteArray(it.getRecord(0).bytes))
        }
    }

    @Test
    fun `add many records`() {
        createDiskPage().let { page ->
            for (i in   0..100) {
                val rec = TestRecord(i, i)
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
            }
            for (i in 0..100) {
                val rec = TestRecord(i, i)
                assertEquals(rec, TestRecord.fromByteArray(page.getRecord(i).bytes))
            }
        }
    }

    @Test
    fun `putRecord failures`() {
        createDiskPage().let { page ->
            val bytes = ByteArray(1300)
            assertTrue(page.putRecord(bytes, 0).isOk)
            assertTrue(page.putRecord(bytes, 1).isOk)
            assertTrue(page.putRecord(bytes, 2).isOk)
            assertFalse(page.putRecord(bytes, 3).isOk)
            assertTrue(page.putRecord(bytes, 3).isOutOfSpace)

            assertFalse(page.putRecord(bytes, 5).isOk)
            assertTrue(page.putRecord(bytes, 5).isOutOfRange)
        }
    }

    @Test
    fun `grow records`() {
        createDiskPage().let {page ->
            for (i in 0..10) {
                val rec = TestRecord(i, i)
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
            }
            for (i in 0..10) {
                val rec = VarLengthRecord().also {
                    it.key = i
                    it.value = Random.nextLong(100000).toString()
                }
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
                assertEquals(rec, VarLengthRecord.fromByteArray(page.getRecord(i).bytes))
                for (j in i+1..10) {
                    assertEquals(TestRecord(j ,j), TestRecord.fromByteArray(page.getRecord(j).bytes))
                }
            }
        }
    }

    @Test
    fun `shrink records`() {
        createDiskPage().let {page ->
            for (i in 0..10) {
                val rec = VarLengthRecord().also {
                    it.key = i
                    it.value = Random.nextLong(100000).toString()
                }
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
            }
            for (i in 0..10) {
                val rec = TestRecord(i, i)
                assertTrue(page.putRecord(rec.toByteArray(), i).isOk)
                assertEquals(rec, TestRecord.fromByteArray(page.getRecord(i).bytes))
                for (j in i+1..10) {
                    assertEquals(j, VarLengthRecord.fromByteArray(page.getRecord(j).bytes).key)
                    assertTrue(VarLengthRecord.fromByteArray(page.getRecord(j).bytes).value.isNotBlank())
                }

            }
        }
    }

    @Test
    fun `preserve deleted records when resizing`() {
        createDiskPage().let {page ->
            for (i in 0..3) {
                assertTrue(page.putRecord(TestRecord(i, i).toByteArray(), i).isOk)
            }
            page.deleteRecord(2)
            assertTrue(page.getRecord(2).isDeleted)
            assertFalse(page.getRecord(2).isOk)
            assertTrue(page.putRecord(VarLengthRecord().also {
                it.key = 0
                it.value = "Foo bar"
            }.toByteArray(), 0).isOk)
            assertTrue(page.getRecord(2).isDeleted)
        }
    }

    @Test
    fun `all records`() {
        createDiskPage().let { page ->
            for (i in 0..3) {
                assertTrue(page.putRecord(TestRecord(i, i).toByteArray(), i).isOk)
            }
            page.deleteRecord(2)
            page.allRecords().let {
                assertEquals(TestRecord(0, 0), TestRecord.fromByteArray(it[0]!!.bytes))
                assertEquals(TestRecord(1, 1), TestRecord.fromByteArray(it[1]!!.bytes))
                assertEquals(TestRecord(3, 3), TestRecord.fromByteArray(it[3]!!.bytes))
                assertNotNull(it[2])
                assertTrue(it[2]!!.isDeleted)

            }
        }
    }
}