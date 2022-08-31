import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class RecordsTest {
    @Test
    fun `fixed size record write-read test`() {
        val rec = Record2(intField(42), intField(146))
        val bytes = rec.asBytes()
        assertEquals(8, bytes.size)

        Record2(intField(), intField()).fromBytes(bytes).let {
            assertEquals(42, it.value1)
            assertEquals(146, it.value2)
        }
    }

    @Test
    fun `variable size record write-read test`() {
        val rec = Record2(stringField("lorem ipsum"), stringField("dolor sit amet"))
        val bytes = rec.asBytes()
        assertEquals("lorem ipsum".length*2 + "dolor sit amet".length*2 + Int.SIZE_BYTES*2, bytes.size)

        Record2(stringField(), stringField()).fromBytes(bytes).let {
            assertEquals("lorem ipsum", it.value1)
            assertEquals("dolor sit amet", it.value2)
        }
    }

    @Test
    fun `mixed size field record`() {
        val rec = Record3(stringField("Привет Мир!"), intField(42), stringField("Hello World!"))
        val bytes = rec.asBytes()
        Record3(stringField(), intField(), stringField()).fromBytes(bytes).let {
            assertEquals("Привет Мир!", it.value1)
            assertEquals(42, it.value2)
            assertEquals("Hello World!", it.value3)
        }
    }

}