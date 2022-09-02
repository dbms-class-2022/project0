import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class RootRecordsTest {
    @Test
    fun `iterate in bounds of one page`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        cache.getAndPin(0).use {buf ->
            buf.diskPage.putRecord(OidPageidRecord(intField(1), intField(1000)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(1), intField(1001)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(2), intField(2000)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(1), intField(1002)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(2), intField(2001)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(3), intField(3000)).asBytes())
        }


        assertEquals(listOf(1000, 1001, 2000, 1002, 2001, 3000), RootRecords(cache, 0, 1).map { it.value2 }.toList())
    }

    @Test
    fun `iterate over several pages`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        cache.getAndPin(0).use {buf ->
            buf.diskPage.putRecord(OidPageidRecord(intField(1), intField(1000)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(1), intField(1001)).asBytes())
        }
        cache.getAndPin(1).use {buf ->
            buf.diskPage.putRecord(OidPageidRecord(intField(2), intField(2000)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(1), intField(1002)).asBytes())
        }
        cache.getAndPin(2).use { buf ->
            buf.diskPage.putRecord(OidPageidRecord(intField(2), intField(2001)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(3), intField(3000)).asBytes())
        }

        assertEquals(listOf(1000, 1001, 2000, 1002, 2001, 3000), RootRecords(cache, 0, 3).map { it.value2 }.toList())

    }

}
