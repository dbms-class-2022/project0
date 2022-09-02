import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class FullScanIteratorImplTest {
    private fun createRootRecords(cache: PageCache) {
        cache.getAndPin(0).use {buf ->
            buf.diskPage.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1000)).asBytes())
        }

    }

    @Test
    fun `test table is missing`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        val rootRecords = RootRecords(cache, 0, 1)
        FullScanIteratorImpl(cache, NAME_SYSTABLE_OID, rootRecords.iterator()) {
            error("Not expected to be here")
        }.forEach{}
    }

    @Test
    fun `test table is empty`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        createRootRecords(cache)
        val rootRecords = RootRecords(cache, 0, 1)
        FullScanIteratorImpl(cache, NAME_SYSTABLE_OID, rootRecords.iterator()) {
            error("Not expected to be here")
        }.forEach{}
    }

    @Test
    fun `test single page table`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        createRootRecords(cache)
        val rootRecords = RootRecords(cache, 0, 1)
        cache.getAndPin(1000).diskPage.let {
            it.putRecord(OidNameRecord(intField(2), stringField("table2")).asBytes())
            it.putRecord(OidNameRecord(intField(3), stringField("table3")).asBytes())
        }
        assertEquals(
            listOf("table2", "table3"),
            FullScanAccessImpl(cache, NAME_SYSTABLE_OID, rootRecords.iterator()) {
                OidNameRecord(intField(), stringField()).fromBytes(it)
            }.map { it.value2 }.toList()
        )
    }

    @Test
    fun `test multiple page table`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        cache.getAndPin(0).use {buf ->
            buf.diskPage.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1000)).asBytes())
            buf.diskPage.putRecord(OidPageidRecord(intField(NAME_SYSTABLE_OID), intField(1001)).asBytes())
        }
        val rootRecords = RootRecords(cache, 0, 1)
        cache.getAndPin(1000).diskPage.let {
            it.putRecord(OidNameRecord(intField(2), stringField("table2")).asBytes())
            it.putRecord(OidNameRecord(intField(3), stringField("table3")).asBytes())
        }
        cache.getAndPin(1001).diskPage.let {
            it.putRecord(OidNameRecord(intField(4), stringField("table4")).asBytes())
            it.putRecord(OidNameRecord(intField(5), stringField("table5")).asBytes())
        }
        assertEquals(
            listOf("table2", "table3", "table4", "table5"),
            FullScanAccessImpl(cache, NAME_SYSTABLE_OID, rootRecords.iterator()) {
                OidNameRecord(intField(), stringField()).fromBytes(it)
            }.map { it.value2 }.toList()
        )

    }

}