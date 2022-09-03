import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

class SystemCatalogTest {
    @Test
    fun `test table is missing`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        val catalog = DumbSystemCatalogImpl(cache, storage)
        assertThrows<SystemCatalogException> {
            catalog.createFullScan("qwerty") { error("Not expected to be here") }
        }
    }

    @Test
    fun `create table`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        val catalog = DumbSystemCatalogImpl(cache, storage)
        catalog.createTable("table1")
        val fullScan = catalog.createFullScan("table1") { error("Not expected to be here ")}
        assertEquals(listOf(), fullScan.toList())
    }

    @Test
    fun `add table page`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, 20)
        val catalog = DumbSystemCatalogImpl(cache, storage)
        val tableOid = catalog.createTable("table1")
        cache.getAndPin(catalog.addPage(tableOid)).use { dataPage ->
            dataPage.diskPage.putRecord(Record2(intField(42), stringField("Hello world")).asBytes())
        }

        val fullScan = catalog.createFullScan("table1") {
            Record2(intField(), stringField()).fromBytes(it)
        }
        assertEquals(listOf(Record2(intField(42), stringField("Hello world"))), fullScan.toList())

    }


}