import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PageCacheImplTest {
    @Test
    fun `basic test - cache loads pages from the storage and writes them back`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage)
        val pageId = storage.createPage().also { page ->
            page.putRecord(TestRecord(1,1).toByteArray(), 0)
            storage.writePage(page)
        }.id

        val cost = storage.totalAccessCost
        cache.getAndPin(pageId).use {
            assertEquals(TestRecord(1,1), TestRecord.fromByteArray(it.diskPage.getRecord(0).bytes))
            it.diskPage.putRecord(TestRecord(2,2).toByteArray())
        }
        cache.flush()
        assertEquals(TestRecord(2,2), TestRecord.fromByteArray(storage.readPage(pageId).getRecord(1).bytes))
        assertTrue(storage.totalAccessCost > cost)
    }

    @Test
    fun `pin after load costs zero`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage)
        val pageId = storage.createPage().also { page ->
            page.putRecord(TestRecord(1,1).toByteArray(), 0)
            storage.writePage(page)
        }.id
        cache.load(pageId)
        val cost = storage.totalAccessCost

        cache.getAndPin(pageId)
        assertEquals(0.0, storage.totalAccessCost - cost)
    }

    @Test
    fun `sequential load costs less than random gets`() {
        val storage = createHardDriveEmulatorStorage().also { storage ->
            (1 .. 20).forEach { idx ->
                storage.readPage(idx).also { page ->
                    page.putRecord(TestRecord(idx, idx).toByteArray())
                    storage.writePage(page)
                }
            }
        }
        val cache = DummyPageCacheImpl(storage)
        val cost1 = storage.totalAccessCost
        val coldPages = (1 .. 10).map { idx -> cache.getAndPin(idx) }.toList()
        val cost2 = storage.totalAccessCost
        cache.load(11, 10)
        val warmPages = (11 .. 20).map { idx -> cache.getAndPin(idx) }.toList()
        val cost3 = storage.totalAccessCost
        assertTrue(cost2 - cost1 > cost3 - cost2)
    }

    @Test
    fun `pages are evicted when cache is full`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, maxCacheSize = 5)
        cache.load(1, 5)
        val cost1 = storage.totalAccessCost

        cache.getAndPin(10).close()
        val cost2 = storage.totalAccessCost
        assertEquals(1, cache.stats.cacheMiss)

        (1..5).forEach { cache.getAndPin(it) }
        val cost3 = storage.totalAccessCost
        assertEquals(4, cache.stats.cacheHit)
        assertEquals(2, cache.stats.cacheMiss)

        assertTrue(cost3 > cost2)
    }

    @Test
    fun `subcache pages eviction priority`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = DummyPageCacheImpl(storage, maxCacheSize = 10)
        cache.load(1, 5)
        val subcache = cache.createSubCache(5)
        subcache.load(6, 5)
        subcache.getAndPin(6)
        assertEquals(1, cache.stats.cacheHit)
        assertEquals(1, subcache.stats.cacheHit)

        subcache.getAndPin(20)
        subcache.getAndPin(1)
        (1..5).forEach { cache.getAndPin(it) }
        assertEquals(2, subcache.stats.cacheMiss)
        assertEquals(1, cache.stats.cacheMiss)
        assertEquals(6, subcache.stats.cacheHit)
    }
}