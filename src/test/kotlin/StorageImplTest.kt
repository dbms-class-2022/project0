import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertEquals

class StorageImplTest {
    @Test
    fun `create and read page`() {
        createHardDriveEmulatorStorage().let {storage ->
            val pageId1 = storage.createPage().let {page ->
                page.putRecord(TestRecord(1, 1).toByteArray())
                storage.writePage(page)
                page.id
            }
            val pageId2 = storage.createPage().let {page ->
                page.putRecord(TestRecord(2, 2).toByteArray())
                storage.writePage(page)
                page.id
            }
            storage.readPage(pageId1).let {page ->
                assertEquals(TestRecord(1,1), TestRecord.fromByteArray(page.getRecord(0).bytes))
            }
            storage.readPage(pageId2).let {page ->
                assertEquals(TestRecord(2,2), TestRecord.fromByteArray(page.getRecord(0).bytes))
            }
        }
    }

    @Test
    fun `create write and read page sequence`() {
        createHardDriveEmulatorStorage().let {storage ->
            val pageList = (1..10).map { idx ->
                storage.createPage().also { page ->
                    page.putRecord(TestRecord(idx, idx).toByteArray())
                }
            }.toList()
            val writer = storage.writePageSequence()
            val writtenPages = pageList.mapNotNull { writer.apply(it) }.toList()
            writer.apply(null)

            var idx = 1
            storage.readPageSequence(writtenPages.first().id, writtenPages.size) { page ->
                assertEquals(TestRecord(idx, idx), TestRecord.fromByteArray(page.getRecord(0).bytes))
                idx++
            }
        }
    }

    @Test
    fun `read and write random page`() {
        createHardDriveEmulatorStorage().let { storage ->
            repeat(42) {
                storage.readPage(Random.nextInt(100)).let { page ->
                    page.putRecord(TestRecord(page.id, page.id).toByteArray())
                    storage.writePage(page)
                }
            }
            (0..100).forEach {
                storage.readPage(it).let {page ->
                    if (page.allRecords().isNotEmpty()) {
                        assertEquals(TestRecord(page.id, page.id), TestRecord.fromByteArray(page.getRecord(0).bytes))
                    }
                }
            }
        }
    }



}