package net.barashev.dbi2022

import net.datafaker.Faker
import org.junit.jupiter.api.Test

class IndexesTest {
    val faker = Faker()
    @Test
    fun `fake index test`() {
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleAccessMethodManager(cache)
        val fooOid = accessMethodManager.createTable("foo")
        cache.getAndPin(accessMethodManager.addPage(fooOid)).use {inPage ->
            (1..10).forEach { inPage.putRecord(Record2(intField(it), stringField(faker.name().fullName())).asBytes()) }
        }
        val index1 = IndexManager.indexFactory.build("foo", "foo_id_idx", keyType = IntAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value1
        }
        index1.lookup(10)


        val index2 = IndexManager.indexFactory.build("foo", "foo_name_idx", keyType = StringAttribute()) {
            Record2(intField(), stringField()).fromBytes(it).value2
        }
        index2.lookup("John Smith")
    }

}