package net.barashev.dbi2022

import net.barashev.dbi2022.fake.FakeMergeSort
import net.barashev.dbi2022.fake.FakeNestedLoops
import net.datafaker.Faker
import org.junit.jupiter.api.Test
import java.util.*
import kotlin.random.Random
import kotlin.test.assertEquals

class OperationsTest  {
    @Test
    fun `fake sort`() {
        Operations.sortFactory = { accessMethodManager, pageCache -> FakeMergeSort(accessMethodManager, pageCache) }
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleAccessMethodManager(cache)
        val fooOid = accessMethodManager.createTable("foo")
        cache.getAndPin(accessMethodManager.addPage(fooOid)).use {inPage ->
            (1..10).forEach { inPage.putRecord(intField().first.asBytes(it)) }
        }
        val outTable = Operations.sortFactory(accessMethodManager, cache).sort("foo") {
            intField().first.fromBytes(it).first
        }
        assertEquals((1..10).toList(), accessMethodManager.createFullScan(outTable) {
            intField().first.fromBytes(it).first
        }.toList())
    }

    @Test
    fun `fake nested loops`() {
        Operations.innerJoinFactory = { accessMethodManager, pageCache, joinAlgorithm ->
            when (joinAlgorithm) {
                JoinAlgorithm.NESTED_LOOPS -> FakeNestedLoops(accessMethodManager, pageCache)
                else -> TODO("Join algorithm $joinAlgorithm not implemented yet")
            }
        }
        val storage = createHardDriveEmulatorStorage()
        val cache = SimplePageCacheImpl(storage, 20)
        val accessMethodManager = SimpleAccessMethodManager(cache)
        val fooOid = accessMethodManager.createTable("foo")

        val faker = Faker()
        cache.getAndPin(accessMethodManager.addPage(fooOid)).use {fooPage ->
            (1..10).forEach { fooPage.putRecord(Record2(intField(it), stringField(faker.name().fullName())).asBytes()) }
        }

        val barOid = accessMethodManager.createTable("bar")
        cache.getAndPin(accessMethodManager.addPage(barOid)).use {barPage ->
            (1..20).forEach { barPage.putRecord(Record2(
                intField(Random.nextInt(10)),
                dateField(Date.from(faker.date().birthday().toInstant()))).asBytes()
            ) }
        }

        Operations.innerJoinFactory(accessMethodManager, cache, JoinAlgorithm.NESTED_LOOPS).join(
            JoinOperand("foo") { Record2(intField(), stringField()).fromBytes(it).value1 },
            JoinOperand("bar") { Record2(intField(), dateField()).fromBytes(it).value1 }
        ).use {
            while (it.hasNext()) {
                val matchingTuples = it.next()
                val leftRecord = Record2(intField(), stringField()).fromBytes(matchingTuples.first)
                val rightRecord = Record2(intField(), dateField()).fromBytes(matchingTuples.second)
                println(leftRecord)
                println(rightRecord)
                println("--------")
            }
        }
    }


}