package net.barashev.dbi2022

import net.barashev.dbi2022.app.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class QueryExecutorTest {
    fun generateData(accessMethodManager: AccessMethodManager, cache: PageCache) {
        DataGenerator(accessMethodManager, cache, 1, true, true)
    }

    @Test
    fun `push down predicate to join leaf`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessMethodManager) = initializeFactories(storage, 20)
        generateData(accessMethodManager, cache)

        val plan = QueryPlan(listOf(
            JoinSpec("planet", "id").also {
                it.filterBy(FilterSpec("planet", "id", 1 as Comparable<Any>, EQ))
            } to JoinSpec("flight", "planet_id").also {
                it.filterBy(FilterSpec("flight", "num", 2 as Comparable<Any>, GT))
            },
            JoinSpec("flight", "spacecraft_id") to JoinSpec("spacecraft", "id").also {
                it.filterBy(FilterSpec("spacecraft", "id", 2 as Comparable<Any>, EQ))
            }
        ), emptyList())

        var resultRowCount = 0
        val resultSpec = QueryExecutor(accessMethodManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        accessMethodManager.createFullScan(resultSpec.tableName) { bytes ->
            parseJoinedRecord(bytes, resultSpec.realTables, tableRecordParsers)
        }.forEach {
            resultRowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                when (tableName) {
                    "planet" -> assertEquals<Any>(1, attributeValueParsers["planet.id"]!!.apply(recordBytes))
                    "flight" -> assertTrue(attributeValueParsers["flight.num"]!!.apply(recordBytes) > 2)
                    "spacecraft" -> assertEquals<Any>(2, attributeValueParsers["spacecraft.id"]!!.apply(recordBytes))
                }
            }
        }
        assertTrue(resultRowCount > 0)
    }

    @Test
    fun `push down predicate to join inner node`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessMethodManager) = initializeFactories(storage, 20)
        generateData(accessMethodManager, cache)

        val plan = QueryPlan(listOf(
            JoinSpec("planet", "id").also {
                it.filterBy(FilterSpec("planet", "id", 1 as Comparable<Any>, EQ))
            } to JoinSpec("flight", "planet_id"),

            JoinSpec("flight", "spacecraft_id").also {
                it.filterBy(FilterSpec("flight", "num", 100 as Comparable<Any>, LE))
            } to JoinSpec("spacecraft", "id")

        ), emptyList())

        var resultRowCount = 0
        val resultSpec = QueryExecutor(accessMethodManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        accessMethodManager.createFullScan(resultSpec.tableName) { bytes ->
            parseJoinedRecord(bytes, resultSpec.realTables, tableRecordParsers)
        }.forEach {
            resultRowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                when (tableName) {
                    "planet" -> assertEquals<Any>(1, attributeValueParsers["planet.id"]!!.apply(recordBytes))
                    "flight" -> assertTrue(attributeValueParsers["flight.num"]!!.apply(recordBytes) <= 100)
                }
            }
        }
        assertTrue(resultRowCount > 0)
    }

    @Test
    fun `apply filters after joins`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessMethodManager) = initializeFactories(storage, 20)
        generateData(accessMethodManager, cache)

        val plan = QueryPlan(listOf(
            JoinSpec("planet", "id") to JoinSpec("flight", "planet_id"),
            JoinSpec("flight", "spacecraft_id") to JoinSpec("spacecraft", "id")
        ), listOf(
            FilterSpec("planet", "id", 1 as Comparable<Any>, EQ),
            FilterSpec("flight", "num", 2 as Comparable<Any>, GT),
            FilterSpec("spacecraft", "id", 2 as Comparable<Any>, EQ)
        ))

        var resultRowCount = 0
        val resultSpec = QueryExecutor(accessMethodManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        accessMethodManager.createFullScan(resultSpec.tableName) { bytes ->
            parseJoinedRecord(bytes, resultSpec.realTables, tableRecordParsers)
        }.forEach {
            resultRowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                when (tableName) {
                    "planet" -> assertEquals<Any>(1, attributeValueParsers["planet.id"]!!.apply(recordBytes))
                    "flight" -> assertTrue(attributeValueParsers["flight.num"]!!.apply(recordBytes) > 2)
                    "spacecraft" -> assertEquals<Any>(2, attributeValueParsers["spacecraft.id"]!!.apply(recordBytes))
                }
            }
        }
        assertTrue(resultRowCount > 0)
    }

    @Test
    fun `filter using index`() {
        val storage = createHardDriveEmulatorStorage()
        val (cache, accessMethodManager) = initializeFactories(storage, 20)
        generateData(accessMethodManager, cache)
        accessMethodManager.createIndex("flight", "num", IntAttribute()) {
            flightRecord(it).value1
        }

        val plan = QueryPlan(listOf(
            JoinSpec("planet", "id") to JoinSpec("flight", "planet_id").also {
                it.filterBy(FilterSpec("flight", "num", 2 as Comparable<Any>, EQ, useIndex = true))
            },
            JoinSpec("flight", "spacecraft_id") to JoinSpec("spacecraft", "id")
        ), emptyList())

        var resultRowCount = 0
        val resultSpec = QueryExecutor(accessMethodManager, cache, tableRecordParsers, attributeValueParsers).execute(plan)
        accessMethodManager.createFullScan(resultSpec.tableName) { bytes ->
            parseJoinedRecord(bytes, resultSpec.realTables, tableRecordParsers)
        }.forEach {
            resultRowCount++
            it.entries.forEach { (tableName, recordBytes) ->
                when (tableName) {
                    "flight" -> assertEquals<Any>(2, attributeValueParsers["flight.num"]!!.apply(recordBytes))
                }
            }
        }
        assertTrue(resultRowCount > 0)
    }

}