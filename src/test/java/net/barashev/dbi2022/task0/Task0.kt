package net.barashev.dbi2022.task0

import net.barashev.dbi2022.Record3
import net.barashev.dbi2022.dateField
import net.barashev.dbi2022.intField
import net.barashev.dbi2022.stringField
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*

class Task0 {
    lateinit var taskSetup: TaskSetup

    @BeforeEach
    fun populate() {
        taskSetup = TaskSetup()
        taskSetup.populateTables()
    }

    @Test
    fun task0() {
        // O(table1_size * table2_size)
        val firstTableIterator = taskSetup.accessManager.createFullScan("table1") { bytes ->
            Record3(
                intField(),
                stringField(),
                stringField()
                ).fromBytes(bytes)
        }
        firstTableIterator.forEach { table1Data ->
            val secondTableIterator = taskSetup.accessManager.createFullScan("table2") { bytes ->
                Record3(
                    intField(),
                    dateField(),
                    stringField()
                ).fromBytes(bytes)
            }
            secondTableIterator.forEach { table2Data ->
                if (table1Data.value1 == table2Data.value1) {
                    val calendar = Calendar.getInstance()
                    calendar.time = table2Data.value2
                    println("${table1Data.value1} ${calendar.get(Calendar.YEAR)} ${table1Data.value3}")
                }
            }
        }

        println("Table initialization cost: ${taskSetup.storage.totalAccessCost}")
    }

    @Test
    fun `iterate on first table`() {
        val firstTableIterator = taskSetup.accessManager.createFullScan("table1") { bytes ->
            Record3(
                intField(),
                stringField(),
                stringField()
            ).fromBytes(bytes)
        }
        firstTableIterator.forEach { table1Data ->
            println(table1Data)
        }
    }

    @Test
    fun `iterate on second table`() {
        val secondTableIterator = taskSetup.accessManager.createFullScan("table2") { bytes ->
            Record3(
                intField(),
                dateField(),
                stringField()
            ).fromBytes(bytes)
        }.iterator()
        while (secondTableIterator.hasNext()) {
            println(secondTableIterator)
            secondTableIterator.next()
        }
    }
}