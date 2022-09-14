package net.barashev.dbi2022.task0

import net.barashev.dbi2022.*
import java.util.*
import kotlin.test.Test

operator fun <A : Any, B : Any, C : Any> Record3<A, B, C>.component1() = value1
operator fun <A : Any, B : Any, C : Any> Record3<A, B, C>.component2() = value2
operator fun <A : Any, B : Any, C : Any> Record3<A, B, C>.component3() = value3

class Task0 {

    private val Date.myYear: Int
        get() {
            val calendar by lazy { Calendar.getInstance() }
            calendar.time = this
            return calendar.get(Calendar.YEAR)
        }

    @Test
    fun dumbTest() {
        val taskSetup = TaskSetup().apply { populateTables() }

        fun getTable1Records() = taskSetup.accessManager.createFullScan("table1") {
            Record3(intField(), stringField(), stringField()).fromBytes(it)
        }.asSequence()

        fun getTable2Records() = taskSetup.accessManager.createFullScan("table2") {
            Record3(intField(), dateField(), stringField()).fromBytes(it)
        }.asSequence()

        val result = getTable2Records().filter { (_, date2, _) ->
            date2.myYear == 2024
        }.flatMap { (id2, date2, _) ->
            getTable1Records()
                .filter { (id1, _, _) -> id1 == id2 }
                .map { (id1, _, weather1) -> Record3(intField(id1), dateField(date2), stringField(weather1)) }
        }.toList() // just to get the size

        result.forEach { println(it) }
        println("Row count: " + result.size)
        println("Total cost: " + taskSetup.storage.totalAccessCost)
    }

}
