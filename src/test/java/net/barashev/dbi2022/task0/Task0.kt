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

        val result = getTable1Records().flatMap { (id1, _, weather1) ->
            getTable2Records().filter { (id2, date2, _) ->
                id1 == id2 && date2.myYear == 2024
            }.map { (_, date2, _) -> Record3(intField(id1), dateField(date2), stringField(weather1)) }
        }

        result.forEach { println(it) }
        println("Total cost: ${taskSetup.storage.totalAccessCost}")
    }

}
