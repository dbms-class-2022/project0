package net.barashev.dbi2022.task0

import net.barashev.dbi2022.*
import org.junit.jupiter.api.Test
import java.util.*

typealias T1_ROW = Record3<Int, String, String>
typealias T2_ROW = Record3<Int, Date, String>

fun createRecord (id : Int, date : Date, weather : String) : T2_ROW {
    return Record3(intField(id), dateField(date), stringField(weather))
}

val parseRecordForTable1 : (bytesArray : ByteArray) -> T1_ROW  = { bytesArray ->
    Record3(intField(), stringField(), stringField()).fromBytes(bytesArray)
}

val parseRecordForTable2 : (bytesArray : ByteArray) -> T2_ROW  = { bytesArray ->
    Record3(intField(), dateField(), stringField()).fromBytes(bytesArray)
}

fun<T> getTable(
    accessManager : AccessMethodManager,
    tableName : String,
    parser : (bytesArray : ByteArray) -> T
) : Iterable<T> {
    return accessManager.createFullScan(tableName) { bytesArray -> parser(bytesArray) }
}

const val CONDITION_YEAR = 2024;

fun Date.yearEqTo(year : Int) : Boolean {
    val calendar = Calendar.getInstance()
    calendar.time = this;
    return calendar.get(Calendar.YEAR) == year
}

class QueriesTest {
    @Test
    @Throws(Exception::class)
    fun queryTest() {
        val taskSetup = TaskSetup()
        taskSetup.populateTables()
        val accessManager = taskSetup.accessManager;

        /* id, address, weather */
        val table1 = getTable(accessManager, "table1", parseRecordForTable1)
        /* id, date, buzzwords */
        val table2 = getTable(accessManager, "table2", parseRecordForTable2)

        table1.forEach { rec1 ->
            val id = rec1.value1
            table2
                .filter { rec2 -> id == rec2.value1 }
                .filter { rec2 -> rec2.value2.yearEqTo(CONDITION_YEAR) }
                .forEach { rec2 -> println(createRecord(id, rec2.value2, rec1.value3)) }
        }

        println(taskSetup.storage.totalAccessCost)
    }
}