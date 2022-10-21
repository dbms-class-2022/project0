package net.barashev.dbi2022.app

import java.util.function.BiPredicate

data class FilterSpec(
    val tableName: String, val attributeName: String, val attributeValue: Comparable<Any>,
    val op: BiPredicate<Comparable<Any>, Comparable<Any>>, val useIndex: Boolean = false) {
    val attribute get() =
        if (attributeName.indexOf('.') >= 0) {
            attributeName
        } else "$tableName.$attributeName"

}

class JoinSpec(val tableName: String, val attributeName: String) {
    val attribute get() =
        if (attributeName.indexOf('.') >= 0) {
            attributeName
        } else {
            "${realTables.joinToString(separator = ",")}.${attributeName}"
        }

    var filter: FilterSpec? = null


    operator fun component1() = tableName
    operator fun component2() = attribute
    override fun toString(): String {
        return "JoinSpec(tableName='$tableName', attributeShortName='$attributeName')"
    }

    fun filterBy(filterSpec: FilterSpec) {
        this.filter = filterSpec
    }
    val realTables: List<String> = tableName.split(",").filter { !it.startsWith("@") }
}

typealias JoinTree = List<Pair<JoinSpec, JoinSpec>>
class QueryPlan(val joinTree: JoinTree, val filters: List<FilterSpec>)

fun parseJoinClause(joinClause: String): List<Pair<JoinSpec, JoinSpec>> =
    if (joinClause.isBlank()) emptyList()
    else
        joinClause.split("""\s+""".toRegex()).map { pair ->
            val (left, right) = pair.split(":", limit = 2)
            val parseSpec = { stringSpec: String ->
                val (table, attribute) = stringSpec.split(".", limit = 2)
                JoinSpec(table, attribute)
            }
            parseSpec(left) to parseSpec(right)
        }.toList()

val EQ: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left == right }
val GT: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left > right }
val LT: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left < right }
val LE: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left <= right }
val GE: BiPredicate<Comparable<Any>, Comparable<Any>> = BiPredicate { left, right -> left >= right }

fun parseFilterClause(filterClause: String): List<FilterSpec> =
    if (filterClause.isBlank()) emptyList()
    else
        filterClause.split("""&+""".toRegex()).map { condition ->
            val (attributeFullName, predicate, literal) = condition.trim().split("""\s+""".toRegex(), limit = 3)
            val (table, attribute) = attributeFullName.split(".", limit = 2)
            FilterSpec(table.trim(), attribute.trim(), literal.trim().toInt() as Comparable<Any>,
                when (predicate.trim()) {
                    "=" -> EQ
                    ">" -> GT
                    "<" -> LT
                    "<=" -> LE
                    ">=" -> GE
                    else -> error("Unsupported predicate $predicate")
                }
            )
        }.toList()

data class IndexSpec(val tableName: String, val attributeName: String)
fun parseIndexClause(indexClause: String): List<IndexSpec> =
    if (indexClause.isBlank()) emptyList()
    else
        indexClause.split(",").map { indexSpec ->
            val (table, attribute) = indexSpec.trim().split(".", limit = 2)
            IndexSpec(table, attribute)
        }.toList()

