package net.barashev.dbi2022
import java.util.function.Function

typealias TableRecordParser = (ByteArray) -> Record3<Any, Any, Any>
typealias AttributeValueParser = Function<ByteArray, Comparable<Any>>
typealias JoinSpec = Pair<String, String>

class QueryExecutorException: Exception {
    constructor(message: String): super(message)
    constructor(message: String, cause: Throwable): super(message, cause)
}

class JoinExecutor(
    private val accessMethodManager: AccessMethodManager,
    private val pageCache: PageCache,
    private val tableRecordParsers: Map<String, TableRecordParser>,
    private val attributeValueParsers: Map<String, AttributeValueParser>
) {
    fun buildJoinAttrFxn(joinSpec: JoinSpec): AttributeValueParser {
        val (table, attribute) = joinSpec
        val joinedTables = table.split(",").toList()
        return Function {
            val recordBytes = parseJoinedRecord(it, joinedTables, tableRecordParsers)
            val attributeTable = attribute.split(".")[0]
            val result: Comparable<Any> = attributeValueParsers[attribute]!!.apply(recordBytes[attributeTable]!!)
            result
        }
    }

    fun join(joinSpecs: List<JoinSpec>): JoinSpec {
        val joinStack = ArrayDeque(joinSpecs)
        var result: JoinSpec = joinStack.removeFirst()
        while (joinStack.isNotEmpty()) {
            val rightSpec = joinStack.removeFirst()

            val leftOperand = JoinOperand(result.first, buildJoinAttrFxn(result))
            val rightOperand = JoinOperand(rightSpec.first, buildJoinAttrFxn(rightSpec))
            val innerJoin = createJoinOperation(JoinAlgorithm.HASH).fold(
                onSuccess = { Result.success(it) },
                onFailure = { createJoinOperation(JoinAlgorithm.NESTED_LOOPS) }
            ).fold(
                onSuccess = { Result.success(it) },
                onFailure = {createJoinOperation(JoinAlgorithm.MERGE) }
            ).onFailure {
                throw QueryExecutorException("Can't find working join algorithm", it)
            }
            val joinOutput = innerJoin.getOrThrow().join(leftOperand, rightOperand)

            val outputTableName = "${leftOperand.tableName},${rightOperand.tableName}"
            val outputTableOid = accessMethodManager.createTable(outputTableName)
            TableBuilder(accessMethodManager, pageCache, outputTableOid).use { outputTableBuilder ->
                joinOutput.forEach { pair ->
                    outputTableBuilder.insert(ByteArray(pair.first.size + pair.second.size).also {
                        pair.first.copyInto(it, 0)
                        pair.second.copyInto(it, pair.first.size)
                    })
                }
            }

            if (!joinStack.isEmpty()) {
                val mergeSpec = joinStack.removeFirst()
                result = JoinSpec(outputTableName, mergeSpec.second)
            } else {
                result = JoinSpec(outputTableName, "")
            }
        }
        return result
    }

    fun createJoinOperation(method: JoinAlgorithm): Result<InnerJoin> =
        try {
            Result.success(Operations.innerJoinFactory(accessMethodManager, pageCache, method))
        } catch (ex: NotImplementedError) {
            Result.failure(ex)
        }
}

fun parseJoinedRecord(bytes: ByteArray, tableNames: List<String>, tableRecordParsers: Map<String, TableRecordParser>): Map<String, ByteArray> {
    var suffixStart = 0
    val result = mutableMapOf<String, ByteArray>()
    tableNames.forEach {table ->
        val suffix = bytes.copyOfRange(suffixStart, bytes.size)
        val parser = tableRecordParsers[table]!!
        val record = parser(suffix)
        val recordAsBytes = record.asBytes()
        result[table] = recordAsBytes
        suffixStart += recordAsBytes.size
    }
    return result
}

