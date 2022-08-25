import java.lang.IllegalArgumentException
import java.nio.ByteBuffer
import java.util.*
import java.util.function.Consumer
import java.util.function.Function
import kotlin.math.absoluteValue
import kotlin.math.sign

private var pageCount = 0
fun createDiskPage(): DiskPage = DiskPageImpl(pageCount++, DEFAULT_DISK_PAGE_SIZE)
fun createHardDriveEmulatorStorage(): Storage = HardDiskEmulatorStorage()

private class DiskPageImpl(
    override val id: PageId,
    private val pageSize: Int,
    private val bytes: ByteArray = ByteArray(pageSize)) : DiskPage {

    private val directoryStartOffset = Int.SIZE_BYTES
    private var directorySize = 0
        set(value) {
            field = value
            this.bytes.setDirectorySize(value)
        }

    private var lastRecordOffset: Int = pageSize

    override val rawBytes: ByteArray get() = bytes
    private val byteBuffer: ByteBuffer get() = ByteBuffer.wrap(bytes, 0, pageSize)

    override val freeSpace: Int
        get() = lastRecordOffset - directorySize * Int.SIZE_BYTES - directoryStartOffset

    init {
        directorySize = this.bytes.getDirectorySize()
    }
    override fun putRecord(recordData: ByteArray, recordId_: RecordId): PutRecordResult {
        val recordId = if (recordId_ == -1) directorySize else recordId_
        return if (recordId < 0 || recordId > directorySize) {
            PutRecordResult(recordId, isOutOfSpace = false, isOutOfRange = true)
        } else {
            if (recordId == directorySize) {
                if (recordData.size > freeSpace + Int.SIZE_BYTES) {
                    PutRecordResult(recordId, isOutOfSpace = true, isOutOfRange = false)
                } else {
                    val newLastRecordOffset = lastRecordOffset - recordData.size
                    bytes.setDirectoryEntry(recordId, newLastRecordOffset)
                    ByteBuffer.wrap(bytes, newLastRecordOffset, recordData.size).put(recordData)

                    lastRecordOffset = newLastRecordOffset
                    directorySize += 1
                    PutRecordResult(recordId, isOutOfSpace = false, isOutOfRange = false)
                }
            } else {
                getByteBuffer(recordId).let { bytes ->
                    val requiredSpace = recordData.size - bytes.first.capacity()
                    if (freeSpace < requiredSpace) {
                        PutRecordResult(recordId, isOutOfSpace = true, isOutOfRange = false)
                    } else {
                        shiftRecords(recordId, requiredSpace)
                        getByteBuffer(recordId).let { newBytes ->
                            assert(newBytes.first.capacity() == recordData.size)
                            newBytes.first.put(recordData)
                            this.bytes.setDirectoryEntry(recordId, newBytes.first.arrayOffset())
                            PutRecordResult(recordId, isOutOfSpace = false, isOutOfRange = false)
                        }
                    }
                }
            }
        }
    }

    override fun getRecord(recordId: RecordId): GetRecordResult =
        if (recordId < 0 || recordId >= directorySize) {
            GetRecordResult(EMPTY_BYTE_ARRAY, isDeleted = false, isOutOfRange = true)
        } else {
            getByteBuffer(recordId).let {buffer ->
                if (buffer.second) {
                    GetRecordResult(EMPTY_BYTE_ARRAY, isDeleted = true, isOutOfRange = false)
                } else {
                    GetRecordResult(buffer.first.toBytes(), isDeleted = false, isOutOfRange = false)
                }
            }
        }


    override fun deleteRecord(recordId: RecordId) {
        if (recordId in 0 until directorySize) {
            val recordOffset = bytes.getDirectoryEntry(recordId)
            if (recordOffset > 0) {
                bytes.setDirectoryEntry(recordId, -recordOffset)
            }
        }
    }

    override fun allRecords(): Map<RecordId, GetRecordResult> =
        (0 until directorySize)
            .mapNotNull { recordId -> getByteBuffer(recordId).let {
                recordId to GetRecordResult(it.first.toBytes(), isDeleted = it.second, isOutOfRange = false)
            }}
            .toMap()


    private fun getByteBuffer(recordId: RecordId): Pair<ByteBuffer,  Boolean> {
        val byteBuffer = ByteBuffer.wrap(bytes, 0, pageSize)
        val offset = bytes.getDirectoryEntry(recordId)
        val prevRecordOffset = if (recordId == 0) pageSize else bytes.getDirectoryEntry(recordId - 1)
        val slice = byteBuffer.slice(offset.absoluteValue, prevRecordOffset.absoluteValue - offset.absoluteValue)
        return slice to (offset < 0)
    }

    private fun shiftRecords(startRecordId: RecordId, requiredSpace: Int) {
        if (requiredSpace == 0) {
            return
        }
        val startRecordOffset = bytes.getDirectoryEntry(startRecordId).absoluteValue
        val shiftedBytes = byteBuffer.slice(lastRecordOffset, startRecordOffset - lastRecordOffset).toBytes()
        val newLastRecordOffset = lastRecordOffset - requiredSpace
        byteBuffer.put(newLastRecordOffset, shiftedBytes)
        if (requiredSpace < 0) {
            // We are freeing space, let's fill the new space with zeroes
            byteBuffer.put(lastRecordOffset, ByteArray(requiredSpace.absoluteValue))
        }
        for (i in startRecordId until directorySize) {
            val offset = this.bytes.getDirectoryEntry(i)
            // Preserve negative sign of deleted records.
            this.bytes.setDirectoryEntry(i, offset.sign * (offset.absoluteValue - requiredSpace))
        }
        lastRecordOffset = newLastRecordOffset
    }

    private fun ByteArray.getDirectoryEntry(idx: Int) =
        ByteBuffer.wrap(this, 0, pageSize).getInt(directoryStartOffset + idx * Int.SIZE_BYTES)

    private fun ByteArray.setDirectoryEntry(idx: Int, value: Int) =
        ByteBuffer.wrap(this, 0, pageSize).putInt(directoryStartOffset + idx * Int.SIZE_BYTES, value)

    private fun ByteArray.getDirectorySize() = ByteBuffer.wrap(this, 0, pageSize).getInt(0)
    private fun ByteArray.setDirectorySize(size: Int) = ByteBuffer.wrap(this, 0, pageSize).putInt(0, size)

    private fun ByteBuffer.toBytes() = ByteArray(this.limit()).also {this.get(it)}
}

private class HardDiskEmulatorStorage: Storage {
    private var accessCostMs = 0.0
    private val pageMap = TreeMap<PageId, DiskPage>()

    override val totalAccessCost: Double get() = accessCostMs
    override fun readPage(pageId: PageId): DiskPage =
        pageMap.getOrElse(pageId) {
            DiskPageImpl(pageId, DEFAULT_DISK_PAGE_SIZE)
        }.also {
            countRandomAccess(pageId)
        }


    override fun readPageSequence(startPageId: PageId, numPages: Int, reader: Consumer<DiskPage>) {
        (startPageId until startPageId+numPages).forEach {
            reader.accept(readPage(it))
        }
        countRandomAccess(startPageId)
        countSeqScan(numPages)
    }

    override fun writePage(page: DiskPage) {
        if (page.id < 0) {
            throw IllegalArgumentException("A disk page is supposed to have an ID")
        }
        pageMap[page.id] = page
        countRandomAccess(page.id)
    }

    override fun createPage(): DiskPage {
        val nextKey = if (pageMap.isEmpty()) 0 else pageMap.lastKey() + 1
        return DiskPageImpl(nextKey, DEFAULT_DISK_PAGE_SIZE).also {
            pageMap[nextKey] = it
            countRandomAccess(nextKey)
        }
    }

    override fun writePageSequence(): Function<in DiskPage?, out DiskPage?> {
        var nextKey = pageMap.lastKey() + 1
        var numPages = 0
        countRandomAccess(nextKey)
        return Function { pageIn ->
            if (pageIn == null) {
                countSeqScan(numPages)
                null
            } else {
                DiskPageImpl(nextKey, DEFAULT_DISK_PAGE_SIZE, pageIn.rawBytes).also {
                    pageMap[nextKey] = it
                    numPages++
                    nextKey++
                }
            }
        }
    }

    private fun countRandomAccess(pageId: PageId) {
        accessCostMs += 5.0
    }

    private fun countSeqScan(numPages: Int) {
        // 5400 rotations per minute = 90 rotations per second = 1 rotation per 0.0111 seconds
        // 1 page is 8 sectors and there are 64 sectors per track => there are 8 pages per track
        // => we need 1/8 rotation = 1.3ms to read the whole page
        accessCostMs = 1.3 * numPages
    }
}

private val EMPTY_BYTE_ARRAY = ByteArray(0)
private val DEFAULT_DISK_PAGE_SIZE = 4096