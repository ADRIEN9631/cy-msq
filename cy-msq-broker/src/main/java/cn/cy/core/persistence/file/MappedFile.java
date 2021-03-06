package cn.cy.core.persistence.file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import cn.cy.common.utils.CalcUtils;
import cn.cy.core.persistence.PersistenceProcessor;

/**
 * 描述单个文件的对象
 * 这里封装基于Byte[]的操作
 * 封装了MappedByteChannel相关细节
 * 为上层暴露操作文件的接口
 * 加锁不在这里做
 */
public class MappedFile implements PersistenceProcessor {

    private FileChannel fileChannel;

    private Path path;

    /**
     * 用于追加写的buffer
     */
    private MappedByteBuffer appendBuffer;

    /**
     * 页管理
     */
    private Cache<Long, MappedByteBuffer> cachePages;

    /**
     * 尾部偏移量大小
     */
    private long tailOffset;

    /**
     * 每一页的大小
     */
    private static final int PAGE_SIZE = 8192;

    private MappedFile() {
        buildCache();
    }

    private void assertFileExist(Path path) throws FileNotFoundException {
        if (!Files.exists(path)) {
            throw new FileNotFoundException(" path : " + path.toString() + " not found ! ");
        }
    }

    public MappedFile(Path path, boolean assertExist) throws FileNotFoundException {
        this();
        this.path = path;
        this.tailOffset = 0L;
        if (assertExist) {
            assertFileExist(path);
        }
    }

    public MappedFile(Path path, Long tailOffset, boolean assertExist) throws FileNotFoundException {
        this(path, assertExist);
        this.tailOffset = tailOffset;
    }

    private void buildCache() {
        this.cachePages = CacheBuilder.newBuilder()
                .maximumSize(200).build();
    }

    private void ensureOpen() throws IOException {
        if (fileChannel == null) {
            if (!Files.exists(path)) {
                Files.createFile(path);
            }
            fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ);
        }
    }

    private void ensureAppendBuffer() throws IOException {
        if (appendBuffer == null) {
            appendBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, tailOffset, PAGE_SIZE);
        }
    }

    /**
     * append the bytes to the file
     * must be called in the sync field
     *
     * @param bytes the bytes array to be written to the file
     * @param force indicates that if the content will be written to the file immediately
     */
    public AppendInfo append(byte[] bytes, boolean force) throws IOException {

        ensureOpen();

        ensureAppendBuffer();

        AppendInfo appendInfo = new AppendInfo(tailOffset, (long) bytes.length);

        // 预估大小
        if (appendBuffer.position() + bytes.length > appendBuffer.capacity()) {
            // 扩充大小
            appendBuffer = allocateAppendableBuffer(tailOffset,
                    Math.max(PAGE_SIZE, CalcUtils.enlargeToNextMultiple(bytes.length, PAGE_SIZE)));
        }

        for (Byte aByte : bytes) {
            appendBuffer.put(aByte);
        }

        tailOffset += bytes.length;

        if (force) {
            appendBuffer.force();
        }

        return appendInfo;
    }

    /**
     * allocate a new region for the {@link MappedFile#appendBuffer}
     *
     * @param startOffset
     * @param length
     *
     * @return
     *
     * @throws IOException
     */
    private MappedByteBuffer allocateAppendableBuffer(long startOffset, long length) throws IOException {
        return fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, length);
    }

    /**
     * allocate a readonly buffer
     *
     * @param startOffset
     * @param length
     *
     * @return
     *
     * @throws IOException
     */
    private MappedByteBuffer allocateReadonlyBuffer(long startOffset, long length) throws IOException {
        return fileChannel.map(FileChannel.MapMode.READ_ONLY, startOffset, length);
    }

    /**
     * read the file
     * must be called in the sync field
     */
    public byte[] read(final long offset, final long length) throws IOException {
        ensureOpen();

        // 当前读的页的头offset
        long pageHeadOffset = CalcUtils.reduceToPreMultiple(offset, PAGE_SIZE);
        // 当前要读的真实offset
        long readOffset = offset;
        // 剩余要读的长度
        long remain = length;

        List<Byte> res = Lists.newArrayList();
        for (; remain > 0; ) {

            MappedByteBuffer readOnlyBuffer = getReadBuffer(pageHeadOffset);

            long readLength = CalcUtils.enlargeToNextMultiple(readOffset, PAGE_SIZE) - readOffset;

            // 说明读到了最后一个page
            if (readLength > remain) {
                readLength = remain;
            }

            res.addAll(readFromBufferAtIndex(readOnlyBuffer, readOffset % PAGE_SIZE, readLength));

            remain -= readLength;
            pageHeadOffset += PAGE_SIZE;
            readOffset += readLength;

        }

        return Bytes.toArray(res);
    }

    /**
     * read from the buffer
     *
     * @param buffer
     * @param startIndex
     * @param length
     *
     * @return
     */
    private List<Byte> readFromBufferAtIndex(MappedByteBuffer buffer, long startIndex, long length) {

        assert (int) startIndex == startIndex;
        assert (int) length == length;

        buffer.limit((int) startIndex + (int) length);
        buffer.position((int) startIndex);

        List<Byte> list = Lists.newArrayList();

        for (; buffer.position() < buffer.limit(); ) {
            list.add(buffer.get());
        }

        return list;
    }

    /**
     * calculate the read Buffer
     * <p>
     * the offset should be divided by {@link MappedFile#PAGE_SIZE}
     */
    private MappedByteBuffer getReadBuffer(long offset) throws IOException {

        assert offset % PAGE_SIZE == 0;

        /**
         * 这里原本有一个特判
         * 但是根据{@link cn.cy.core.persistence.FileChannelTest#testReadOnlyAccessible()} 这个单测
         * 发现 {@link java.nio.channels.FileChannel.MapMode#READ_ONLY} 和
         * {@link java.nio.channels.FileChannel.MapMode#READ_WRITE}
         * 是可见的
         * 所以，即使有和 {@link MappedFile#appendBuffer} 重叠的部分,也都是readOnly的
         */
        Long pageNo = offset / PAGE_SIZE;
        MappedByteBuffer res = readFromCacheByIndex(pageNo);

        if (res == null) {
            res = allocateReadonlyBuffer(offset, PAGE_SIZE);
            updateCache(pageNo, res);
        }

        return res;
    }

    private MappedByteBuffer readFromCacheByIndex(Long index) {
        return cachePages.getIfPresent(index);
    }

    private void updateCache(Long index, MappedByteBuffer content) {
        cachePages.put(index, content);
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
