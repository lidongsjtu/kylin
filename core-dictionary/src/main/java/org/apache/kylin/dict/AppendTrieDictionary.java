/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.dict;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ComparableWritable;
import org.apache.kylin.common.persistence.Writable;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CachedTreeMap;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.dimension.Dictionary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kylin.dict.AppendTrieDictionaryBuilder.Node;

/**
 * A dictionary based on Trie data structure that maps enumerations of byte[] to
 * int IDs, used for global dictionary.
 *
 * Trie data is split into sub trees, called {@link DictSlice}, and stored in a {@link CachedTreeMap} with a configurable cache size.
 * 
 * With Trie the memory footprint of the mapping is kinda minimized at the cost
 * CPU, if compared to HashMap of ID Arrays. Performance test shows Trie is
 * roughly 10 times slower, so there's a cache layer overlays on top of Trie and
 * gracefully fall back to Trie using a weak reference.
 * 
 * The implementation is thread-safe.
 * 
 * @author sunyerui
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class AppendTrieDictionary<T> extends Dictionary<T> {

    public static final byte[] HEAD_MAGIC = new byte[] { 0x41, 0x70, 0x70, 0x65, 0x63, 0x64, 0x54, 0x72, 0x69, 0x65, 0x44, 0x69, 0x63, 0x74 }; // "AppendTrieDict"
    public static final int HEAD_SIZE_I = HEAD_MAGIC.length;

    public static final int BIT_IS_LAST_CHILD = 0x80;
    public static final int BIT_IS_END_OF_VALUE = 0x40;

    private static final Logger logger = LoggerFactory.getLogger(AppendTrieDictionary.class);

    transient private int baseId;
    transient private int maxValueLength;
    transient private int nValues;
    transient private int sizeOfId;
    transient private long totalBytes;
    transient private BytesConverter<T> bytesConvert;

    transient private boolean enableValueCache = true;
    transient private SoftReference<HashMap> valueToIdCache;

    private TreeMap<DictSliceKey, DictSlice> dictSliceMap;
    private boolean immutable;
    private String uuid;
    private String baseDir;

    // Constructor for deserialize
    public AppendTrieDictionary() {
        this.immutable = true;
        if (enableValueCache) {
            valueToIdCache = new SoftReference<>(new HashMap());
        }
    }

    // Constructor for build
    public AppendTrieDictionary(BytesConverter bytesConvert, String uuid) {
        this.bytesConvert = bytesConvert;
        this.uuid = uuid;
        this.baseDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/dict/" + this.getClass().getSimpleName() + "/" + uuid;
        this.immutable = false;
        initDictSliceMap();
        if (enableValueCache) {
            valueToIdCache = new SoftReference<>(new HashMap());
        }
    }

    private void initDictSliceMap() {
        int cacheSize = KylinConfig.getInstanceFromEnv().getAppendDictCacheSize();
        dictSliceMap = CachedTreeMap.CachedTreeMapBuilder.newBuilder().maxSize(cacheSize)
                .baseDir(baseDir).persistent(true).immutable(immutable).keyClazz(DictSliceKey.class).valueClazz(DictSlice.class).build();
    }

    public void addDictSlice(byte[] sliceBytes) {
        DictSlice slice = new DictSlice(sliceBytes);
        byte[] value = slice.getFirstValue();
        dictSliceMap.put(DictSliceKey.wrap(value), slice);
        maxValueLength = Math.max(maxValueLength, slice.maxValueLength);
        sizeOfId = Math.max(sizeOfId, slice.sizeOfId);
        nValues += slice.nValues;
        totalBytes += slice.trieBytes.length;
        if (bytesConvert == null) {
            bytesConvert = slice.bytesConvert;
        }
        assert bytesConvert.getClass() == slice.bytesConvert.getClass();
    }

    public static class DictSliceKey implements ComparableWritable {
        byte[] key;

        public static DictSliceKey wrap(byte[] key) {
            DictSliceKey dictKey = new DictSliceKey();
            dictKey.key = key;
            return dictKey;
        }

        @Override
        public String toString() {
            return Bytes.toStringBinary(key);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

        @Override
        public int compareTo(Object o) {
            if (!(o instanceof DictSliceKey)) {
                return -1;
            }
            DictSliceKey other = (DictSliceKey)o;
            return Bytes.compareTo(key, other.key);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(key.length);
            out.write(key);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            key = new byte[in.readInt()];
            in.readFully(key);
        }
    }

    public static class DictSlice<T> implements Writable {
        public DictSlice() {}

        public DictSlice(byte[] trieBytes) {
            init(trieBytes);
        }

        private byte[] trieBytes;

        // non-persistent part
        transient private int headSize;
        @SuppressWarnings("unused")
        transient private int bodyLen;
        transient private int sizeChildOffset;
        transient private int maxValueLength;
        transient private BytesConverter<T> bytesConvert;

        transient private int nValues;
        transient private int sizeOfId;
        transient private int childOffsetMask;
        transient private int firstByteOffset;

        private void init(byte[] trieBytes) {
            this.trieBytes = trieBytes;
            if (BytesUtil.compareBytes(HEAD_MAGIC, 0, trieBytes, 0, HEAD_MAGIC.length) != 0)
                throw new IllegalArgumentException("Wrong file type (magic does not match)");

            try {
                DataInputStream headIn = new DataInputStream(new ByteArrayInputStream(trieBytes, HEAD_SIZE_I, trieBytes.length - HEAD_SIZE_I));
                this.headSize = headIn.readShort();
                this.bodyLen = headIn.readInt();
                this.nValues = headIn.readInt();
                this.sizeChildOffset = headIn.read();
                this.sizeOfId = headIn.read();
                this.maxValueLength = headIn.readShort();

                String converterName = headIn.readUTF();
                if (converterName.isEmpty() == false)
                    this.bytesConvert = ClassUtil.forName(converterName, BytesConverter.class).newInstance();

                this.childOffsetMask = ~((BIT_IS_LAST_CHILD | BIT_IS_END_OF_VALUE) << ((sizeChildOffset - 1) * 8));
                this.firstByteOffset = sizeChildOffset + 1; // the offset from begin of node to its first value byte
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    throw (RuntimeException) e;
                else
                    throw new RuntimeException(e);
            }

        }

        public byte[] getFirstValue() {
            int nodeOffset = headSize;
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            while (true) {
                int valueLen = BytesUtil.readUnsigned(trieBytes, nodeOffset + firstByteOffset - 1, 1);
                bytes.write(trieBytes, nodeOffset + firstByteOffset, valueLen);
                if (checkFlag(nodeOffset, BIT_IS_END_OF_VALUE)) {
                    break;
                }
                nodeOffset = headSize + (BytesUtil.readUnsigned(trieBytes, nodeOffset, sizeChildOffset) & childOffsetMask);
                if (nodeOffset == headSize) {
                    break;
                }
            }
            return bytes.toByteArray();
        }

        /**
         * returns a code point from [0, nValues), preserving order of value
         *
         * @param n
         *            -- the offset of current node
         * @param inp
         *            -- input value bytes to lookup
         * @param o
         *            -- offset in the input value bytes matched so far
         * @param inpEnd
         *            -- end of input
         * @param roundingFlag
         *            -- =0: return -1 if not found
         *            -- <0: return closest smaller if not found, return -1
         *            -- >0: return closest bigger if not found, return nValues
         */
        private int lookupSeqNoFromValue(int n, byte[] inp, int o, int inpEnd, int roundingFlag) {
            while (true) {
                // match the current node
                int p = n + firstByteOffset; // start of node's value
                int end = p + BytesUtil.readUnsigned(trieBytes, p - 1, 1); // end of node's value
                for (; p < end && o < inpEnd; p++, o++) { // note matching start from [0]
                    if (trieBytes[p] != inp[o]) {
                        return -1; // mismatch
                    }
                }

                // node completely matched, is input all consumed?
                boolean isEndOfValue = checkFlag(n, BIT_IS_END_OF_VALUE);
                if (o == inpEnd) {
                    return p == end && isEndOfValue ? BytesUtil.readUnsigned(trieBytes, end, sizeOfId) : -1;
                }

                // find a child to continue
                int c = headSize + (BytesUtil.readUnsigned(trieBytes, n, sizeChildOffset) & childOffsetMask);
                if (c == headSize) // has no children
                    return -1;
                byte inpByte = inp[o];
                int comp;
                while (true) {
                    p = c + firstByteOffset;
                    comp = BytesUtil.compareByteUnsigned(trieBytes[p], inpByte);
                    if (comp == 0) { // continue in the matching child, reset n and loop again
                        n = c;
                        break;
                    } else if (comp < 0) { // try next child
                        if (checkFlag(c, BIT_IS_LAST_CHILD))
                            return -1;
                        c = p + BytesUtil.readUnsigned(trieBytes, p - 1, 1) + (checkFlag(c, BIT_IS_END_OF_VALUE) ? sizeOfId : 0);
                    } else { // children are ordered by their first value byte
                        return -1;
                    }
                }
            }
        }

        private boolean checkFlag(int offset, int bit) {
            return (trieBytes[offset] & bit) > 0;
        }

        public int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
            int id = lookupSeqNoFromValue(headSize, value, offset, offset + len, roundingFlag);
            if (id < 0)
                logger.error("Not a valid value: " + bytesConvert.convertFromBytes(value, offset, len));
            return id;
        }

        private Node rebuildTrieTree(AtomicInteger maxId) {
           return rebuildTrieTreeR(headSize, null, maxId);
        }

        private Node rebuildTrieTreeR(int n, Node parent, AtomicInteger maxId) {
            Node root = null;
            while (true) {
                int p = n + firstByteOffset;
                int childOffset = BytesUtil.readUnsigned(trieBytes, n, sizeChildOffset) & childOffsetMask;
                int parLen = BytesUtil.readUnsigned(trieBytes, p - 1, 1);
                boolean isEndOfValue = checkFlag(n, BIT_IS_END_OF_VALUE);

                byte[] value = new byte[parLen];
                System.arraycopy(trieBytes, p, value, 0, parLen);

                Node node = new Node(value, isEndOfValue);
                if (isEndOfValue) {
                    int id = BytesUtil.readUnsigned(trieBytes, p + parLen, sizeOfId);
                    node.id = id;
                    if (maxId.get() < id) {
                        maxId.set(id);
                    }
                }

                if (parent == null) {
                    root = node;
                } else {
                    parent.addChild(node);
                }

                if (childOffset != 0) {
                    rebuildTrieTreeR(childOffset + headSize, node, maxId);
                }

                if (checkFlag(n, BIT_IS_LAST_CHILD)) {
                    break;
                } else {
                    n += firstByteOffset + parLen + (isEndOfValue ? sizeOfId : 0);
                }
            }
            return root;
        }

        public void write(DataOutput out) throws IOException {
            out.write(trieBytes);
        }

        public void readFields(DataInput in) throws IOException {
            byte[] headPartial = new byte[HEAD_MAGIC.length + Short.SIZE + Integer.SIZE];
            in.readFully(headPartial);

            if (BytesUtil.compareBytes(HEAD_MAGIC, 0, headPartial, 0, HEAD_MAGIC.length) != 0)
                throw new IllegalArgumentException("Wrong file type (magic does not match)");

            DataInputStream headIn = new DataInputStream( //
                    new ByteArrayInputStream(headPartial, HEAD_SIZE_I, headPartial.length - HEAD_SIZE_I));
            int headSize = headIn.readShort();
            int bodyLen = headIn.readInt();
            headIn.close();

            byte[] all = new byte[headSize + bodyLen];
            System.arraycopy(headPartial, 0, all, 0, headPartial.length);
            in.readFully(all, headPartial.length, all.length - headPartial.length);

            init(all);
        }

        @Override
        public String toString() {
            return String.format("DictSlice[firstValue=%s, values=%d, bytes=%d]", Bytes.toStringBinary(getFirstValue()), nValues, bodyLen);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(trieBytes);
        }

        @Override
        public boolean equals(Object o) {
            if ((o instanceof AppendTrieDictionary.DictSlice) == false) {
                logger.info("Equals return false because it's not DictInfo");
                return false;
            }
            DictSlice that = (DictSlice) o;
            return Arrays.equals(this.trieBytes, that.trieBytes);
        }
    }

    @Override
    protected int getIdFromValueBytesImpl(byte[] value, int offset, int len, int roundingFlag) {
        if (dictSliceMap.isEmpty()) {
            return -1;
        }
        byte[] tempVal = new byte[len];
        System.arraycopy(value, offset, tempVal, 0, len);
        DictSliceKey sliceKey = dictSliceMap.floorKey(DictSliceKey.wrap(tempVal));
        if (sliceKey == null) {
            sliceKey = dictSliceMap.firstKey();
        }
        DictSlice slice = dictSliceMap.get(sliceKey);
        int id = slice.getIdFromValueBytesImpl(value, offset, len, roundingFlag);
        return id;
    }

    @Override
    public int getMinId() {
        return baseId;
    }

    @Override
    public int getMaxId() {
        return baseId + nValues - 1;
    }

    @Override
    public int getSizeOfId() {
        return sizeOfId;
    }

    @Override
    public int getSizeOfValue() {
        return maxValueLength;
    }

    @Override
    final protected int getIdFromValueImpl(T value, int roundingFlag) {
        if (enableValueCache && roundingFlag == 0) {
            HashMap cache = valueToIdCache.get(); // SoftReference to skip cache gracefully when short of memory
            if (cache != null) {
                Integer id = null;
                id = (Integer) cache.get(value);
                if (id != null)
                    return id.intValue();

                byte[] valueBytes = bytesConvert.convertToBytes(value);
                id = getIdFromValueBytes(valueBytes, 0, valueBytes.length, roundingFlag);

                cache.put(value, id);
                return id;
            }
        }
        byte[] valueBytes = bytesConvert.convertToBytes(value);
        return getIdFromValueBytes(valueBytes, 0, valueBytes.length, roundingFlag);
    }

    @Override
    final protected T getValueFromIdImpl(int id) {
        throw new UnsupportedOperationException("AppendTrieDictionary can't retrive value from id");
    }

    @Override
    protected byte[] getValueBytesFromIdImpl(int id) {
        throw new UnsupportedOperationException("AppendTrieDictionary can't retrive value from id");
    }

    @Override
    protected int getValueBytesFromIdImpl(int id, byte[] returnValue, int offset) {
        throw new UnsupportedOperationException("AppendTrieDictionary can't retrive value from id");
    }

    public AppendTrieDictionaryBuilder<T> rebuildTrieTree() {
        AtomicInteger maxId = new AtomicInteger(0);
        AppendTrieDictionaryBuilder builder = new AppendTrieDictionaryBuilder(bytesConvert, uuid);
        if (dictSliceMap != null) {
            for (DictSlice slice : dictSliceMap.values()) {
                Node root = slice.rebuildTrieTree(maxId);
                builder.addDictSlice(root, maxId.get());
            }
        }

        return builder;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(uuid);
        out.writeUTF(baseDir);
        out.writeInt(maxValueLength);
        out.writeInt(sizeOfId);
        out.writeInt(nValues);
        out.writeLong(totalBytes);
        out.writeUTF(bytesConvert.getClass().getName());
        ((Writable)dictSliceMap).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = in.readUTF();
        this.baseDir = in.readUTF();
        this.maxValueLength = in.readInt();
        this.sizeOfId = in.readInt();
        this.nValues = in.readInt();
        this.totalBytes = in.readLong();
        String converterName = in.readUTF();
        if (converterName.isEmpty() == false)
        try {
            this.bytesConvert = ClassUtil.forName(converterName, BytesConverter.class).newInstance();
        } catch (Exception e) {
            throw new IOException(e);
        }
        initDictSliceMap();
        ((Writable)this.dictSliceMap).readFields(in);
    }

    @Override
    public void dump(PrintStream out) {
        out.println("Total " + nValues + " values, " + (dictSliceMap == null ? 0 : dictSliceMap.size()) + " slice, size " + totalBytes + " bytes");
    }

    @Override
    public int hashCode() {
        int hashCode = 31;
        for (DictSlice slice : dictSliceMap.values()) {
            hashCode += 31 * slice.hashCode();
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }

    @Override
    public boolean contains(Dictionary other) {
        return false;
    }
}
