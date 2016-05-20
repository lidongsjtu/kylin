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
import org.apache.kylin.common.persistence.Writable;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CachedTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import static org.apache.kylin.dict.AppendTrieDictionary.DictSliceKey;

/**
 * Builds an append dictionary using Trie structure.
 * All values are taken in byte[] form * and organized in a Trie WITHOUT ordering ensure.
 * Large Trie tree will be split into sub trees, called slice.
 * All slice stored in a {@link CachedTreeMap} with a configurable cache size.
 * @author sunyerui
 */
public class AppendTrieDictionaryBuilder<T> {
    private static final Logger logger = LoggerFactory.getLogger(AppendTrieDictionaryBuilder.class);

    public static class Node implements Writable {
        public byte[] part;
        public int id = -1;
        public boolean isEndOfValue;
        public ArrayList<Node> children = new ArrayList<>();

        public int nValuesBeneath;
        public Node parent;
        public int childrenCount = 1;

        public Node() {}

        Node(byte[] value, boolean isEndOfValue) {
            reset(value, isEndOfValue);
        }

        Node(byte[] value, boolean isEndOfValue, ArrayList<Node> children) {
            reset(value, isEndOfValue, children);
        }

        void reset(byte[] value, boolean isEndOfValue) {
            reset(value, isEndOfValue, new ArrayList<Node>());
        }

        void reset(byte[] value, boolean isEndOfValue, ArrayList<Node> children) {
            this.part = value;
            this.isEndOfValue = isEndOfValue;
            clearChild();
            for (Node child : children) {
                addChild(child);
            }
            this.id = -1;
        }

        void clearChild() {
            this.children.clear();
            int childrenCountDelta = this.childrenCount - 1;
            for (Node p = this; p != null; p = p.parent) {
                p.childrenCount -= childrenCountDelta;
            }
        }

        void addChild(Node child) {
            addChild(-1, child);
        }

        void addChild(int index, Node child) {
            child.parent = this;
            if (index < 0) {
                this.children.add(child);
            } else {
                this.children.add(index, child);
            }
            for (Node p = this; p != null; p = p.parent) {
                p.childrenCount += child.childrenCount;
            }
        }

        public Node removeChild(int index) {
            Node child = children.remove(index);
            child.parent = null;
            for (Node p = this; p != null; p = p.parent) {
                p.childrenCount -= child.childrenCount;
            }
            return child;
        }

        public Node duplicateNode() {
            Node newChild = new Node(part, false);
            newChild.parent = parent;
            if (parent != null) {
                int index = parent.children.indexOf(this);
                parent.addChild(index + 1, newChild);
            }
            return newChild;
        }

        public byte[] firstValue() {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            Node p = this;
            while (true) {
                bytes.write(p.part, 0, p.part.length);
                if (p.isEndOfValue || p.children.size() == 0) {
                    break;
                }
                p = p.children.get(0);
            }
            return bytes.toByteArray();
        }

        public static Node splitNodeTree(Node splitNode) {
            if (splitNode == null) {
                return null;
            }
            Node current = splitNode;
            Node p = current.parent;
            while (p != null) {
                int index = p.children.indexOf(current);
                assert index != -1;
                Node newParent = p.duplicateNode();
                for (int i = p.children.size()-1; i >= index; i--) {
                    Node child = p.removeChild(i);
                    newParent.addChild(0, child);
                }
                current = newParent;
                p = p.parent;
            }
            return current;
        }

        public static void mergeSingleByteNode(Node root, int leftOrRight) {
            Node current = root;
            Node child;
            while (!current.children.isEmpty()) {
                child = leftOrRight == 0 ? current.children.get(0) : current.children.get(current.children.size()-1);
                if (current.children.size() > 1 || current.isEndOfValue) {
                    current = child;
                    continue;
                }
                byte[] newValue = new byte[current.part.length+child.part.length];
                System.arraycopy(current.part, 0, newValue, 0, current.part.length);
                System.arraycopy(child.part, 0, newValue, current.part.length, child.part.length);
                current.reset(newValue, child.isEndOfValue, child.children);
                current.id = child.id;
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(part.length);
            out.write(part);
            out.writeInt(id);
            out.writeBoolean(isEndOfValue);
            out.writeInt(children.size());
            for (int i = 0; i < children.size(); i++) {
                children.get(i).write(out);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.part = new byte[in.readByte()];
            in.readFully(this.part);
            this.id = in.readInt();
            this.isEndOfValue = in.readBoolean();
            int childCount = in.readInt();
            for (int i = 0; i < childCount; i++) {
                Node child = new Node();
                child.readFields(in);
                this.addChild(child);
            }
        }

        @Override
        public String toString() {
            return String.format("DictNode[root=%s, nodes=%d, firstValue=%s]", Bytes.toStringBinary(part), childrenCount, Bytes.toStringBinary(firstValue()));
        }
    }

    public interface Visitor {
        void visit(Node n, int level);
    }

    // ============================================================================

    private TreeMap<DictSliceKey, Node> dictSliceMap;

    private BytesConverter<T> bytesConverter;
    private int maxId;
    private static int MAX_ENTRY_IN_SLICE = 10_000_000;
    private static final double MAX_ENTRY_OVERHEAD_FACTOR = 1.0;
    private int valueCount;
    private String uuid;

    public AppendTrieDictionaryBuilder(BytesConverter<T> bytesConverter, String uuid) {
        this.bytesConverter = bytesConverter;
        this.uuid = uuid;
        MAX_ENTRY_IN_SLICE = KylinConfig.getInstanceFromEnv().getAppendDictBuilderEntrySize();
        int cacheSize = KylinConfig.getInstanceFromEnv().getAppendDictBuilderCacheSize();
        String baseDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/CachedTreeMapCache/" + this.getClass().getSimpleName() + "/" + uuid;
        dictSliceMap = CachedTreeMap.CachedTreeMapBuilder.newBuilder().maxSize(cacheSize)
                .baseDir(baseDir).keyClazz(DictSliceKey.class).valueClazz(Node.class).build();
    }

    public void addDictSlice(Node root, int maxId) {
        dictSliceMap.put(DictSliceKey.wrap(root.firstValue()), root);
        this.maxId = Math.max(this.maxId, maxId);
    }

    public void addValue(T value) {
        addValue(bytesConverter.convertToBytes(value));
    }

    public void addValue(byte[] value) {
        if (++valueCount % 1_000_000 == 0) {
            logger.debug("add value count " + valueCount);
        }
        if (dictSliceMap.isEmpty()) {
            Node root = new Node(new byte[0], false);
            dictSliceMap.put(DictSliceKey.wrap(new byte[0]), root);
        }
        DictSliceKey sliceKey = dictSliceMap.floorKey(DictSliceKey.wrap(value));
        if (sliceKey == null) {
            sliceKey = dictSliceMap.firstKey();
        }
        Node root = dictSliceMap.get(sliceKey);
        addValueR(root, value, 0);
        if (root.childrenCount > MAX_ENTRY_IN_SLICE * MAX_ENTRY_OVERHEAD_FACTOR) {
            dictSliceMap.remove(sliceKey);
            Node newRoot = splitNodeTree(root);
            Node.mergeSingleByteNode(root, 1);
            Node.mergeSingleByteNode(newRoot, 0);
            dictSliceMap.put(DictSliceKey.wrap(root.firstValue()), root);
            dictSliceMap.put(DictSliceKey.wrap(newRoot.firstValue()), newRoot);
        }
    }

    public Node splitNodeTree(Node root) {
        Node parent = root;
        Node splitNode;
        int childCountToSplit = (int)(MAX_ENTRY_IN_SLICE * MAX_ENTRY_OVERHEAD_FACTOR / 2);
        while (true) {
            List<Node> children = parent.children;
            if (children.size() == 0){
                splitNode = parent;
                break;
            } else if (children.size() == 1) {
                parent = children.get(0);
                continue;
            } else {
                for (int i = children.size()-1; i >= 0; i--) {
                    parent = children.get(i);
                    if (childCountToSplit > children.get(i).childrenCount) {
                        childCountToSplit -= children.get(i).childrenCount;
                    } else {
                        childCountToSplit --;
                        break;
                    }
                }
            }
        }
        return Node.splitNodeTree(splitNode);
    }

    private void addValueR(Node node, byte[] value, int start) {
        assert value.length - start <= 255 : "value bytes overflow than 255";
        // match the value part of current node
        int i = 0, j = start;
        int n = node.part.length, nn = value.length;
        int comp = 0;
        for (; i < n && j < nn; i++, j++) {
            comp = BytesUtil.compareByteUnsigned(node.part[i], value[j]);
            if (comp != 0)
                break;
        }

        // if value fully matched within the current node
        if (j == nn) {
            // if equals to current node, just mark end of value
            if (i == n) {
                node.isEndOfValue = true;
            }
            // otherwise, split the current node into two
            else {
                Node c = new Node(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);
                c.id = node.id;
                node.reset(BytesUtil.subarray(node.part, 0, i), true);
                node.addChild(c);
            }
            return;
        }

        // if partially matched the current, split the current node, add the new
        // value, make a 3-way
        if (i < n) {
            Node c1 = new Node(BytesUtil.subarray(node.part, i, n), node.isEndOfValue, node.children);
            c1.id = node.id;
            Node c2 = new Node(BytesUtil.subarray(value, j, nn), true);
            node.reset(BytesUtil.subarray(node.part, 0, i), false);
            if (comp < 0) {
                node.addChild(c1);
                node.addChild(c2);
            } else {
                node.addChild(c2);
                node.addChild(c1);
            }
            return;
        }

        // out matched the current, binary search the next byte for a child node
        // to continue
        byte lookfor = value[j];
        int lo = 0;
        int hi = node.children.size() - 1;
        int mid = 0;
        boolean found = false;
        comp = 0;
        while (!found && lo <= hi) {
            mid = lo + (hi - lo) / 2;
            Node c = node.children.get(mid);
            comp = BytesUtil.compareByteUnsigned(lookfor, c.part[0]);
            if (comp < 0)
                hi = mid - 1;
            else if (comp > 0)
                lo = mid + 1;
            else
                found = true;
        }
        // found a child node matching the first byte, continue in that child
        if (found) {
            addValueR(node.children.get(mid), value, j);
        }
        // otherwise, make the value a new child
        else {
            Node c = new Node(BytesUtil.subarray(value, j, nn), true);
            node.addChild(comp <= 0 ? mid : mid + 1, c);
        }
    }

    private void traverseR(Node node, Visitor visitor, int level) {
        visitor.visit(node, level);
        for (Node c : node.children)
            traverseR(c, visitor, level + 1);
    }

    private void traversePostOrderR(Node node, Visitor visitor, int level) {
        for (Node c : node.children)
            traversePostOrderR(c, visitor, level + 1);
        visitor.visit(node, level);
    }

    public static class Stats {
        public int nValues; // number of values in total
        public int nValueBytesPlain; // number of bytes for all values
                                     // uncompressed
        public int nValueBytesCompressed; // number of values bytes in Trie
                                          // (compressed)
        public int maxValueLength; // size of longest value in bytes

        // the trie is multi-byte-per-node
        public int mbpn_nNodes; // number of nodes in trie
        public int mbpn_trieDepth; // depth of trie
        public int mbpn_maxFanOut; // the maximum no. children
        public int mbpn_nChildLookups; // number of child lookups during lookup
                                       // every value once
        public int mbpn_nTotalFanOut; // the sum of fan outs during lookup every
                                      // value once
        public int mbpn_sizeValueTotal; // the sum of value space in all nodes
        public int mbpn_sizeNoValueBytes; // size of field noValueBytes
        public int mbpn_sizeChildOffset; // size of field childOffset, points to
                                         // first child in flattened array
        public int mbpn_sizeId;          // size of id value, always be 4
        public int mbpn_footprint; // MBPN footprint in bytes

        public void print() {
            PrintStream out = System.out;
            out.println("============================================================================");
            out.println("No. values:             " + nValues);
            out.println("No. bytes raw:          " + nValueBytesPlain);
            out.println("No. bytes in trie:      " + nValueBytesCompressed);
            out.println("Longest value length:   " + maxValueLength);

            // flatten trie footprint calculation, case of Multi-Byte-Per-Node
            out.println("----------------------------------------------------------------------------");
            out.println("MBPN max fan out:       " + mbpn_maxFanOut);
            out.println("MBPN no. child lookups: " + mbpn_nChildLookups);
            out.println("MBPN total fan out:     " + mbpn_nTotalFanOut);
            out.println("MBPN average fan out:   " + (double) mbpn_nTotalFanOut / mbpn_nChildLookups);
            out.println("MBPN values size total: " + mbpn_sizeValueTotal);
            out.println("MBPN node size:         " + (mbpn_sizeNoValueBytes + mbpn_sizeChildOffset + mbpn_sizeId) + " = " + mbpn_sizeNoValueBytes + " + " + " + " + mbpn_sizeChildOffset + " + " + mbpn_sizeId);
            out.println("MBPN no. nodes:         " + mbpn_nNodes);
            out.println("MBPN trie depth:        " + mbpn_trieDepth);
            out.println("MBPN footprint:         " + mbpn_footprint + " in bytes");
        }
    }

    /** out print some statistics of the trie and the dictionary built from it */
    public Stats stats(Node root) {
        // calculate nEndValueBeneath
        traversePostOrderR(root, new Visitor() {
            @Override
            public void visit(Node n, int level) {
                n.nValuesBeneath = n.isEndOfValue ? 1 : 0;
                for (Node c : n.children)
                    n.nValuesBeneath += c.nValuesBeneath;
            }
        }, 0);

        // run stats
        final Stats s = new Stats();
        final ArrayList<Integer> lenAtLvl = new ArrayList<Integer>();
        traverseR(root, new Visitor() {
            @Override
            public void visit(Node n, int level) {
                if (n.isEndOfValue)
                    s.nValues++;
                s.nValueBytesPlain += n.part.length * n.nValuesBeneath;
                s.nValueBytesCompressed += n.part.length;
                s.mbpn_nNodes++;
                if (s.mbpn_trieDepth < level + 1)
                    s.mbpn_trieDepth = level + 1;
                if (n.children.size() > 0) {
                    if (s.mbpn_maxFanOut < n.children.size())
                        s.mbpn_maxFanOut = n.children.size();
                    int childLookups = n.nValuesBeneath - (n.isEndOfValue ? 1 : 0);
                    s.mbpn_nChildLookups += childLookups;
                    s.mbpn_nTotalFanOut += childLookups * n.children.size();
                }

                if (level < lenAtLvl.size())
                    lenAtLvl.set(level, n.part.length);
                else
                    lenAtLvl.add(n.part.length);
                int lenSoFar = 0;
                for (int i = 0; i <= level; i++)
                    lenSoFar += lenAtLvl.get(i);
                if (lenSoFar > s.maxValueLength)
                    s.maxValueLength = lenSoFar;
            }
        }, 0);

        // flatten trie footprint calculation, case of Multi-Byte-Per-Node
        s.mbpn_sizeId = 4;
        s.mbpn_sizeValueTotal = s.nValueBytesCompressed + s.nValues * s.mbpn_sizeId;
        s.mbpn_sizeNoValueBytes = 1;
        s.mbpn_sizeChildOffset = 4;
        s.mbpn_footprint = s.mbpn_sizeValueTotal + s.mbpn_nNodes * (s.mbpn_sizeNoValueBytes + s.mbpn_sizeChildOffset);
        while (true) { // minimize the offset size to match the footprint
            int t = s.mbpn_sizeValueTotal + s.mbpn_nNodes * (s.mbpn_sizeNoValueBytes + s.mbpn_sizeChildOffset - 1);
            // *4 because 2 MSB of offset is used for isEndOfValue & isEndChild flag
            if (BytesUtil.sizeForValue(t * 4) <= s.mbpn_sizeChildOffset - 1) {
                s.mbpn_sizeChildOffset--;
                s.mbpn_footprint = t;
            } else
                break;
        }

        return s;
    }

    /** out print trie for debug */
    public void print(Node root) {
        print(root, System.out);
    }

    public void print(Node root, final PrintStream out) {
        traverseR(root, new Visitor() {
            @Override
            public void visit(Node n, int level) {
                try {
                    for (int i = 0; i < level; i++)
                        out.print("  ");
                    out.print(new String(n.part, "UTF-8"));
                    out.print(" - ");
                    if (n.nValuesBeneath > 0)
                        out.print(n.nValuesBeneath);
                    if (n.isEndOfValue)
                        out.print("* [" + n.id + "]");
                    out.print("\n");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        }, 0);
    }

    public AppendTrieDictionary<T> build(int baseId) {
        AppendTrieDictionary<T> r = new AppendTrieDictionary<>(bytesConverter, uuid);
        Iterator<Node> dictSliceIterator = dictSliceMap.values().iterator();
        while (dictSliceIterator.hasNext()) {
            Node dictSliceNode = dictSliceIterator.next();
            r.addDictSlice(buildTrieBytes(dictSliceNode));
            dictSliceIterator.remove();
        }
        dictSliceMap.clear();
        return r;
    }

    protected byte[] buildTrieBytes(Node root) {
        Stats stats = stats(root);
        int sizeChildOffset = stats.mbpn_sizeChildOffset;
        int sizeId = stats.mbpn_sizeId;

        // write head
        byte[] head;
        try {
            ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
            DataOutputStream headOut = new DataOutputStream(byteBuf);
            headOut.write(AppendTrieDictionary.HEAD_MAGIC);
            headOut.writeShort(0); // head size, will back fill
            headOut.writeInt(stats.mbpn_footprint); // body size
            headOut.writeInt(stats.nValues);
            headOut.write(sizeChildOffset);
            headOut.write(sizeId);
            headOut.writeShort(stats.maxValueLength);
            headOut.writeUTF(bytesConverter == null ? "" : bytesConverter.getClass().getName());
            headOut.close();
            head = byteBuf.toByteArray();
            BytesUtil.writeUnsigned(head.length, head, AppendTrieDictionary.HEAD_SIZE_I, 2);
        } catch (IOException e) {
            throw new RuntimeException(e); // shall not happen, as we are
                                           // writing in memory
        }

        byte[] trieBytes = new byte[stats.mbpn_footprint + head.length];
        System.arraycopy(head, 0, trieBytes, 0, head.length);

        LinkedList<Node> open = new LinkedList<Node>();
        IdentityHashMap<Node, Integer> offsetMap = new IdentityHashMap<Node, Integer>();

        // write body
        int o = head.length;
        offsetMap.put(root, o);
        o = build_writeNode(root, o, true, sizeChildOffset, sizeId, trieBytes);
        if (root.children.isEmpty() == false)
            open.addLast(root);

        while (open.isEmpty() == false) {
            Node parent = open.removeFirst();
            build_overwriteChildOffset(offsetMap.get(parent), o - head.length, sizeChildOffset, trieBytes);
            for (int i = 0; i < parent.children.size(); i++) {
                Node c = parent.children.get(i);
                boolean isLastChild = (i == parent.children.size() - 1);
                offsetMap.put(c, o);
                o = build_writeNode(c, o, isLastChild, sizeChildOffset, sizeId, trieBytes);
                if (c.children.isEmpty() == false)
                    open.addLast(c);
            }
        }

        if (o != trieBytes.length)
            throw new RuntimeException();
        return trieBytes;
    }

    private void build_overwriteChildOffset(int parentOffset, int childOffset, int sizeChildOffset, byte[] trieBytes) {
        int flags = (int) trieBytes[parentOffset] & (TrieDictionary.BIT_IS_LAST_CHILD | TrieDictionary.BIT_IS_END_OF_VALUE);
        BytesUtil.writeUnsigned(childOffset, trieBytes, parentOffset, sizeChildOffset);
        trieBytes[parentOffset] |= flags;
    }

    private int build_writeNode(Node n, int offset, boolean isLastChild, int sizeChildOffset, int sizeId, byte[] trieBytes) {
        int o = offset;

        // childOffset
        if (isLastChild)
            trieBytes[o] |= TrieDictionary.BIT_IS_LAST_CHILD;
        if (n.isEndOfValue)
            trieBytes[o] |= TrieDictionary.BIT_IS_END_OF_VALUE;
        o += sizeChildOffset;

        // nValueBytes
        if (n.part.length > 255)
            throw new RuntimeException();
        BytesUtil.writeUnsigned(n.part.length, trieBytes, o, 1);
        o++;

        // valueBytes
        System.arraycopy(n.part, 0, trieBytes, o, n.part.length);
        o += n.part.length;

        if (n.isEndOfValue) {
            if (n.id < 0) {
                n.id = ++maxId;
                // Abort if id overflow Integer.MAX_VALUE
                if (n.id < 0) {
                    throw new IllegalArgumentException("AppendTrieDictionary Id overflow Integer.MAX_VALUE");
                }
            }
            BytesUtil.writeUnsigned(n.id, trieBytes, o, sizeId);
            o += sizeId;
        }

        return o;
    }

}
