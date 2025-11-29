const std = @import("std");
const mem = std.mem;
const testing = std.testing;

pub const Key = u64;
pub const Value = u64;

const T: usize = 2;
pub const MAX_KEYS: usize = 2 * T - 1;
pub const MAX_CHILDREN: usize = 2 * T;

pub const KV = struct {
    k: Key,
    v: Value,
};

pub const Node = struct {
    is_leaf: bool,
    n: usize,
    keys: [MAX_KEYS]Key,
    values: [MAX_KEYS]Value,
    children: [MAX_CHILDREN]?*Node,

    const Self = @This();

    pub fn init(is_leaf: bool) Self {
        var node: Node = .{
            .is_leaf = is_leaf,
            .n = 0,
            .keys = undefined,
            .values = undefined,
            .children = undefined,
        };

        for (&node.children) |*c| {
            c.* = null;
        }
        return node;
    }
};

pub const BTree = struct {
    allocator: mem.Allocator,
    root: ?*Node,

    const Self = @This();

    pub fn init(allocator: mem.Allocator) Self {
        return BTree{
            .allocator = allocator,
            .root = null,
        };
    }

    pub fn deinit(self: *Self) void {
        self.clear();
    }

    pub fn clear(self: *Self) void {
        if (self.root) |r| {
            self.freeRecursive(r);
        }
        self.root = null;
    }

    fn freeRecursive(self: *Self, node: *Node) void {
        if (!node.is_leaf) {
            var i: usize = 0;
            while (i <= node.n) : (i += 1) {
                if (node.children[i]) |child| {
                    self.freeRecursive(child);
                }
            }
        }
        self.allocator.destroy(node);
    }

    pub fn contains(self: *Self, key: Key) bool {
        return self.search(key) != null;
    }

    pub fn search(self: *Self, key: Key) ?Value {
        return if (self.root) |r| searchNode(r, key) else null;
    }

    pub fn insert(self: *Self, key: Key, value: Value) !void {
        if (self.root == null) {
            var new_node = try self.allocator.create(Node);
            new_node.* = .init(true);
            new_node.keys[0] = key;
            new_node.values[0] = value;
            new_node.n = 1;
            self.root = new_node;
            return;
        }

        const root = self.root.?;
        if (root.n == MAX_KEYS) {
            var new_node = try self.allocator.create(Node);
            new_node.* = .init(false);
            new_node.children[0] = root;

            try self.splitChild(new_node, 0);
            var i: usize = 0;
            if (key > new_node.keys[0]) {
                i = 1;
            }

            try self.insertNonFull(new_node.children[i].?, key, value);
            self.root = new_node;
        } else {
            try self.insertNonFull(root, key, value);
        }
    }

    fn searchNode(node: *Node, key: Key) ?Value {
        var i: usize = 0;
        while (i < node.n and key > node.keys[i]) : (i += 1) {}

        if (i < node.n and key == node.keys[i]) {
            return node.values[i];
        }
        if (node.is_leaf) {
            return null;
        }
        if (node.children[i]) |child| {
            return searchNode(child, key);
        }
        return null;
    }

    fn insertNonFull(self: *Self, node: *Node, key: Key, value: Value) !void {
        var i = @as(isize, @intCast(node.n)) - 1;
        if (node.is_leaf) {
            while (i >= 0) : (i -= 1) {
                const idx: usize = @intCast(i);
                if (key == node.keys[idx]) {
                    node.values[idx] = value;
                    return;
                }
                if (key > node.keys[idx]) {
                    break;
                }
                node.keys[idx + 1] = node.keys[idx];
                node.values[idx + 1] = node.values[idx];
            }

            const insert_idx: usize = @intCast(i + 1);
            node.keys[insert_idx] = key;
            node.values[insert_idx] = value;
            node.n += 1;
        } else {
            while (i >= 0) : (i -= 1) {
                const idx: usize = @intCast(i);
                if (key == node.keys[idx]) {
                    node.values[idx] = value;
                    return;
                }
                if (key > node.keys[idx]) {
                    break;
                }
            }

            var child_idx: usize = @intCast(i + 1);
            var child = node.children[child_idx].?;
            if (child.n == MAX_KEYS) {
                try self.splitChild(node, child_idx);
                if (key > node.keys[child_idx]) {
                    child_idx += 1;
                }
                child = node.children[child_idx].?;
            }

            try self.insertNonFull(child, key, value);
        }
    }

    fn splitChild(self: *Self, parent: *Node, i: usize) !void {
        const y = parent.children[i].?;
        var z = try self.allocator.create(Node);
        z.* = .init(y.is_leaf);
        z.n = T - 1;

        var j: usize = 0;
        while (j < T - 1) : (j += 1) {
            z.keys[j] = y.keys[j + T];
            z.values[j] = y.values[j + T];
        }
        if (!y.is_leaf) {
            j = 0;
            while (j < T) : (j += 1) {
                z.children[j] = y.children[T + j];
            }
        }

        y.n = T - 1;
        var k: isize = @intCast(parent.n);
        while (k >= @as(isize, @intCast(i + 1))) : (k -= 1) {
            const idx: usize = @intCast(k);
            parent.children[idx + 1] = parent.children[idx];
        }

        parent.children[i + 1] = z;
        k = @as(isize, @intCast(parent.n)) - 1;
        while (k >= @as(isize, @intCast(i + 1))) : (k -= 1) {
            const idx: usize = @intCast(k);
            parent.keys[idx + 1] = parent.keys[idx];
            parent.values[idx + 1] = parent.values[idx];
        }

        parent.keys[i] = y.keys[T - 1];
        parent.values[i] = y.values[T - 1];
        parent.n += 1;
    }

    pub fn min(self: *Self) ?KV {
        if (self.root == null) {
            return null;
        }

        var node = self.root.?;
        while (!node.is_leaf) {
            if (node.children[0]) |c| {
                node = c;
            } else break;
        }

        if (node.n == 0) return null;
        return KV{
            .k = node.keys[0],
            .v = node.values[0],
        };
    }

    pub fn max(self: *Self) ?KV {
        if (self.root == null) {
            return null;
        }

        var node = self.root.?;
        while (!node.is_leaf) {
            const last_idx = node.n;
            if (node.children[last_idx]) |c| {
                node = c;
            } else break;
        }

        if (node.n == 0) return null;
        return KV{
            .k = node.keys[node.n - 1],
            .v = node.values[node.n - 1],
        };
    }

    pub fn height(self: *Self) usize {
        if (self.root == null) {
            return 0;
        }
        return heightNode(self.root.?);
    }

    fn heightNode(node: *Node) usize {
        if (node.is_leaf) {
            return 1;
        }
        if (node.children[0]) |c| {
            return 1 + heightNode(c);
        }
        return 1;
    }

    pub fn forEach(self: *Self, visitor: anytype) void {
        if (self.root) |r| {
            forEachNode(r, visitor);
        }
    }

    fn forEachNode(node: *Node, visitor: anytype) void {
        if (node.is_leaf) {
            var i: usize = 0;
            while (i < node.n) : (i += 1) {
                callVisitor(visitor, node.keys[i], node.values[i]);
            }
        } else {
            var i: usize = 0;
            while (i < node.n) : (i += 1) {
                if (node.children[i]) |c| {
                    forEachNode(c, visitor);
                }
                callVisitor(visitor, node.keys[i], node.values[i]);
            }
            if (node.children[node.n]) |last_child| {
                forEachNode(last_child, visitor);
            }
        }
    }

    fn callVisitor(visitor: anytype, key: Key, value: Value) void {
        const U = @TypeOf(visitor);
        const ti = @typeInfo(U);
        if (ti == .@"fn") {
            visitor(key, value);
            return;
        }
        if (ti == .pointer) {
            const child_info = @typeInfo(ti.pointer.child);
            if (child_info == .@"fn") {
                visitor(key, value);
                return;
            }
            if (child_info == .@"struct") {
                if (@hasDecl(ti.pointer.child, "call")) {
                    visitor.call(key, value);
                    return;
                }
            }
        }
        @compileError("BTree.forEach/range visitor must be fn(Key,Value) OR *struct with .call(key,value)");
    }

    pub fn range(self: *Self, start: Key, end: Key, visitor: anytype) void {
        if (self.root) |r| {
            rangeNode(r, start, end, visitor);
        }
    }

    fn rangeNode(node: *Node, start: Key, end: Key, visitor: anytype) void {
        if (node.is_leaf) {
            var i: usize = 0;
            while (i < node.n) : (i += 1) {
                const k = node.keys[i];
                if (k >= start and k <= end) {
                    callVisitor(visitor, k, node.values[i]);
                }
            }
        } else {
            var i: usize = 0;
            while (i < node.n) : (i += 1) {
                if (node.children[i]) |child| {
                    rangeNode(child, start, end, visitor);
                }

                const k = node.keys[i];
                if (k >= start and k <= end) {
                    callVisitor(visitor, k, node.values[i]);
                }
            }
            if (node.children[node.n]) |last_child| {
                rangeNode(last_child, start, end, visitor);
            }
        }
    }

    pub fn delete(self: *Self, key: Key) void {
        if (self.root == null) return;
        self.deleteNode(self.root.?, key);

        const root = self.root.?;
        if (root.n == 0) {
            if (root.is_leaf) {
                self.allocator.destroy(root);
                self.root = null;
            } else {
                const new_root = root.children[0].?;
                self.allocator.destroy(root);
                self.root = new_root;
            }
        }
    }

    fn deleteNode(self: *Self, node: *Node, key: Key) void {
        var idx: usize = 0;
        while (idx < node.n and key > node.keys[idx]) : (idx += 1) {}
        if (idx < node.n and node.keys[idx] == key) {
            if (node.is_leaf) {
                deleteFromLeaf(node, idx);
            } else {
                self.deleteFromNonLeaf(node, idx);
            }
        } else {
            if (node.is_leaf) {
                return;
            }

            var child_idx = idx;
            if (node.children[child_idx]) |child| {
                if (child.n < T) {
                    self.fill(node, child_idx);
                    if (child_idx > node.n) {
                        child_idx -= 1;
                    }
                }
            } else {
                return;
            }

            if (node.children[child_idx]) |child2| {
                self.deleteNode(child2, key);
            }
        }
    }

    fn deleteFromLeaf(node: *Node, idx: usize) void {
        var i = idx;
        while (i + 1 < node.n) : (i += 1) {
            node.keys[i] = node.keys[i + 1];
            node.values[i] = node.values[i + 1];
        }
        node.n -= 1;
    }

    fn deleteFromNonLeaf(self: *Self, node: *Node, idx: usize) void {
        const k = node.keys[idx];
        const left_child = node.children[idx].?;
        const right_child = node.children[idx + 1].?;

        if (left_child.n >= T) {
            const pred = getPred(node, idx);
            node.keys[idx] = pred.k;
            node.values[idx] = pred.v;
            self.deleteNode(left_child, pred.k);
        } else if (right_child.n >= T) {
            const succ = getSucc(node, idx);
            node.keys[idx] = succ.k;
            node.values[idx] = succ.v;
            self.deleteNode(right_child, succ.k);
        } else {
            self.merge(node, idx);
            const merged_child = node.children[idx].?;
            self.deleteNode(merged_child, k);
        }
    }

    fn getPred(node: *Node, idx: usize) KV {
        var cur = node.children[idx].?;
        while (!cur.is_leaf) {
            cur = cur.children[cur.n].?;
        }

        return KV{
            .k = cur.keys[cur.n - 1],
            .v = cur.values[cur.n - 1],
        };
    }

    fn getSucc(node: *Node, idx: usize) KV {
        var cur = node.children[idx + 1].?;
        while (!cur.is_leaf) {
            cur = cur.children[0].?;
        }

        return KV{
            .k = cur.keys[0],
            .v = cur.values[0],
        };
    }

    fn fill(self: *Self, parent: *Node, idx: usize) void {
        if (idx > 0) {
            if (parent.children[idx - 1]) |left| {
                if (left.n >= T) {
                    borrowFromPrev(parent, idx);
                    return;
                }
            }
        }

        if (idx < parent.n) {
            if (parent.children[idx + 1]) |right| {
                if (right.n >= T) {
                    borrowFromNext(parent, idx);
                    return;
                }
            }
        }

        if (idx < parent.n) {
            self.merge(parent, idx);
        } else {
            self.merge(parent, idx - 1);
        }
    }

    fn borrowFromPrev(parent: *Node, idx: usize) void {
        const child = parent.children[idx].?;
        const sibling = parent.children[idx - 1].?;

        var i: usize = child.n;
        while (i > 0) : (i -= 1) {
            child.keys[i] = child.keys[i - 1];
            child.values[i] = child.values[i - 1];
        }
        if (!child.is_leaf) {
            i = child.n + 1;
            while (i > 0) : (i -= 1) {
                child.children[i] = child.children[i - 1];
            }
        }

        child.keys[0] = parent.keys[idx - 1];
        child.values[0] = parent.values[idx - 1];
        if (!child.is_leaf) {
            child.children[0] = sibling.children[sibling.n].?;
        }

        parent.keys[idx - 1] = sibling.keys[sibling.n - 1];
        parent.values[idx - 1] = sibling.values[sibling.n - 1];

        sibling.n -= 1;
        child.n += 1;
    }

    fn borrowFromNext(parent: *Node, idx: usize) void {
        const child = parent.children[idx].?;
        const sibling = parent.children[idx + 1].?;

        child.keys[child.n] = parent.keys[idx];
        child.values[child.n] = parent.values[idx];
        if (!child.is_leaf) {
            child.children[child.n + 1] = sibling.children[0];
        }

        parent.keys[idx] = sibling.keys[0];
        parent.values[idx] = sibling.values[0];

        var i: usize = 0;
        while (i + 1 < sibling.n) : (i += 1) {
            sibling.keys[i] = sibling.keys[i + 1];
            sibling.values[i] = sibling.values[i + 1];
        }
        if (!sibling.is_leaf) {
            i = 0;
            while (i + 1 <= sibling.n) : (i += 1) {
                sibling.children[i] = sibling.children[i + 1];
            }
        }

        sibling.n -= 1;
        child.n += 1;
    }

    fn merge(self: *Self, parent: *Node, idx: usize) void {
        const left = parent.children[idx].?;
        const right = parent.children[idx + 1].?;

        left.keys[T - 1] = parent.keys[idx];
        left.values[T - 1] = parent.values[idx];

        var j: usize = 0;
        while (j < right.n) : (j += 1) {
            left.keys[T + j] = right.keys[j];
            left.values[T + j] = right.values[j];
        }

        if (!left.is_leaf) {
            j = 0;
            while (j <= right.n) : (j += 1) {
                left.children[T + j] = right.children[j];
            }
        }

        left.n = left.n + right.n + 1;

        var k: usize = idx;
        while (k + 1 < parent.n) : (k += 1) {
            parent.keys[k] = parent.keys[k + 1];
            parent.values[k] = parent.values[k + 1];
        }

        k = idx + 1;
        while (k + 1 <= parent.n) : (k += 1) {
            parent.children[k] = parent.children[k + 1];
        }

        parent.n -= 1;
        self.allocator.destroy(right);
    }

    pub fn debugPrint(self: *Self) void {
        if (self.root) |r| {
            dbgPrintNode(r, 0);
        } else {
            std.debug.print("(Empty tree)\n", .{});
        }
    }

    fn dbgPrintNode(node: *Node, depth: usize) void {
        std.debug.print("level={}: keys=[", .{depth});
        for (node.keys[0..node.n], 0..) |k, i| {
            std.debug.print("{}", .{k});
            if (i + 1 < node.n) std.debug.print(", ", .{});
        }

        std.debug.print("]\n", .{});
        if (!node.is_leaf) {
            for (node.children[0 .. node.n + 1]) |child_opt| {
                if (child_opt) |child| {
                    dbgPrintNode(child, depth + 1);
                }
            }
        }
    }
};

test "insert and search keys" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    try tree.insert(10, 100);
    try tree.insert(20, 200);
    try tree.insert(5, 50);
    try testing.expectEqual(@as(?Value, 100), tree.search(10));
    try testing.expectEqual(@as(?Value, 200), tree.search(20));
    try testing.expectEqual(@as(?Value, 50), tree.search(5));
}

test "search missing keys" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();
    try tree.insert(10, 100);
    try testing.expectEqual(@as(?Value, null), tree.search(100));
}

test "multiple splits and balanced tree" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();
    var i: usize = 1;
    while (i <= 30) : (i += 1) {
        try tree.insert(@intCast(i), @intCast(i * 10));
    }

    i = 1;
    while (i <= 30) : (i += 1) {
        try testing.expectEqual(@as(?Value, @intCast(i * 10)), tree.search(@intCast(i)));
    }
    try testing.expectEqual(@as(?Value, null), tree.search(2343));
}

test "overwrite behaviour (duplication)" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    try tree.insert(10, 100);
    try tree.insert(10, 999);
    const v = tree.search(10);
    try testing.expect(v == 100 or v == 999);
}

test "upsert / overwrite behaviour" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    try tree.insert(10, 100);
    try tree.insert(10, 999);
    try testing.expectEqual(@as(?Value, 999), tree.search(10));
}

test "clear and reuse tree" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    try tree.insert(1, 10);
    try tree.insert(2, 20);
    try testing.expect(tree.height() > 0);

    tree.clear();
    try testing.expectEqual(@as(?Value, null), tree.search(1));
    try testing.expectEqual(@as(usize, 0), tree.height());

    try tree.insert(42, 420);
    try testing.expectEqual(@as(?Value, 420), tree.search(42));
}

// test "debugPrint on small tree" {
//     var gpa = std.heap.DebugAllocator(.{}).init;
//     defer _ = gpa.deinit();
//     const alloc = gpa.allocator();

//     var tree = BTree.init(alloc);
//     defer tree.deinit();

//     try tree.insert(10, 100);
//     try tree.insert(20, 200);
//     try tree.insert(5, 50);

//     std.debug.print("--- DebugPrint small ---\n", .{});
//     tree.debugPrint();
// }

// test "debugPrint on deep tree (multiple splits)" {
//     var gpa = std.heap.DebugAllocator(.{}).init;
//     defer _ = gpa.deinit();
//     const alloc = gpa.allocator();

//     var tree = BTree.init(alloc);
//     defer tree.deinit();

//     var i: usize = 1;
//     while (i <= 50) : (i += 1) {
//         try tree.insert(@intCast(i), @intCast(i * 10));
//     }

//     try testing.expect(tree.height() >= 3);
//     std.debug.print("--- DebugPrint deep tree ---\n", .{});
//     tree.debugPrint();
// }

test "min max" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    try testing.expect(tree.min() == null);
    try testing.expect(tree.max() == null);

    try tree.insert(10, 100);
    try tree.insert(5, 50);
    try tree.insert(20, 200);
    try tree.insert(15, 150);

    const mn = tree.min().?;
    const mx = tree.max().?;
    try testing.expectEqual(@as(Key, 5), mn.k);
    try testing.expectEqual(@as(Value, 50), mn.v);
    try testing.expectEqual(@as(Key, 20), mx.k);
    try testing.expectEqual(@as(Value, 200), mx.v);
}

test "forEach visits keys in sorted order" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree: BTree = .init(alloc);
    defer tree.deinit();

    const nums = [_]u64{ 10, 3, 7, 1, 15, 12, 8 };
    for (nums) |n| {
        try tree.insert(n, n * 10);
    }

    var collected_keys: [32]Key = undefined;
    var count: usize = 0;

    const Collector = struct {
        keys: []Key,
        count: *usize,

        const Self = @This();

        pub fn call(self: *Self, key: Key, value: Value) void {
            _ = value;
            self.keys[self.count.*] = key;
            self.count.* += 1;
        }
    };

    var col = Collector{ .keys = collected_keys[0..], .count = &count };
    tree.forEach(&col);
    try testing.expectEqual(@as(usize, nums.len), count);

    var i: usize = 1;
    while (i < count) : (i += 1) {
        try testing.expect(collected_keys[i - 1] <= collected_keys[i]);
    }
}

test "range query filters keys" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    var i: usize = 1;
    while (i <= 20) : (i += 1) {
        try tree.insert(@intCast(i), @intCast(i * 10));
    }

    var collected: [32]Key = undefined;
    var count: usize = 0;
    const RCollector = struct {
        keys: []Key,
        count: *usize,

        const Self = @This();

        pub fn call(self: *Self, key: Key, value: Value) void {
            _ = value;
            self.keys[self.count.*] = key;
            self.count.* += 1;
        }
    };

    var rcol = RCollector{ .keys = collected[0..], .count = &count };
    tree.range(5, 10, &rcol);

    try testing.expectEqual(@as(usize, 6), count);
    try testing.expectEqual(@as(Key, 5), collected[0]);
    try testing.expectEqual(@as(Key, 10), collected[count - 1]);

    var idx: usize = 0;
    var expected: Key = 5;
    while (idx < count) : (idx += 1) {
        try testing.expectEqual(expected, collected[idx]);
        expected += 1;
    }
}

test "delete from leaf" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    try tree.insert(10, 100);
    try tree.insert(20, 200);
    try tree.insert(5, 50);

    tree.delete(20);
    try testing.expectEqual(@as(?Value, null), tree.search(20));
    try testing.expectEqual(@as(?Value, 100), tree.search(10));
    try testing.expectEqual(@as(?Value, 50), tree.search(5));
}

test "delete internal node keys with rebalancing" {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    var i: usize = 1;
    while (i <= 20) : (i += 1) {
        try tree.insert(@intCast(i), @intCast(i * 10));
    }

    tree.delete(10);
    tree.delete(5);
    tree.delete(15);

    try testing.expectEqual(@as(?Value, null), tree.search(10));
    try testing.expectEqual(@as(?Value, null), tree.search(5));
    try testing.expectEqual(@as(?Value, null), tree.search(15));

    try testing.expectEqual(@as(?Value, 60), tree.search(6));
    try testing.expectEqual(@as(?Value, 200), tree.search(20));
}

test "delete all keys until tree is empty" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var tree = BTree.init(alloc);
    defer tree.deinit();

    var i: usize = 1;
    while (i <= 30) : (i += 1) {
        try tree.insert(@intCast(i), @intCast(i * 10));
    }

    i = 1;
    while (i <= 30) : (i += 1) {
        tree.delete(@intCast(i));
    }

    try testing.expectEqual(@as(usize, 0), tree.height());
    try testing.expectEqual(@as(?Value, null), tree.search(1));
    try testing.expect(tree.root == null);
}
