import h5py
from object_manager import ObjectManager

def assert_true(x):
    if not x:
        raise ValueError(x)

def assert_equal(x, y):
    if x != y:
        raise ValueError("not equal")

def mm_test(f):
    mm = ObjectManager(f)
    print(mm.names)
    assert_equal(mm.count, 1)

    group_paths = ["g1", "g2", "g3"]
    mm2 = mm.create_groups(group_paths)
    print(mm2.names)
    assert_equal(mm2.count, 3)

    mm3 = mm2.create_groups(["grp",])
    print(mm3.names)
    assert_equal(mm3.count, 3)

    mm3 = ObjectManager(mm2, ["grp",])
    print("mm3.names:", mm3.names)
    assert_equal(mm3.names, ['/g1/grp', '/g2/grp', '/g3/grp'])

    group_paths = ["A", "B", "C"]
    mm4 = mm3.create_groups(group_paths)
    print(mm4.names)
    assert_equal(mm4.count, 3)

    attr_names = ["a1",]
    attr_values = [42,]
    mm4.set_attrs(attr_names, attr_values)

    print(mm4.get_attrs(attr_names))

    attr_names = ["x", "y", "z"]
    attr_values = [111,222,333]
    mm4.set_attrs(attr_names, attr_values)

    print(mm4.get_attrs(attr_names))

    group_paths = ["g1", "g2", "g3"]
    mm = ObjectManager(f, group_paths)
    print(mm.names)
    dset_paths = ["dset1", "dset2", "dset3"]
    mm2 = mm.create_datasets(dset_paths, shape=(10,), dtype="i4")
    print(mm2.names)

    selections = [slice(0,10), slice(1,9), slice(2,8)]
    values = [list(range(1, 11)), list(range(2, 10)), list(range(3, 9))]
    mm2.write_selections(selections=selections, values=values)
    arrs = mm2.read_all()
    for arr in arrs:
        print(arr)

#
# main
#

with h5py.File("mm_test.h5", "w") as f:
    mm_test(f)
