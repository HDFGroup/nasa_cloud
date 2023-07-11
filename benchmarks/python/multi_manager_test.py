import h5py
import numpy as np
from multi_manager import MultiManager

def assert_true(x):
    if not x:
        raise ValueError(x)

def assert_equal(x, y):
    if x != y:
        raise ValueError("not equal")

def mm_test(f):
    mm = MultiManager(f)
    assert_equal(mm.count, 1)

    group_paths = ["g1", "g2", "g3"]
    mm2 = mm.create_groups(group_paths)
    assert_equal(mm2.count, 3)
    assert_equal(mm2.names, ["/g1", "/g2", "/g3"])
    
    mm3 = mm2.create_groups(["grp",])
    assert_equal(mm3.count, 3)
    assert_equal(mm3.names, ["/g1/grp", "/g2/grp", "/g3/grp"])

    mm3 = MultiManager(mm2, ["grp",])
    assert_equal(mm3.names, ['/g1/grp', '/g2/grp', '/g3/grp'])

    group_paths = ["A", "B", "C"]
    mm4 = mm3.create_groups(group_paths)
    assert_equal(mm4.count, 3)
    assert_equal(mm4.names, ['/g1/grp/A', '/g2/grp/B', '/g3/grp/C'])

    attr_names = ["a1",]
    attr_values = [42,]
    mm4.set_attrs(attr_names, attr_values)
    assert_equal(mm4.get_attrs(attr_names), [[42], [42], [42]])

    attr_names = ["x", "y", "z"]
    attr_values = [111,222,333]
    mm4.set_attrs(attr_names, attr_values)
    assert_equal(mm4.get_attrs(attr_names), [[111, 222, 333], [111, 222, 333], [111, 222, 333]])

    group_paths = ["g1", "g2", "g3"]
    mm = MultiManager(f, group_paths)    
    assert_equal(mm.names, ["/g1", "/g2", "/g3"])

    dset_paths = ["dset1", "dset2", "dset3"]
    mm2 = mm.create_datasets(dset_paths, shape=(10,), dtype="i4")    
    assert_equal(mm2.names, ["/g1/dset1", "/g2/dset2", "/g3/dset3"])

    selections = [slice(0,10), slice(1,9), slice(2,8)]
    # values = [list(range(1, 11)), list(range(2, 10)), list(range(3, 9))]
    dt = np.dtype("i4")
    values = [ np.arange(0, 10, dtype=dt),  np.arange(2, 10, dtype=dt), np.arange(4, 10, dtype=dt)]
    mm2.write_selections(selections=selections, values=values)
    arrs = mm2.read_all()

    assert_equal(len(arrs), 3)

    for i in range(3):
        #assert_equal(len(arr), 10)
        arr = arrs[i]
        print(arr)
        assert_equal(len(arr), 10)
        for j in range(10):
            n = arr[j]
            if j < i:
                # front pad
                assert_equal(n, 0)
            elif j >= 10 - i:
                # back pad
                assert_equal(n, 0)
            else:
                assert_equal(n, i+j)


            

#
# main
#

with h5py.File("mm_test.h5", "w") as f:
    mm_test(f)
print("done")
