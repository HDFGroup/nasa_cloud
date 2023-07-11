class MultiManager():
    def __init__(self, parent, h5paths=[]):
        objs = []
        count = len(h5paths)
        
        if isinstance(parent, MultiManager):
            self._file = parent._file
            
            if count == 0:
                # just clone this object
                for obj in parent._objs:
                    objs.append(obj)
            elif count == 1:
                h5path = h5paths[0]
                for obj in parent._objs:
                    child = obj[h5path]
                    objs.append(child)
            elif count == len(parent._objs):
                for i in range(count):
                    obj = parent._objs[i]
                    h5path = h5paths[i]
                    child = obj[h5path]
                    objs.append(child)
            else:
                raise ValueError("number of h5paths doesn't match parent count")
        else:
            self._file = parent.file  # expecting parent to be a group
            if count > 0:
                for h5path in h5paths:
                    objs.append(parent[h5path])
            else:
                objs.append(parent)
            
        self._objs = objs
                
            
    @property 
    def names(self):
        names = []
        for obj in self._objs:
            names.append(obj.name)
        return names
    
    @property
    def count(self):
        return len(self._objs)
    

            
    def create_groups(self, names):
        count = len(names)
        if count < 1:
            raise ValueError("no names given")
            
        h5paths = []
        if count == 1 or len(self._objs) == 1:
            for obj in self._objs:
                for name in names:
                    grp = obj.create_group(name)
                    h5paths.append(grp.name)
        elif count == len(self._objs):
            # create group with each name
            for i in range(count):
                obj = self._objs[i]
                name = names[i]
                grp = obj.create_group(name)
                h5paths.append(grp.name)
        else:
            raise ValueError("number of names doesn't match object count")
        # return an om instance with this group set
        mm = MultiManager(self._file, h5paths)
        return mm
                
    def set_attrs(self, names, values):
        count = min(len(names), len(values))
        for obj in self._objs:
            for i in range(count):
                name = names[i]
                value = values[i]
                obj.attrs[name] = value
                
    def get_attrs(self, names):
        values = []
        for obj in self._objs:
            if len(self._objs) == 1:
                obj_list = values
            else:
                obj_list = []
            for name in names:
                value = obj.attrs[name]
                obj_list.append(value)
                
            if len(self._objs) == 1:
                pass # nothing to do
            else:
                values.append(obj_list)
        return values
    
    def create_datasets(self, names, shape=None, dtype=None):
        """ create given datasets"""
        count = len(names)
        if count < 1:
            raise ValueError("no names given")
        
        h5paths = []
        if count == 1 or len(self._objs) == 1:
            for obj in self._objs:
                for name in names:
                    dset = obj.create_dataset(name, shape, dtype=dtype)
                    h5paths.append(dset.name)
        elif count == len(self._objs):
            # create dataset with each name
            for i in range(count):
                obj = self._objs[i]
                name = names[i]
                dset = obj.create_dataset(name, shape, dtype=dtype)
                h5paths.append(dset.name)
        else:
            raise ValueError("number of names doesn't match object count")
        # return an om instance with this group set
        mm = MultiManager(self._file, h5paths)
        return mm
       
                
    def write_selections(self, selections, values):
        count = min(len(self._objs), len(selections), len(values))
        for i in range(count):
            dset = self._objs[i]
            print(dset.name)
            selection = selections[i]
            value = values[i]
            dset.__setitem__(selection, value)
                
    def read_selections(self, selections):
        values = []
        count = min(len(self._objs), len(selections))
        for i in range(count):
            dset = self._objs[i]
            selection = selections[i]
            value = dset.__getitem__(selection)
            values.append(value)
        return values
    
    def read_all(self):
        arrs = []
        for obj in self._objs:
            rank = len(obj.shape)
            if rank == 0:
                arr = None
            elif rank == 1:
                extent = obj.shape[0]
                arr = obj.__getitem__(slice(0, extent))
            else:
                slices = []
                for dim in range(rank):
                    extent = obj.shape[dim]
                    slices.append(slice(0, extent))
                arr = obj.__getitem__(slices)
            arrs.append(arr)
        return arrs

                    
                
            
                
    
    