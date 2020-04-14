import warnings

"""
This function recursively flattens JSON files.
It can be passed either a list or a dict.
It will always return a single-layer dict (though it will preserve lists of values). Run it in a for loop on groups of keys if you want to build a dataframe.
All nested dicts are appended to their parent with the key name '<parent_key>_<key>'.
Arrays of dicts are all appended to each other, with repeated keys getting '_<num>' appended where <num> = {1,2,3...}.
Hypothetically, this should be handled by pd.io.json.json_normalize(), but said function with yield MemoryErrors if passed too large a nested JSON.
However, pd.io.json.json_normalize() is still handy since unlike pd.read_json(), it can create a DF from an array of dicts.
Running this function on each dict in the array (so all are already flat) will massively increase its speed and memory efficiency.
"""
def jsonNormaliseAndFlattenArrs(x):
    # This section recursively flattens dictionaries.
    if isinstance(x, dict):
        for outerKey in list(x.keys()): # Allows dict size to be changed during iteration
            # If nested dictionary
            if isinstance(x[outerKey], dict):
                newDict = {str(outerKey)+"_"+str(innerKey) : value for innerKey, value in jsonNormaliseAndFlattenArrs(x[outerKey]).items() if not value is None}
                del x[outerKey]
                x.update(newDict)
            # If nested array
            elif isinstance(x[outerKey], list):
                if any(x[outerKey]): # 
                    flattened = jsonNormaliseAndFlattenArrs(x[outerKey])
                    if isinstance(flattened, dict): # Arrays of values that have 'fallen through' the recursive call should not be flattened.
                        x.update({str(outerKey)+"_"+str(innerKey) : value for innerKey, value in flattened.items() if not value is None})
                        del x[outerKey]
                    
                # If empty array
                else:
                    del x[outerKey]
            # If null
            elif x[outerKey] == None:
                del x[outerKey]

    # This section combines arrays into a single dict. 
    # It assumes that if an array contains dictionaries or nested arrays it contains that exclusively.
    elif isinstance(x, list):
        if len(x) > 0:
            # If array is of dictionaries
            if isinstance(x[0], dict):
                newX = x[0] # Replace the nested array with its first element
                # If there are multiple dictionaries, combine them all into the first one
                if len(x) > 1:
                    for i in range(1, len(x)):
                        newDict = {str(key)+"_"+str(i) : value for key, value in x[i].items() if not value is None} # Append a number to all keys
                        newX.update(newDict)
                x = newX
                # Then call on new dict
                x = jsonNormaliseAndFlattenArrs(x)
                    
            # If array contains nested arrays, concatenate all 
            elif isinstance(x[0], list):
                x = x[0]
                for i in range(1, len(x)):
                    x += x[i]
                x = jsonNormaliseAndFlattenArrs(x)
            # If array contains anything else, will fall through and not be flattened
    return x

"""
This function also flattens JSON files
However, it differs in how it handles arrays. When it comes across an array of dicts, it finds all the unique keys across all dicts,
and then replaces the array of dicts with a dict of arrays each representing a unique key.
Hence, it will create nesting if something is buried within several layers of array.
However, the resulting arrays will always contain exclusively values - all keys are flattened up to the top dict.
"""
def jsonNormaliseAndInvertArrs(x):
    # This section recursively flattens dictionaries.


    if isinstance(x, dict):
        for outerKey in list(x.keys()): # Allows dict size to be changed during iteration
            # If nested dictionary
            if isinstance(x[outerKey], dict):
                newDict = {str(outerKey)+"_"+str(innerKey) : value for innerKey, value in jsonNormaliseAndInvertArrs(x[outerKey]).items() if not value is None}
                del x[outerKey]
                x.update(newDict)
            # If nested array
            elif isinstance(x[outerKey], list):
                if any(x[outerKey]): # 
                    flattened = jsonNormaliseAndInvertArrs(x[outerKey])
                    if isinstance(flattened, dict): # Arrays of values that have 'fallen through' the recursive call should not be flattened.
                        x.update({str(outerKey)+"_"+str(innerKey) : value for innerKey, value in flattened.items() if not value is None})
                        del x[outerKey]
                    
                # If empty array
                else:
                    del x[outerKey]
            # If null
            elif x[outerKey] == None:
                del x[outerKey]

    # This section combines arrays into a single dict. 
    # It assumes that if an array contains dictionaries or nested arrays it contains that exclusively.
    elif isinstance(x, list):
        if len(x) > 0:
            # If array is of dictionaries
            if isinstance(x[0], dict):
                # If there are multiple dictionaries, combine them all into the first one
                if len(x) > 1:
                    # keys = []
                    newX = {}
                    for i in range(len(x)):
                        newDict = jsonNormaliseAndInvertArrs(x[i])
                        for key in newDict.keys():
                            if not key in newX.keys():
                                newX[key] = [None for x in range(i)]
                                newX[key].append(newDict[key])
                            else:
                                newX[key].extend([None for x in range(i - 1 - len(newX[key]))])
                                newX[key].append(newDict[key])
                    # Extend all dicts to full length
                    for key in newX:
                        newX[key].extend([None for x in range(len(x) - len(newX[key]))])
                        # newDict = {str(key)+"_"+str(i) : value for key, value in x[i].items() if not value is None} # Append a number to all keys
                        # newX.update(newDict)
                    x = newX
                elif len(x) == 1:
                    # Replace the nested array with its first element
                    x = x[0]
                    
            # If array contains nested arrays, concatenate all 
            elif isinstance(x[0], list):
                x = x[0]
                for i in range(1, len(x)):
                    x += x[i]
                x = jsonNormaliseAndInvertArrs(x)
            # If array contains anything else, will fall through and not be flattened
    return x

"""
JSON column merge of lists by index - merges multiple keys containing lists into one key containing a list nested by index.
When passed a dict of the form {'merged' : ['col1','col2']}
{'col1': ['i1','i2','i3'], 'col2' : ['j1','j2','j3']} -> {'merged':[['i1','j1'], ['i2','j2']]}
Complementary to jsonNormaliseAndInvertArrays
If all keys contain strings, will place in nested list - {'col1': 'abc', 'col2' : 'def'} -> {'merged' [['abc', 'def']]}
"""
def jsonColumnMergeListsByIndex(data, concat_dict):
    for concat_key, cols in concat_dict.items():
        # Check if the cols are even present
        cols_to_concat = [x for x in cols if x in data.keys()]
        if len(cols_to_concat) > 0:
            # If all the elements to concat are lists, a list of lists will be created with each nested list corresponding
            # to an index across the cols_to_concat.
            if all([isinstance(x, list) for x in [data[y] for y in cols_to_concat]]):
                # Check that all array lengths across cols_to_concat are identical
                length = set([len(data[y]) for y in cols_to_concat])
                assert len(length) == 1, "Uneven array lengths"
                length = length.pop()
                # Build a new nested array
                new_arr = []
                for j in range(length):
                    new_arr.append([data[x][j] for x in cols_to_concat])
                data.update({concat_key : new_arr})
            # If all the elements to concat are strings, a list of strings will be created and nested.
            elif all([isinstance(x, str) for x in [data[y] for y in cols_to_concat]]):
                data.update({concat_key : [[data[x] for x in cols_to_concat]]})
            else:
                raise Exception('Inconsistent column datatypes - only all list or all str are supported.')
        # Delete concatenated keys
        for key in cols_to_concat:
            del data[key]
    return data

"""
Simple JSON column merge - just shoves everything into a list.
Single layer lists will be joined.
Nested lists will be retained.
Deletes duplicates.
"""
def jsonColumnMergeListsSimple(data, concat_dict):
    for concat_key, cols in concat_dict.items():
        # Check if the cols are even present
        cols_to_concat = [x for x in cols if x in data.keys()]
        if len(cols_to_concat) > 0:
            new_arr = []
            for col in cols_to_concat:
                if isinstance(data[col], list):
                    new_data = [x for x in data[col] if x not in new_arr]
                    new_arr.extend(new_data)
                else:
                    if not data[col] in new_arr:
                        new_arr.append(data[col])
            data.update({concat_key : new_arr})
        # Delete concatenated keys
        for key in cols_to_concat:
            del data[key]
    return data

