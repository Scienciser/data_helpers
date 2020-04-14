# Add code/ folder to system path (so can import)
import sys
sys.path.append('data_helpers')

from json_helpers import jsonNormaliseAndFlattenArrs, jsonNormaliseAndInvertArrs

def test_jsonNormaliseAndFlattenArrs():
    x = {'outer' : [{'mid1' : 1, 
        'mid2' : [{'inn1': 2, 'inn2' : 3}, {'inn1' : 4, 'inn3' : 5}], 
        'mid3' : 6}, 
        {'mid2' : 7}]}

    output = jsonNormaliseAndFlattenArrs(x)

    # Currently the outer_mid_2 key becomes outer_mid_2_1 and outer_mid2_inn3 becomes outer_mid_2_inn3_1 because of the simplistic numbering scheme used - if a new key 
    # is found within the second element of an array, it will always have a 1 appended without ever checking if the unappended version exists. This could be improved.
    expected_keys = ['outer_mid1', 'outer_mid2_inn1', 'outer_mid2_inn2', 'outer_mid2_inn1_1', 'outer_mid2_inn3_1', 'outer_mid3', 'outer_mid2_1']
    
    assert isinstance(output, dict)
    assert all([(x in expected_keys) for x in output.keys()])
    assert [output[x] for x in expected_keys] == [1,2,3,4,5,6,7]
    
    
def test_jsonNormaliseAndInvertArrs():
    x = {'outer' : [{'mid1' : 1, 
        'mid2' : [{'inn1': 2, 'inn2' : 3}, {'inn1' : 4, 'inn3' : 5}], 
        'mid3' : 6}, 
        {'mid2' : 7}]}

    output = jsonNormaliseAndInvertArrs(x)

    expected_keys = ['outer_mid1', 'outer_mid2_inn1', 'outer_mid2_inn2', 'outer_mid2_inn3', 'outer_mid3', 'outer_mid2']

    assert isinstance(output, dict)
    assert all([(x in expected_keys) for x in output.keys()])
    assert [output[x] for x in expected_keys] == [[1, None], [[2, 4], None], [[3, None], None], [[None, 5], None], [6, None], [None, 7]]

test_jsonNormaliseAndInvertArrs()