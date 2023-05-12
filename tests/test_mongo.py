from src.services.mongo_utils import _multiple_filter
from src.services.mongo_utils import _calculate_changes


def test_multiple_filter():
    test_sequence = [
        {'a': 1, 'b': 1, 'id': 120},
        {'a': 1, 'b': 2, 'id': 121},
        {'a': 3, 'b': 2, 'id': 123},
        {'a': 1, 'b': 1, 'id': 124},
        {'a': 3, 'b': 3, 'id': 125},
    ]
    # test single key a
    FILTER_A_NUM_GROUPS = 2
    FILTER_A_GROUP_LENGTHS = [3, 2]
    assert len(list(_multiple_filter(['a'], test_sequence))) == FILTER_A_NUM_GROUPS
    assert [len(l) for l in _multiple_filter(['a'], test_sequence)] == FILTER_A_GROUP_LENGTHS
    # test single key b
    FILTER_B_NUM_GROUPS = 3
    FILTER_B_GROUP_LENGTHS = [2, 2, 1]
    assert len(list(_multiple_filter(['b'], test_sequence))) == FILTER_B_NUM_GROUPS
    assert [len(l) for l in _multiple_filter(['b'], test_sequence)] == FILTER_B_GROUP_LENGTHS
    # test no key 
    FILTER_NO_NUM_GROUPS = 1
    FILTER_NO_GROUP_LENGTHS = [len(test_sequence)]
    assert len(list(_multiple_filter([], test_sequence))) == FILTER_NO_NUM_GROUPS
    assert [len(l) for l in _multiple_filter([], test_sequence)] == FILTER_NO_GROUP_LENGTHS
    # test multiple keys
    FILTER_2_KEYS_NUM_GROUPS = 6
    FILTER_2_KEYS_GROUP_LENGTHS = [2, 1, 0, 0, 1, 1]
    assert len(list(_multiple_filter(['a', 'b'], test_sequence))) == FILTER_2_KEYS_NUM_GROUPS
    assert [len(l) for l in _multiple_filter(['a', 'b'], test_sequence)] == FILTER_2_KEYS_GROUP_LENGTHS


def test_calculate_changes():
    test_sequence = [
        {'val': 1, 'id': 124},
        {'val': 4, 'id': 123},
        {'val': 2, 'id': 122},
        {'val': 2, 'id': 121},
        {'val': 0, 'id': 120},
    ]
    truths = [
        {'val': 1, 'id': 124, 'change': -3, 'percentage_change': -75.0},
        {'val': 4, 'id': 123, 'change': 2, 'percentage_change': 100.0},
        {'val': 2, 'id': 122, 'change': 0, 'percentage_change': 0.0},
        {'val': 2, 'id': 121, 'change': 2, 'percentage_change': None},
        {'val': 0, 'id': 120, 'change': None, 'percentage_change': None}
    ]
    results = _calculate_changes('val', test_sequence)
    assert len(test_sequence) == len(results)
    assert len(truths) == len(results)
    # test integrity
    for org, res, truth in zip(test_sequence, results, truths):
        # integrity
        assert org['val'] == res['val']
        assert org['id'] == res['id']
        # completeness
        assert 'change' in res
        assert 'percentage_change' in res
        # accuracy
        assert res == truth
    
