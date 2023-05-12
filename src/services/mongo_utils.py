from typing import Generator
from typing import Sequence
from typing import List
from typing import Dict
import functools
import itertools


# date/time parsing rules for $addFields
YEAR_FIELD = {'$toInt': {'$substr': ['$review_date', 0, 4]}}
MONTH_FIELD = {'$toInt': {'$substr': ['$review_date', 5, 2]}}
QUARTER_FIELD = {
    '$cond': [
        {'$lte': [MONTH_FIELD, 3]}, 
        1, # quarter 1
        {'$cond' : [
            {'$lte': [MONTH_FIELD, 6]},
            2, # quarter 2
            {'$cond': [
                {'$lte': [MONTH_FIELD, 9]},
                3, # quarter 3
                4 # quarter 4
            ]}
        ]},
    ],
}
POLARITY_FIELD = {  # filter to keep only nlp result of the current nlp pack
    '$arrayElemAt': [
        {
            '$filter': {
                'input': {'$objectToArray': '$nlp_results'},
                'cond': {
                    '$eq': ['$$this.k', '$nlp_results.__NLP_PACKS__']
                }
            }
        },
        0
    ]
}


# helpers for calculating aggregated statistics on output of mongodb queries
# these are to replicate some necessary functionalities of Pandas, e.g.
# groupby, shift, loc
def _multiple_filter(
    keys: Sequence[str], 
    data_sequence: Sequence[Dict]
) -> Generator:
    """
    Filter by multple keys and yield an generator
    """
    values_for_filters = [set([r[k] for r in data_sequence]) for k in keys]
    for tpl in itertools.product(*values_for_filters):
        yield list(filter(
            lambda x: all([x[k] == v for k, v in zip(keys, tpl)]),
            data_sequence
        ))


def _calculate_changes(value_key: str, data_sequence: Sequence[Dict]):
    """
    Calculate changes based on sorted sequence of data dict
    NOTE: will NOT mutate inputs
    """
    def _safe_percentage(a, b):
        try:
            res = (a - b) / b * 100
        except ZeroDivisionError:
            res = None
        finally:
            return res
        
        
    values_no_head = [
        d[value_key] for d in data_sequence[1:] # lagged sequence
    ]
    values_no_tail = [
        d[value_key] for d in data_sequence[:-1]
    ]
    values_changes = [
        t - h for h, t in zip(values_no_head, values_no_tail)
    ] + [None] # with padding
    values_percentage_changes = [
        _safe_percentage(t, h) for h, t in zip(values_no_head, values_no_tail)
    ] + [None] # with padding
    # update
    return_sequence = []
    for ar, c, pc in zip(
        data_sequence, values_changes, values_percentage_changes
    ):
        return_sequence.append({
            **ar,
            'change': c,
            'percentage_change': pc
        })
    return return_sequence


def calculate_change(
    sort_keys: Sequence[str], 
    value_key: str, 
    filter_keys: Sequence[str] = None, 
):
    """
    Decorator factory. 
    Calculate changes in value keys, based on sort keys.
    Filter keys can be supplied so that calculation can be 
    done on facets of data sequence
    """
    def _calculate_change_deco(func):
        """
        Decorator for MongoDB queries
        """       
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            """
            Wrap mongoDB queries:
            """
            data_sequence = func(*args, **kwargs)
            # calculate change percentage
            sorted_sequence = sorted(
                data_sequence, 
                key=lambda x: [x[i] for i in sort_keys],
                reverse=True
            )
            calculated_data = []
            if filter_keys:
                filter_values_generator = _multiple_filter(
                    filter_keys, sorted_sequence
                )
                for filtered_sequence in filter_values_generator:
                    calc_filtered_sequence = _calculate_changes(
                        value_key, filtered_sequence
                    )
                    calculated_data.extend(calc_filtered_sequence)
            else:
                calculated_data.extend(
                    _calculate_changes(value_key, sorted_sequence)
                )

            return calculated_data

        return wrapper
    
    return _calculate_change_deco
