"""
debug_utilities.py contains generic utility functions used for debugging data extracts.
"""


def filter_down_df(df, sort_column, startindex, endindex):

    df.sort_values(by=[sort_column])

    if startindex < 2:
        startindex = 0
    if endindex < startindex:
        print(
            "[WARNING] | FUNCTION: filter_down_df - ENDINDEX is less than or equal to STARTINDEX"
        )
    else:
        record_count = df.shape[0]
        if endindex > record_count:
            endindex = record_count

        df = df.head(endindex)
        if startindex > 0:
            df = df.head(-1 * startindex)

    return df


def filter_down_list_of_dict(fullList, sort_column, startindex, endindex):

    if startindex < 2:
        startindex = 0
    if endindex < startindex:
        print(
            "[WARNING] | FUNCTION: filter_down_list_of_dict - ENDINDEX is less than or equal to STARTINDEX"
        )
    else:
        record_count = len(fullList)
        if endindex > record_count:
            endindex = record_count

    filteredlist = []
    for j, item in enumerate(fullList):
        if j >= startindex and j <= endindex:
            filteredlist.append(item)

    return filteredlist
