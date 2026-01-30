def clickhouseToDict(result_set:list, columns:list =None):
    
    tests_list = []
    for row in result_set:
        test_dict = {}
        for column_pos in range(len(columns)):
            test_dict[columns[column_pos]] = row[column_pos]

        tests_list.append(test_dict)

    return  tests_list
