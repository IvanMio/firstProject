

a, b, c = 15, 400, 5
target_range = range(a, b, c)
d, e, f = 0, 24, 1
hour_range = range(d, e, f)
range_paar_list = list()
for entry_hour in hour_range:
    for ticks_to_target in target_range:
        range_paar_list.append([entry_hour, ticks_to_target])
print(range_paar_list)
for range_paar in range_paar_list:
    print(range_paar[0], range_paar[1])

        # print(entry_hour, ticks_to_target)