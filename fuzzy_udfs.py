# -*- coding: utf-8 -*-
from __future__ import division
from pig_util import outputSchema


@outputSchema("result:float")
def around_trapez(x, a, b, c, d):
    if a > b or b > c or c > d:
        raise RuntimeError("Invalid trapezium")

    if x < a or x > d:
        return 0.0
    if x <= b:
        if b == a:
            return 1.0
        return (x - a) / (b - a)
    if x <= c:
        return 1.0
    else:
        if d == c:
            return 1.0
        return (d - x) / (d - c)


@outputSchema("result:chararray")
def assign_ling(ling_level, x):
    ling_name_to_val = {}

    tables = ling_level.split("/")
    for tab in tables:
        elements = tab.split(";")
        integrity_level = around_trapez(
            x, float(elements[1]), float(elements[2]), float(elements[3]), float(elements[4])
        )
        ling_name_to_val[elements[0]] = integrity_level

    for k, v in ling_name_to_val.items():
        print("name: {}, lvl: {}".format(k, v))

    max_entry = max(ling_name_to_val.items(), key=lambda x: x[1]) if ling_name_to_val else (None, 0.0)

    if max_entry[1] == 0.0:
        return "Not classified"

    return max_entry[0]

@outputSchema("result:float")
def fuzzy_join(a, a_step, b, b_step):
    inner_left = calculate_intersection(
        b - b_step, 0, b, 1, a, 1, a + a_step, 0
    )

    inner_right = calculate_intersection(
        a - a_step, 0, a, 1, b, 1, b + b_step, 0
    )

    if inner_left < 0 or inner_right < 0:
            return 0.0
    if inner_right > 1.0:
        return inner_left
    elif inner_left > 1.0:
        return inner_right
    else:
        return max(inner_right, inner_left)

@outputSchema("result:float")
def calculate_intersection(a1, b1, a2, b2, c1, d1, c2, d2):
    m1 = (b2 - b1) / (a2 - a1)
    b_line1 = b1 - m1 * a1

    m2 = (d2 - d1) / (c2 - c1)
    b_line2 = d1 - m2 * c1

    x_intersect = (b_line2 - b_line1) / (m1 - m2)

    return m1 * x_intersect + b_line1

@outputSchema("result:chararray")
def mono(x):
    for index in range(len(x)):
        variables = {'inc': 0, 'dec': 0, 'con': 0}
        if index == 0:
            variables["con"] +=1

        else:
            if x[index] > x[index -1]:
                variables["inc"] += 1
            elif x[index] < x[index -1]:
                variables["dec"] +=1
            else:
                variables["con"] +=1

    max_variable = max(variables, key=variables.get)
    return max_variable

