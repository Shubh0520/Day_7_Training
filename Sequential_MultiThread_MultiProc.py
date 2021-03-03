import random
import operator
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool as ProcessPool
from concurrent.futures import ProcessPoolExecutor
import time

# operators = [["+","add"], ["-","sub"], ["*","mul"], ['/',"div"]]
# operators = ["+", "-", "*", '/']
operators = [("+", operator.add), ("-", operator.sub), ("*", operator.mul), ('/', operator.truediv)]

# op,oper = random.choice(operators)
# print(type(op), type(oper))
# print(op, oper)
data_size = 200


def data_generator(data_size):
    data_list = []
    for i in range(1, data_size):
        number1 = random.randint(1, 2000)
        number2 = random.randint(1, 2000)
        op, fn = random.choice(operators)
        data = {"num1": number1, "num2": number2, "operator": op}
        # print(data)
        data_list.append(data)
    return data_list


def sequential_execution(dataset):
    """

    :param dataset:
    :return:
    """
    result_list = []
    for data in dataset:
        num1 = data["num1"]
        num2 = data["num2"]
        oper = data["operator"]
        if oper == "+":
            res = operator.add(num1, num2)
        elif oper == "-":
            res = operator.sub(num1, num2)
        elif oper == "*":
            res = operator.mul(num1, num2)
        elif oper == "/":
            res = operator.truediv(num1, num2)
        else:
            res = 0
        res_dict = {"num1": num1, "num2": num2, "operator": oper, "result": res}
        result_list.append(res_dict)
    return result_list


def task(in_data):
    """

    :param in_data:
    :return:
    """
    # print("-------------IN -------", in_data)
    num1 = in_data["num1"]
    num2 = in_data["num2"]
    oper = in_data["operator"]
    if oper == "+":
        res = operator.add(num1, num2)
    elif oper == "-":
        res = operator.sub(num1, num2)
    elif oper == "*":
        res = operator.mul(num1, num2)
    elif oper == "/":
        res = operator.truediv(num1, num2)
    else:
        res = 0
    res_dict = {"num1": num1, "num2": num2, "operator": oper, "result": res}
    # print("---------------out---------", res_dict)
    return res_dict


def parallel_threading(data_list):
    """

    :param data_list:
    :return:
    """
    pool = ThreadPool(10)
    results = pool.map(task, data_list)
    pool.close()
    pool.join()
    return results


def parallel_processing(data_list):
    """

    :param data_list:
    :return:
    """
    pool_proc = ProcessPool(5)
    results = pool_proc.map(task, data_list)
    pool_proc.close()
    pool_proc.join()
    return results


if __name__ == "__main__":
    # Data Generation
    start_time_dg = time.time()
    out_data = data_generator(data_size)
    print(out_data)
    end_time_dg = time.time()
    print(f"run time of the program for data_generator is: {end_time_dg - start_time_dg}")
    print("---------" * 100)
    # Sequential Execution
    start_time_seq = time.time()
    result = sequential_execution(out_data)
    print(result)
    end_time_seq = time.time()
    print(f"run time of the program for Sequential is: {end_time_seq - start_time_seq}")
    print("---------" * 100)
    # Multi-Threading
    start_time_mt = time.time()
    t_out_res = parallel_threading(out_data)
    print(t_out_res)
    end_time_mt = time.time()
    print(f"run time of the program for MultiThreading is: {end_time_mt - start_time_mt}")
    print("---------" * 100)
    # Multi-Processing
    start_time_mp = time.time()
    p_out_res = parallel_processing(out_data)
    print(p_out_res)
    end_time_mp = time.time()
    print(f"run time of the program for MultiProcessing is: {end_time_mt - start_time_mt}")
