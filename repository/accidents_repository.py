ACCIDENTS_DATA_PATH = \
    'C:/Users/Simch/PycharmProjects/chicago_car_accidents/data/Traffic_Crashes_Crashes.csv'


def read_csv(file_path: str):

    with open(file_path, 'r') as f:
        content = f.read()
    return content


if __name__ == '__main__':
    print(read_csv(ACCIDENTS_DATA_PATH))
