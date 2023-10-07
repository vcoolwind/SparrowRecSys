import os

# 加载tf是一个耗时操作
import tensorflow as tf


# 获取目录分隔符
root_project_name = "SparrowRecSys"
# 获取当前工作目录
current_dir = os.getcwd()
# 按照关键字分割字符串
parts = current_dir.split(root_project_name)
# 取第一个子串
root_dir = parts[0]


def getOnlineServerFileBySubdir(subdir):
    # 将字符串参数转换为字符串列表
    sub_dirs = subdir.split("/")
    return getOnlineServerFileByDirs(*sub_dirs)

def getOutputModelDataDirBySubdir(subdir):
    # 将字符串参数转换为字符串列表
    sub_dirs = subdir.split("/")
    return getOutputModelDataDir(*sub_dirs)

def getOnlineServerFileByDirs(*args):
    data_project_name = "OnlineServer"
    fix_dir = os.path.join(root_dir, root_project_name, data_project_name, "src", "main", "resources")
    resources_dir = os.path.join(fix_dir, *args)
    placeholder = ""
    if not resources_dir.startswith("/"):
        placeholder = "/"
    return "file://" + placeholder + resources_dir


def getOutputModelDataDir(*args):
    data_project_name = "output_model_data"
    fix_dir = os.path.join(root_dir, root_project_name, data_project_name)
    resources_dir = os.path.join(fix_dir, *args)
    placeholder = ""
    if not resources_dir.startswith("/"):
        placeholder = "/"
    return "file://" + placeholder + resources_dir
    return output_data_dir


# load sample as tf dataset
def get_dataset(file_path):
    dataset = tf.data.experimental.make_csv_dataset(
        file_path,
        batch_size=12,
        label_name='label',
        na_value="0",
        num_epochs=1,
        ignore_errors=True)
    return dataset


if __name__ == '__main__':
    path = getOnlineServerFileBySubdir("webroot/sampledata/ratingsSimple.csv")
    print(path)

    dir_path = getOutputModelDataDirBySubdir("neuralcf/002")
    print(dir_path)


    # path = getOnlineServerFileByDirs("webroot", "sampledata", "trainingSamples.csv")
    # print(path)
    #
    # # Test samples path, change to your local path
    # test_samples_file_path = tf.keras.utils.get_file("testSamples.csv", path)
    # # split as test dataset and training dataset
    # test_dataset = get_dataset(test_samples_file_path)
    # # 从数据集中取出一行数据
    # for row in test_dataset.take(1):
    #     print(row)

