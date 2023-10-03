import os

def getResourcesDir():
  project_name = "SparrowRecSys"
  # 获取当前工作目录
  current_dir = os.getcwd()
  # 按照关键字分割字符串
  parts = current_dir.split(project_name)
  # 取第一个子串
  root_dir = parts[0]
  resources_dir = "file://" + root_dir + project_name + "/src/main/resources"
  return resources_dir