import os


if __name__ == "__main__":
	base_path = "./datasets/"

	files_2016 = [f for f in os.listdir(base_path + "2016")]
	files_2017 = [f for f in os.listdir(base_path + "2017")]
	files_2018 = [f for f in os.listdir(base_path + "2018")]
	files_2019 = [f for f in os.listdir(base_path + "2019")]
	files_2020 = [f for f in os.listdir(base_path + "2020")]

	files_2016_2017 = list(set(files_2016).intersection(files_2017))
	files_2016_2017_2018 = list(set(files_2016_2017).intersection(files_2018))
	files_2019_2020 = list(set(files_2019).intersection(files_2020))
	common_files = list(set(files_2016_2017_2018).intersection(files_2019_2020))

	print(f"{len(common_files)} files in common between 2016-2017-2018-2019-2020")
	print(len(common_files))
	print(common_files)
