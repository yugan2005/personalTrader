def clean_yahoo(dataframe):
    dataframe = dataframe[dataframe.Volume != 0]
    dataframe = dataframe[dataframe.Volume != dataframe.Volume.shift()]
    return dataframe


def main():
    pass


if __name__ == '__main__':
    main()
