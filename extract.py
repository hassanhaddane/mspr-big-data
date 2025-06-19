def open_file_csv(dataframe,filepath):
    openDataFrame = (
        dataframe
        .read
        .format("csv")
        .option("header", True)
        .option("delimiter", ";")
        .option("inferSchema", True)
        .option("charset", "UTF-8")
        .load(filepath)
    )
    return openDataFrame
