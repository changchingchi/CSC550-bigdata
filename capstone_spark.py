def parse_state(line):
    line = line.strip().split(",")  # strip out carriage return
    key_in = line[5]  # key is first item in list
    value_in = line[0]  # value is 2nd item
    return (key_in, value_in)
def count_DRG(line):
    #Row(_1=u'CA', _2=u'948 - SIGNS & SYMPTOMS W/O MCC')
    key_in = line[1]  # key is first item in list
    return (key_in, 1)

dataset_raw = sc.textFile("input/capstone.csv")
dataset_1 = dataset_raw.map(parse_state)
df_dataset = dataset_1.toDF();
df_ca = df_dataset.where(df_dataset['_1']=='CA')
dataset_ca = df_ca.rdd;

dataset_ca = dataset_ca.map(count_DRG)
dataset_caMaped = dataset_ca.map(count_DRG)
countsByDRG = dataset_caMaped.reduceByKey(lambda a,b: a+b)
countsByDRG.collect()
df_DRGcounts = countsByDRG.toDF()
df_DRGcountsSorted = df_DRGcounts.sort("_2", ascending=False)
df_DRGcountsSorted.show(20,False)

