files = []
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000000')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000001')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000002')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000003')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000004')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000005')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000006')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000007')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000008')
files.append('gs://kiana_bucket/kiana_data/rio_bq_2019000000000009')

df0 = spark.read.json(files[0])

df = df0
for i in range(10):
    print("Loading", files[i])
    current_df = spark.read.json(files[i])
    df = df.union(current_df)
