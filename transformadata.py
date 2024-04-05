spark = SparkSession.builder.appName('myapplication').getOrCreate()

# %%
df_circuit = spark.read.csv('C:\\Users\\MouhamethSECK\\Downloads\\raw\\raw\\circuits.csv', header='True')


# %%
df_circuit.show()

# %%
df_circuit.select('name','location','country','lat').show

# %%
df_circuit.printSchema()

# %%
circuits_selected_df = df_circuit.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# %%
circuits_selected_df.show()

# %%
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# %%
circuits_renamed_df.show()

# %%
circuits_renamed_df.filter(circuits_renamed_df['location']== 'Melbourne').show()

# %%
filtered_df = circuits_renamed_df.filter(circuits_renamed_df.altitude > 100).show()


# %%
french_circuits_df = circuits_renamed_df.filter(circuits_renamed_df.country == "France")


# %%
french_circuits_df.show()

# %%
japan_circuit_df = circuits_renamed_df.filter(circuits_renamed_df.country=="Japan")

# %%
japan_circuit_df.show()

# %%
japan_circuit_df.filter(circuits_renamed_df.latitude < 35).show()

# %%
# le nombre total de circuit par pays
nomber_of_contry = circuits_renamed_df.groupBy ('country').count('')

# %%
circuits_per_country_df = circuits_renamed_df.groupBy("country").count().orderBy("count", ascending=False )


# %%
circuits_per_country_df.show()

# %%
circuit_df_country_latitude = circuits_renamed_df.groupBy('country').avg("latitude" )

# %%
circuits_renamed_df = circuits_renamed_df.withColumn("latitude", circuits_renamed_df["latitude"].cast("float"))


# %%
circuits_renamed_df.show()

# %%
df_race = spark.read.csv('C:\\Users\\MouhamethSECK\\Downloads\\raw\\raw\\races.csv', header='true')


# %%
df_race.show()

# %%
df_merge = df_race.join(circuits_renamed_df)

# %%
df_merge.show()



