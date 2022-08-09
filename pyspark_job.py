import pandas
import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .appName("Italian university ranking") \
    .getOrCreate()


def imm_by_year(imm_atenei):
    """ 
    It returns a dataframe with the codes of each Italian university 
    and the number of new enrolled students for each academic year since a.y. 2010/2011
    """
    current_year = imm_atenei.collect()[0][0]
    df = imm_atenei.filter(
        imm_atenei.AnnoA == current_year).select('AteneoCOD')

    for a in range(10, 21):
        imm_year = imm_atenei.filter(imm_atenei.AnnoA == f"20{a}/20{a+1}")
        imm_year = imm_year.withColumn(
            f"imm_{a}_{a+1}", imm_year.Imm_M + imm_year.Imm_F)
        imm_year = imm_year.select('AteneoCOD', f"imm_{a}_{a+1}")

        df = df.join(imm_year, on='AteneoCOD', how='full')

    return df


def lau_by_year(lau_atenei):
    """
    The function returns a dataframe with the codes of each Italian university 
    and the number of graduated students for each academic year since a.y. 2010/2011
    """
    current_year = lau_atenei.collect()[0][0]
    df = lau_atenei.filter(lau_atenei.AnnoS == current_year) \
                   .select('AteneoCOD') \
                   .dropDuplicates(['AteneoCOD'])

    for year in range(2010, 2022):
        lau_year = lau_atenei.filter(lau_atenei.AnnoS == year).select(
            'AteneoCOD', 'AnnoS', 'SESSO', 'Lau')
        lau_year = lau_year.groupBy("AteneoCOD").sum("Lau").select(
            col("AteneoCOD"), col("sum(Lau)").alias(f"lau_{year}"))

        df = df.join(lau_year, on="AteneoCOD", how="full")

    return df


def isc_by_year(isc_atenei):
    """
    It returns a dataframe with the codes of each Italian university 
    and the number of registered students for each academic year since a.y. 2010/2011
    """
    current_year = isc_atenei.collect()[0][0]
    df = isc_atenei.filter(isc_atenei.AnnoA == current_year) \
                   .select('AteneoCOD') \
                   .dropDuplicates(['AteneoCOD'])

    for a in range(10, 21):
        isc_year = isc_atenei.filter(
            isc_atenei.AnnoA == f"20{a}/20{a+1}").select('AteneoCOD', 'AnnoA', 'SESSO', 'Isc')
        isc_year = isc_year.groupBy("AteneoCOD").sum("Isc").select(
            col("AteneoCOD"), col("sum(Isc)").alias(f"Isc_{a}_{a+1}"))

        df = df.join(isc_year, on="AteneoCOD", how="full")

    return df


def perc_lau(df_lau, df_isc, atenei):
    """
    The function reports the percentage of graduated students 
    out of the total number of students enrolled.
    """
    atenei = atenei.withColumnRenamed("COD_Ateneo", "AteneoCOD").filter(
        atenei.status == "Attivo    ", )

    for i in range(10, 21):

        name_col_isc = f"Isc_{i}_{i+1}"
        name_col_lau = f"Lau_20{i+1}"
        name_col_perc = f"Perc_lau_20{i+1}"

        a = df_isc.select(name_col_isc, "AteneoCOD")
        b = df_lau.select(name_col_lau, "AteneoCOD")
        b = b.join(a, on="AteneoCOD", how="full")

        perc = b.withColumn(name_col_perc, (b[name_col_lau] / b[name_col_isc])*100)\
                .select("AteneoCOD", round(f"Perc_lau_20{i+1}", 1).alias(f"Perc_lau_20{i+1}"))

        atenei = atenei.join(perc, on="AteneoCOD", how="full")
    return atenei


def immforst_by_year(imm_forstd):
    """ 
    The function creates a dataframe with the codes of each Italian university 
    and the number of new enrolled foreign students for each academic year since a.y. 2010/2011
    """
    current_year = imm_forstd.collect()[0][0]
    df = imm_forstd.filter(
        imm_forstd.AnnoA == current_year).select('AteneoCOD')

    for a in range(10, 22):
        imm_forstd_year = imm_forstd.filter(imm_forstd.AnnoA == f"20{a}/20{a+1}")\
                                    .select(col('AteneoCOD'), col('Imm_S').alias(f"Immfs_{a}_{a+1}"))

        df = df.join(imm_forstd_year, on="AteneoCOD", how="full")

    return df


def perc_forst_imm(df_immfs, df_imm):
    """
    It returns the percentage of foreign new enrolled students over the toal number of new students. 
    The index reflects the international attractiveness of the universities. 
    """
    df = df_immfs.select("AteneoCOD")

    for i in range(10, 21):

        name_col_immfs = f"Immfs_{i}_{i+1}"
        name_col_imm = f"Imm_{i}_{i+1}"
        name_col_perc = f"Perc_immfs_{i}_{i+1}"

        a = df_immfs.select(name_col_immfs, "AteneoCOD")
        b = df_imm.select(name_col_imm, "AteneoCOD")
        b = b.join(a, on="AteneoCOD", how="full")

        perc = b.withColumn(name_col_perc, (b[name_col_immfs] / b[name_col_imm])*100)\
                .select("AteneoCOD", round(f"Perc_immfs_{i}_{i+1}", 1).alias(f"Perc_immfs_{i}_{i+1}"))

        df = df.join(perc, on="AteneoCOD", how="full")

    return df


def interventions(id_atenei, year):
    """
    Calculate the number of interventions in favor of students (scholarships). Two parameters:
    - id_atenei = a dataframe with the official university identification numbers.
    - year = the last two digits of the current year, i.e. 2022 --> 22 . 
    """
    for y in range(year-6, year):
        df = spark.read.json(f'gs://un_rank/interventi_atenei_20{y}.json')
        ay = df.collect()[0][0]

        for i in range(4, 9):
            df = df.withColumn(f"INT_{i}", df[i].cast(IntegerType()))

        df = df.withColumn(f"tot_int_{ay}", (df[11]+df[12]+df[13]+df[14]+df[15])) \
               .groupBy("COD_ATENEO").sum(f"tot_int_{ay}") \
               .select(col("COD_ATENEO"), col(f"sum(tot_int_{ay})").alias(f"tot_int_{ay}"))

        id_atenei = id_atenei.join(df, on="COD_ATENEO", how="full")

    return id_atenei


def score_interventions(isc_atenei, year):
    """
    It returns an estimate of the commitment made by universities in the 
    provision of interventions in favor of students (e.g. scholarships). 
    For each academic year, it is assigned a score of 100 to the university 
    with the highest ratio between: 
    - the number of interventions in that academic year.
    - the total number of students enrolled.
    All other ratios are rescaled from this maximum value.
    """
    current_year = isc_atenei.collect()[0][0]
    id_atenei = isc_atenei.filter(isc_atenei.AnnoA == current_year) \
                          .select(col('AteneoCOD').alias('COD_ATENEO')) \
                          .dropDuplicates(['COD_ATENEO'])

    iv = interventions(id_atenei, year)    # previously defined function
    df = isc_by_year(isc_atenei)           # previously defined function

    col_list = df.columns
    indexes = [0, -6, -5, -4, -3, -2, -1]  # only consider the last six years
    df = df.select([col_list[i] for i in indexes]).withColumnRenamed(
        'AteneoCOD', 'COD_ATENEO')

    df = df.join(iv, on="COD_ATENEO", how="full")

    for y in range(year-6, year):
        df = df.withColumn(
            f"score_int_{y-1}_{y}", (col(f"tot_int_20{y-1}-20{y}") / col(f"Isc_{y-1}_{y}"))*100)
        max_val = df.groupby().max(f"score_int_{y-1}_{y}").first().asDict()[
            f"max(score_int_{y-1}_{y})"]  # find the maximum
        # rescaling
        df = df.withColumn(
            f"score_int_{y-1}_{y}", df[f"score_int_{y-1}_{y}"]*100 / max_val)
        # rounding
        df = df.withColumn(
            f"score_int_{y-1}_{y}", round(df[f"score_int_{y-1}_{y}"], 1))

    col_list = df.columns
    df = df.select([col_list[i] for i in indexes]).withColumnRenamed(
        'COD_ATENEO', 'AteneoCOD')

    return df


def global_ranking(atenei, services, international_outlook, quality_education, year):
    """
    It returns a general ranking of the Italian universities, based on the scores of the three indexes. 
    """
    atenei_mod = atenei.filter(atenei.status == "Attivo    ")

    for y in range(year-6, year):
        df_s = services.select(col('AteneoCOD'), col(
            f"score_int_{y-1}_{y}")).withColumnRenamed("AteneoCOD", "COD_Ateneo")

        df_io = international_outlook.select(col("AteneoCOD"), col(
            f"Perc_immfs_{y-1}_{y}")).withColumnRenamed("AteneoCOD", "COD_Ateneo")
        df_1 = df_s.join(df_io, on="COD_Ateneo", how="full")

        df_qe = quality_education.select(col("AteneoCOD"), col(
            f"Perc_lau_20{y}")).withColumnRenamed("AteneoCOD", "COD_Ateneo")
        df_2 = df_1.join(df_qe, on="COD_Ateneo", how="full")

        df = df_2.withColumn(f"Score_20{y}", (col(f"Perc_lau_20{y}")
                                              + col(f"Perc_immfs_{y-1}_{y}")
                                              + col(f"score_int_{y-1}_{y}") / 3))\
            .select("COD_Ateneo", f"Score_20{y}")

        atenei_mod = atenei_mod.join(df, on="COD_Ateneo", how="full")

    return atenei_mod


atenei = spark.read.json('gs://un_rank/atenei.json')
imm_atenei = spark.read.json('gs://un_rank/imm_atenei.json')
lau_atenei = spark.read.json('gs://un_rank/lau_atenei.json')
isc_atenei = spark.read.json('gs://un_rank/isc_atenei.json')
imm_forstd = spark.read.json('gs://un_rank/imm_forstd.json')
lau_forstd = spark.read.json('gs://un_rank/lau_forstd.json')
isc_forstd = spark.read.json('gs://un_rank/isc_forstd.json')

id_atenei = atenei.select('COD_Ateneo').withColumnRenamed(
    'COD_Ateneo', 'COD_ATENEO')

year = int(str(datetime.date.today().year)[-2:])

df_lau = lau_by_year(lau_atenei)
df_isc = isc_by_year(isc_atenei)
df_imm = imm_by_year(imm_atenei)
df_immfs = immforst_by_year(imm_forstd)

df_qoe = perc_lau(df_lau, df_isc, atenei)      # quality of education index
df_intout = perc_forst_imm(df_immfs, df_imm)  # international outlook index
df_services = score_interventions(isc_atenei, year)        # services index
df_general = global_ranking(
    atenei, df_services, df_intout, df_qoe, year)  # general ranking

df_qoe.write.csv('quality_of_education.csv')
df_intout.write.csv('international_outlook.csv')
df_services.write.csv('services.csv')
df_general.write.csv('general_ranking.csv')
