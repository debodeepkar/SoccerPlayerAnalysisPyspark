# We must import pyspark-sql for the analysis
from pyspark.sql import *
import matplotlib.pyplot as plt

# Here we are creating a Spark Session, Spark context is already present inside it so no need for that explicitly
spark = SparkSession.builder.appName("Analysis Project").getOrCreate()

# Here we read the data and store it in 'df'
df = spark.read.csv('players.csv', header=True)

# Here we create a temporary SQL view from the data to work with
df.createOrReplaceTempView("soccer")

print("1. Load the csv file and show top 5 records from it.")

spark.sql("select * from soccer").show(5)

print("2. How you would be able to see each column's name.")

df.printSchema();


print("3. Need to show number of rows and columns of this dataset.")


spark.sql("select count(sofifa_id) as Number_of_Rows from soccer").show()
print("Number of columns : "+str(len(df.columns)))


print("4. Show number of players and their countries.")

spark.sql("select  nationality, count(nationality) as number_of_players from soccer group by nationality order by number_of_players desc").show()



print("5. If you find many records in point 4 then show only top 10 countries and their number of players.")

spark.sql("select  nationality, count(nationality) as number_of_players from soccer group by nationality order by number_of_players desc").show(10)



print("6. Now you have to create a bar plot of top 5 countries and their number of players, try to fill green color in bars.")

df6 = spark.sql("select  nationality, count(nationality) as number_of_players from soccer group by nationality order by number_of_players desc").toPandas()
df6.set_index('nationality')
plt.figure(figsize=(10,10))
x_ax=df6['nationality'].head(5)
y_ax=df6["number_of_players"].head(5)
plt.xlabel("Countries")
plt.ylabel("Number of Players")
plt.title("Top 5 Countries and their Number of Players")
bar1=plt.bar(x_ax, y_ax, color='green', width=0.4)
for bar in bar1:
    y_val=bar.get_height()
    plt.text(bar.get_x()+0.05,y_val+0.05,y_val)
plt.show()




print("7. Show top 5 players short name and wages.")

spark.sql("select short_name, value_eur, wage_eur from soccer order by int(value_eur) desc ").show(5)




print("8. Show top 5 players short name and wages that are getting highest salaries")

spark.sql("select short_name, value_eur, wage_eur from soccer order by int(wage_eur) desc ").show(5)





print("9. Create a bar plot of point number 8.")

df9 =spark.sql("select short_name, value_eur, wage_eur from soccer order by int(wage_eur) desc ").toPandas()
print(df9.dtypes)
df9['short_name']=df9['short_name'].astype(str)
df9['wage_eur']=df9['wage_eur'].astype(int)
x_ax_1= df9['short_name'].head(5)
y_ax_1= df9['wage_eur'].head(5)
plt.figure(figsize=(10,10))
plt.xticks(rotation =20)
plt.xlabel("PLAYERS")
plt.ylabel("WAGES")
plt.title("Top 5 Earning Players")
bars = plt.bar(x_ax_1, y_ax_1, color='indigo')
for bar in bars:
    y_val=bar.get_height()
    plt.text(bar.get_x()+0.25,y_val+20,y_val)
plt.show()





print("10.Show top 10 records of Germany.")

spark.sql("select * from soccer where nationality = 'Germany'").show(10)





print("11.Now show top 5 records of Germany players who have maximum height, weight and wages.")
print("Height")
spark.sql("select * from soccer where nationality = 'Germany' order by int(height_cm) desc").show(5)
print("Weight")
spark.sql("select * from soccer where nationality = 'Germany' order by int(weight_kg) desc").show(5)
print("Wages")
spark.sql("select * from soccer where nationality = 'Germany' order by int(wage_eur) desc").show(5)





print("12.Show short name and wages of top 5 Germany players.")

spark.sql(" select short_name, int(wage_eur), value_eur from soccer where nationality= 'Germany' order by int(wage_eur) DESC").show(5)




print("13.Show top 5 players who have great shooting skills among all with short name.")

spark.sql("select short_name, shooting from soccer order by int(shooting) DESC ").show(5)




print("14.Show top 5 players records (short name, defending, nationality, and club) that have awesome defending skills.")

spark.sql("select short_name, defending, nationality, club from soccer order by int(defending) DESC").show(5)



# from here onwards the player with highest salary is the better player

print("15.Show wages records of top 5 players of 'Real Madrid' team.")

spark.sql("select long_name, wage_eur from soccer where club='Real Madrid' order by int(wage_eur) DESC").show(5)





print("16.Show shooting records of top 5 players of 'Real Madrid' team.")

spark.sql("select long_name, shooting from soccer where club='Real Madrid' order by int(wage_eur) DESC").show(5)





print("17.Show defending records of top 5 players of 'Real Madrid' team.")

spark.sql("select long_name, defending from soccer where club='Real Madrid' order by int(wage_eur) DESC").show(5)


print("18.Show nationality records of top 5 players of 'Real Madrid' team.")

spark.sql("select long_name, nationality from soccer where club='Real Madrid' order by int(wage_eur) DESC").show(5)