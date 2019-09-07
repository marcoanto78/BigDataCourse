HIVE:

create table fstore (trans_id bigint, store_id int, product_id int, loyalty_member_id int, promo_code_id int, emp_id int, amount double, qty bigint, tx_date string) row format delimited fields terminated by ',';

create table store(store_id int, store_num string, city string, address string, open_dt string, close_dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table employee(emp_id int, emp_num int, store_num string, emp_name string, joining_dt string, designation string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table promotions(promo_cd_id int, promo_cd string, description string, promo_start_dt string, promo_end_dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table loyalty(loyalty_member_id int, cust_id int, card_no string, joining_dt string, points int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table product(product_id int, product_cd string, add_dt string, remove_dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table trans_codes(trans_code_id int, trans_cd string, description string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

-----------------------
SCALA:

val inputfile = sc.textFile("/data/retail/trans_log/trans_log.csv")

val csv = inputfile.map(_.split(","))

import sqlContext.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType};

val impData = csv.filter(x=>(x(1) == "TT" || x(1) == "LL" || x(1) == "PP"))

val TT = impData.filter(x=> (x(1) == "TT")).map(x=> Row(x(0).toInt, x(1), x(2).toInt, x(3), x(4).toDouble, x(5).toDouble, x(6).toInt, x(7), x(8).toInt, x(9).toInt, x(10)))
val PP = impData.filter(x=> (x(1) == "PP")).map(x=> Row(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6)))
val LL = impData.filter(x=> (x(1) == "LL")).map(x=> Row(x(0).toInt, x(1), x(2), x(3), x(4).toInt, x(5).toInt, x(6)))

val ttSchema = StructType(Seq(StructField("seq_num", IntegerType, true), 
StructField("trans_code", StringType, true), 
StructField("item_seq", IntegerType, true), 
StructField("product_cd", StringType, true), 
StructField("amt", DoubleType, true), 
StructField("disc_amt", DoubleType, true), 
StructField("add_flg", IntegerType, true), 
StructField("store", StringType, true), 
StructField("emp_num", IntegerType, true), 
StructField("lane", IntegerType, true), 
StructField("t_stmp", StringType, true) ) )

val llSchema = StructType(Seq(StructField("seq_num", IntegerType, true), 
StructField("trans_code", StringType, true), 
StructField("loyalty_cd", StringType, true), 
StructField("store", StringType, true), 
StructField("emp_num", IntegerType, true), 
StructField("lane", IntegerType, true), 
StructField("t_stmp", StringType, true) ) )

val ppSchema = StructType(Seq(StructField("seq_num", IntegerType, true), 
StructField("trans_code", StringType, true), 
StructField("promo_cd", StringType, true), 
StructField("store", StringType, true), 
StructField("emp_num", IntegerType, true), 
StructField("lane", IntegerType, true), 
StructField("t_stmp", StringType, true) ) )

val ttDF = sqlContext.createDataFrame(TT, ttSchema)
val llDF = sqlContext.createDataFrame(LL, llSchema)
val ppDF = sqlContext.createDataFrame(PP, ppSchema)

ttDF.registerTempTable("ttST")
llDF.registerTempTable("llST")
ppDF.registerTempTable("ppST")

def getTrandId(ts:String, d_store_id:Int, d_lane:Int, d_trans_seq:Int): Long = {
  val daydate = ts.replaceAll("-","").split(" ")(0)
  val store_id = "%05d".format(d_store_id)
  val lane = "%02d".format(d_lane)
  val trans_seq = "%04d".format(d_trans_seq)
  (daydate + store_id.toString + lane.toString + trans_seq.toString).toLong
}

val transId = getTrandId(_,_,_,_)
val tid = sqlContext.udf.register("getTransId", transId)

val a = 
sqlContext.sql("""
with 
s1 as
(
	select c.seq_num, c.lane, a.store_id, b.Product_id, d.loyalty_member_num as loyalty_member_id, p.promo_code_id, 
	       e.employee_id as emp_id, c.amt, substr (c.t_stmp,1,10) as tx_date
	from ttST c  
	left join  llST LL on LL.seq_num = c.seq_num 
	left join  ppST PP on PP.seq_num = c.seq_num 
	left join  retail.promotion p on p.promo_code = PP.promo_cd  
	join       retail.store a     on a.store_num = c.store
	join       retail.product b   on b.product_code = c.product_cd
	left join  retail.loyalty d   on d.card_no = LL.loyalty_cd 
	join       retail.employee e  on e.employee_num = c.emp_num
	order by employee_id, product_id
)
select seq_num, lane, store_id, Product_id, loyalty_member_id, promo_code_id, emp_id, sum(amt) as amount, count(*) as qty, tx_date
from s1 
group by seq_num, lane, store_id, Product_id, loyalty_member_id, promo_code_id, emp_id, tx_date 
order by emp_id, Product_id
""")

val b = a.select(tid(a("tx_date"), a("store_id"), a("lane"), a("seq_num")).alias("trans_id"), a("store_id"), a("Product_id"), a("loyalty_member_id"), a("promo_code_id"), a("emp_id"), a("amount"), a("qty"),a("tx_date"))

b.registerTempTable("temptable")

sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
sqlContext.sql("insert into retail.fstore partition (tx_date) select * from temptable")

create table retail_db.fstore (trans_id DECIMAL(20), store_id INT, product_id INT, loyalty_member_id INT, promo_code_id INT, emp_id INT, amount DECIMAL(10,2), qty INT, tx_date VARCHAR(50));

sqoop export --connect jdbc:mysql://localhost:3306/retail_db --username retail_dba --password cloudera --table fstore --fields-terminated-by ',' --export-dir /data/retail/fstore2/  --null-string '\N'